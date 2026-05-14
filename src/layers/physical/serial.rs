use crate::error::ModbusError;
use crate::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use crate::utils::gen_connection_id;
use serialport::{DataBits, FlowControl, Parity, StopBits};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Background read loop poll interval. With `try_clone` splitting read and
/// write onto independent OS handles, this no longer gates write throughput;
/// it only bounds how long `close()` waits for the read thread to observe
/// `is_open = false` and release its handle.
const READ_TIMEOUT_MS: u64 = 100;

/// Serial port configuration. Mirrors `SerialPhysicalLayerOptions` in the
/// sister njs-modbus project so callers can set data bits, stop bits, parity,
/// flow control, and timeout.
#[derive(Clone, Debug)]
pub struct SerialPhysicalLayerOptions {
    pub path: String,
    pub baud_rate: u32,
    pub data_bits: DataBits,
    pub stop_bits: StopBits,
    pub parity: Parity,
    pub flow_control: FlowControl,
    pub timeout_ms: u64,
}

impl Default for SerialPhysicalLayerOptions {
    fn default() -> Self {
        Self {
            path: String::new(),
            baud_rate: 9600,
            data_bits: DataBits::Eight,
            stop_bits: StopBits::One,
            parity: Parity::None,
            flow_control: FlowControl::None,
            timeout_ms: READ_TIMEOUT_MS,
        }
    }
}

pub struct SerialPhysicalLayer {
    // Write handle. `try_clone` gives this its own OS file/HANDLE so the
    // mutex serializes writer-vs-writer only, never blocks on the read loop.
    write_port: Arc<std::sync::Mutex<Option<Box<dyn serialport::SerialPort>>>>,
    // Joined by `close()` so the OS port is fully released before returning,
    // preventing close-then-open from racing into "device busy".
    read_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    is_open: Arc<AtomicBool>,
    is_opening: AtomicBool,
    is_destroyed: AtomicBool,
    options: SerialPhysicalLayerOptions,
    connection_id: std::sync::Mutex<Option<ConnectionId>>,
    data_tx: broadcast::Sender<DataEvent>,
    write_tx: broadcast::Sender<Vec<u8>>,
    error_tx: broadcast::Sender<ModbusError>,
    connection_close_tx: broadcast::Sender<ConnectionId>,
    close_tx: broadcast::Sender<()>,
}

impl SerialPhysicalLayer {
    pub fn new(options: SerialPhysicalLayerOptions) -> Arc<Self> {
        let (data_tx, _) = broadcast::channel(16);
        let (write_tx, _) = broadcast::channel(16);
        let (error_tx, _) = broadcast::channel(16);
        let (connection_close_tx, _) = broadcast::channel(16);
        let (close_tx, _) = broadcast::channel(16);
        Arc::new(Self {
            write_port: Arc::new(std::sync::Mutex::new(None)),
            read_task: tokio::sync::Mutex::new(None),
            is_open: Arc::new(AtomicBool::new(false)),
            is_opening: AtomicBool::new(false),
            is_destroyed: AtomicBool::new(false),
            options,
            connection_id: std::sync::Mutex::new(None),
            data_tx,
            write_tx,
            error_tx,
            connection_close_tx,
            close_tx,
        })
    }

    pub fn baud_rate(&self) -> u32 {
        self.options.baud_rate
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for SerialPhysicalLayer {
    type OpenOptions = ();

    fn layer_type(&self) -> PhysicalLayerType {
        PhysicalLayerType::Serial
    }

    async fn open(&self, _options: Self::OpenOptions) -> Result<(), ModbusError> {
        if self.is_destroyed.load(Ordering::Acquire) {
            return Err(ModbusError::PortDestroyed);
        }
        if self
            .is_opening
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ModbusError::PortAlreadyOpen);
        }
        if self.is_open.load(Ordering::Acquire) {
            self.is_opening.store(false, Ordering::Release);
            return Err(ModbusError::PortAlreadyOpen);
        }

        // Short read timeout so the background loop wakes regularly to
        // check `is_open`; sync `serialport` has no cancellation primitive,
        // so this is the only portable shutdown signal.
        let opts = &self.options;
        let read_port = match serialport::new(&opts.path, opts.baud_rate)
            .data_bits(opts.data_bits)
            .stop_bits(opts.stop_bits)
            .parity(opts.parity)
            .flow_control(opts.flow_control)
            .timeout(std::time::Duration::from_millis(opts.timeout_ms))
            .open()
        {
            Ok(p) => p,
            Err(e) => {
                self.is_opening.store(false, Ordering::Release);
                return Err(ModbusError::ConnectionError(e.to_string()));
            }
        };
        // Independent OS handle for writes. Read and write paths now share
        // no synchronization — a blocking read never serializes a write.
        let write_port = match read_port.try_clone() {
            Ok(p) => p,
            Err(e) => {
                self.is_opening.store(false, Ordering::Release);
                return Err(ModbusError::ConnectionError(e.to_string()));
            }
        };
        *self.write_port.lock().unwrap() = Some(write_port);

        let conn_id: ConnectionId = Arc::from(gen_connection_id("serial"));
        *self.connection_id.lock().unwrap() = Some(Arc::clone(&conn_id));

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let connection_close_tx = self.connection_close_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open_for_task = Arc::clone(&self.is_open);
        let write_port_for_response = Arc::clone(&self.write_port);
        let conn_id_for_task = Arc::clone(&conn_id);

        let join = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut read_port = read_port;
            let mut buf = vec![0u8; 1024];
            loop {
                if !is_open_for_task.load(Ordering::Acquire) {
                    break;
                }
                match read_port.read(&mut buf) {
                    Ok(0) => {
                        // Timeout with no data — loop and recheck is_open.
                        continue;
                    }
                    Ok(n) => {
                        let data = buf[..n].to_vec();
                        let write_port_for_response = Arc::clone(&write_port_for_response);
                        let response: ResponseFn = Arc::new(move |reply: Vec<u8>| {
                            let write_port_for_response =
                                Arc::clone(&write_port_for_response);
                            Box::pin(async move {
                                tokio::task::spawn_blocking(move || {
                                    use std::io::Write;
                                    let mut g =
                                        write_port_for_response.lock().map_err(|_| {
                                            ModbusError::ConnectionError(
                                                "serial port poisoned".to_string(),
                                            )
                                        })?;
                                    match g.as_mut() {
                                        Some(p) => p.write_all(&reply).map_err(|e| {
                                            ModbusError::ConnectionError(e.to_string())
                                        }),
                                        None => Err(ModbusError::PortNotOpen),
                                    }
                                })
                                .await
                                .map_err(|e| ModbusError::ConnectionError(e.to_string()))?
                            })
                        });
                        let _ = data_tx.send(DataEvent {
                            data,
                            response,
                            connection: Arc::clone(&conn_id_for_task),
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {
                        continue;
                    }
                    Err(e) => {
                        let _ = error_tx.send(ModbusError::ConnectionError(e.to_string()));
                        break;
                    }
                }
            }
            // Natural exit (read error or `close()` flipped is_open). Gate
            // close-event emission via the is_open swap so close() and this
            // task can't both fire listeners on the same session.
            let was_open = is_open_for_task.swap(false, Ordering::AcqRel);
            if was_open {
                let _ = connection_close_tx.send(conn_id_for_task);
                let _ = close_tx.send(());
            }
        });
        *self.read_task.lock().await = Some(join);

        self.is_open.store(true, Ordering::Release);
        self.is_opening.store(false, Ordering::Release);
        Ok(())
    }

    async fn write(&self, data: &[u8]) -> Result<(), ModbusError> {
        if !self.is_open.load(Ordering::Acquire) {
            return Err(ModbusError::PortNotOpen);
        }
        let port = Arc::clone(&self.write_port);
        let data = data.to_vec();
        let write_tx = self.write_tx.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = port
                .lock()
                .map_err(|_| ModbusError::ConnectionError("serial port poisoned".to_string()))?;
            match guard.as_mut() {
                Some(port) => {
                    use std::io::Write;
                    port.write_all(&data)
                        .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
                    let _ = write_tx.send(data);
                    Ok(())
                }
                None => Err(ModbusError::PortNotOpen),
            }
        })
        .await
        .map_err(|e| ModbusError::ConnectionError(e.to_string()))?
    }

    async fn close(&self) -> Result<(), ModbusError> {
        let was_open = self.is_open.swap(false, Ordering::AcqRel);
        if !was_open {
            return Ok(());
        }
        // Drop the write handle first so any subsequent writer fails fast;
        // an already-running write_all completes normally on its own handle.
        *self.write_port.lock().unwrap() = None;
        // Wait for the read loop to observe is_open=false and drop its
        // own (cloned) OS handle — otherwise a close-then-open sequence
        // would race against the OS port not being fully released.
        let task = self.read_task.lock().await.take();
        if let Some(handle) = task {
            let _ = handle.await;
        }
        let conn_id_opt = self.connection_id.lock().unwrap().take();
        if let Some(conn_id) = conn_id_opt {
            let _ = self.connection_close_tx.send(conn_id);
        }
        let _ = self.close_tx.send(());
        Ok(())
    }

    async fn destroy(&self) {
        if self.is_destroyed.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.close().await;
    }

    fn is_open(&self) -> bool {
        self.is_open.load(Ordering::Acquire)
    }

    fn is_destroyed(&self) -> bool {
        self.is_destroyed.load(Ordering::Acquire)
    }

    fn subscribe_data(&self) -> broadcast::Receiver<DataEvent> {
        self.data_tx.subscribe()
    }

    fn subscribe_write(&self) -> broadcast::Receiver<Vec<u8>> {
        self.write_tx.subscribe()
    }

    fn subscribe_error(&self) -> broadcast::Receiver<ModbusError> {
        self.error_tx.subscribe()
    }

    fn subscribe_connection_close(&self) -> broadcast::Receiver<ConnectionId> {
        self.connection_close_tx.subscribe()
    }

    fn subscribe_close(&self) -> broadcast::Receiver<()> {
        self.close_tx.subscribe()
    }
}
