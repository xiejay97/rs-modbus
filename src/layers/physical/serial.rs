use crate::error::ModbusError;
use crate::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use crate::utils::gen_connection_id;
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct SerialPhysicalLayer {
    port: Arc<std::sync::Mutex<Option<Box<dyn serialport::SerialPort>>>>,
    is_open: Arc<std::sync::Mutex<bool>>,
    is_opening: Arc<std::sync::Mutex<bool>>,
    is_destroyed: Arc<std::sync::Mutex<bool>>,
    path: String,
    baud_rate: u32,
    connection_id: Arc<std::sync::Mutex<Option<ConnectionId>>>,
    data_tx: broadcast::Sender<DataEvent>,
    write_tx: broadcast::Sender<Vec<u8>>,
    error_tx: broadcast::Sender<ModbusError>,
    connection_close_tx: broadcast::Sender<ConnectionId>,
    close_tx: broadcast::Sender<()>,
}

impl SerialPhysicalLayer {
    pub fn new(path: String, baud_rate: u32) -> Arc<Self> {
        let (data_tx, _) = broadcast::channel(16);
        let (write_tx, _) = broadcast::channel(16);
        let (error_tx, _) = broadcast::channel(16);
        let (connection_close_tx, _) = broadcast::channel(16);
        let (close_tx, _) = broadcast::channel(16);
        Arc::new(Self {
            port: Arc::new(std::sync::Mutex::new(None)),
            is_open: Arc::new(std::sync::Mutex::new(false)),
            is_opening: Arc::new(std::sync::Mutex::new(false)),
            is_destroyed: Arc::new(std::sync::Mutex::new(false)),
            path,
            baud_rate,
            connection_id: Arc::new(std::sync::Mutex::new(None)),
            data_tx,
            write_tx,
            error_tx,
            connection_close_tx,
            close_tx,
        })
    }

    pub fn baud_rate(&self) -> u32 {
        self.baud_rate
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for SerialPhysicalLayer {
    fn layer_type(&self) -> PhysicalLayerType {
        PhysicalLayerType::Serial
    }

    async fn open(&self) -> Result<(), ModbusError> {
        if *self.is_destroyed.lock().unwrap() {
            return Err(ModbusError::PortDestroyed);
        }
        {
            let opened = self.is_open.lock().unwrap();
            let opening = self.is_opening.lock().unwrap();
            if *opened || *opening {
                return Err(ModbusError::PortAlreadyOpen);
            }
        }
        *self.is_opening.lock().unwrap() = true;

        let port = match serialport::new(&self.path, self.baud_rate).open() {
            Ok(p) => p,
            Err(e) => {
                *self.is_opening.lock().unwrap() = false;
                return Err(ModbusError::ConnectionError(e.to_string()));
            }
        };
        *self.port.lock().unwrap() = Some(port);

        let conn_id: ConnectionId = Arc::from(gen_connection_id("serial"));
        *self.connection_id.lock().unwrap() = Some(Arc::clone(&conn_id));

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let connection_close_tx = self.connection_close_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open_for_task = Arc::clone(&self.is_open);
        let port = Arc::clone(&self.port);
        let port_for_response = Arc::clone(&self.port);
        let conn_id_for_task = Arc::clone(&conn_id);

        tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut buf = vec![0u8; 1024];
            while let Ok(mut guard) = port.lock() {
                if let Some(ref mut p) = *guard {
                    match p.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            let data = buf[..n].to_vec();
                            let port_for_response = Arc::clone(&port_for_response);
                            let response: ResponseFn = Arc::new(move |reply: Vec<u8>| {
                                let port_for_response = Arc::clone(&port_for_response);
                                Box::pin(async move {
                                    use std::io::Write;
                                    let mut g = port_for_response.lock().map_err(|_| {
                                        ModbusError::ConnectionError(
                                            "serial port poisoned".to_string(),
                                        )
                                    })?;
                                    if let Some(ref mut p) = *g {
                                        p.write_all(&reply).map_err(|e| {
                                            ModbusError::ConnectionError(e.to_string())
                                        })?;
                                    }
                                    Ok(())
                                })
                            });
                            let _ = data_tx.send(DataEvent {
                                data,
                                response,
                                connection: Arc::clone(&conn_id_for_task),
                            });
                        }
                        Err(e) => {
                            let _ = error_tx.send(ModbusError::ConnectionError(e.to_string()));
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
            // Natural exit (port errored or removed). Emit close events
            // exactly once via the is_open transition gate, so close() and
            // this task don't both fire the listeners on the same session.
            let was_open = if let Ok(mut g) = is_open_for_task.lock() {
                let prev = *g;
                *g = false;
                prev
            } else {
                false
            };
            if was_open {
                let _ = connection_close_tx.send(conn_id_for_task);
                let _ = close_tx.send(());
            }
        });

        *self.is_open.lock().unwrap() = true;
        *self.is_opening.lock().unwrap() = false;
        Ok(())
    }

    async fn write(&self, data: &[u8]) -> Result<(), ModbusError> {
        if !*self.is_open.lock().unwrap() {
            return Err(ModbusError::PortNotOpen);
        }
        if let Ok(mut guard) = self.port.lock() {
            if let Some(ref mut port) = *guard {
                use std::io::Write;
                port.write_all(data)
                    .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
                let _ = self.write_tx.send(data.to_vec());
                Ok(())
            } else {
                Err(ModbusError::PortNotOpen)
            }
        } else {
            Err(ModbusError::PortNotOpen)
        }
    }

    async fn close(&self) -> Result<(), ModbusError> {
        let was_open = {
            let mut g = self.is_open.lock().unwrap();
            let prev = *g;
            *g = false;
            prev
        };
        if !was_open {
            return Ok(());
        }
        // Drop the port; the spawn_blocking read loop will see the next
        // guard hold `None` and exit (it may still be parked inside a
        // blocking `read` until data arrives or the OS layer closes — same
        // pre-existing limitation of the sync `serialport` API).
        *self.port.lock().unwrap() = None;
        let conn_id_opt = self.connection_id.lock().unwrap().take();
        if let Some(conn_id) = conn_id_opt {
            let _ = self.connection_close_tx.send(conn_id);
        }
        let _ = self.close_tx.send(());
        Ok(())
    }

    async fn destroy(&self) {
        *self.is_destroyed.lock().unwrap() = true;
        let _ = self.close().await;
    }

    fn is_open(&self) -> bool {
        if let Ok(guard) = self.is_open.lock() {
            *guard
        } else {
            false
        }
    }

    fn is_destroyed(&self) -> bool {
        if let Ok(guard) = self.is_destroyed.lock() {
            *guard
        } else {
            false
        }
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
