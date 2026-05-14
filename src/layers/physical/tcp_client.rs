use crate::error::ModbusError;
use crate::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use crate::utils::gen_connection_id;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

pub struct TcpClientPhysicalLayer {
    write_half: Arc<Mutex<Option<tokio::net::tcp::OwnedWriteHalf>>>,
    is_open: Arc<Mutex<bool>>,
    is_opening: Arc<Mutex<bool>>,
    is_destroyed: Arc<Mutex<bool>>,
    pub(crate) addr: Arc<Mutex<Option<String>>>,
    connection_id: Arc<Mutex<Option<ConnectionId>>>,
    read_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    data_tx: broadcast::Sender<DataEvent>,
    write_tx: broadcast::Sender<Vec<u8>>,
    error_tx: broadcast::Sender<ModbusError>,
    connection_close_tx: broadcast::Sender<ConnectionId>,
    close_tx: broadcast::Sender<()>,
}

impl TcpClientPhysicalLayer {
    pub fn new() -> Arc<Self> {
        let (data_tx, _) = broadcast::channel(16);
        let (write_tx, _) = broadcast::channel(16);
        let (error_tx, _) = broadcast::channel(16);
        let (connection_close_tx, _) = broadcast::channel(16);
        let (close_tx, _) = broadcast::channel(16);
        Arc::new(Self {
            write_half: Arc::new(Mutex::new(None)),
            is_open: Arc::new(Mutex::new(false)),
            is_opening: Arc::new(Mutex::new(false)),
            is_destroyed: Arc::new(Mutex::new(false)),
            addr: Arc::new(Mutex::new(None)),
            connection_id: Arc::new(Mutex::new(None)),
            read_task: Arc::new(Mutex::new(None)),
            data_tx,
            write_tx,
            error_tx,
            connection_close_tx,
            close_tx,
        })
    }

    pub async fn set_addr(&self, addr: String) {
        *self.addr.lock().await = Some(addr);
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for TcpClientPhysicalLayer {
    type OpenOptions = Option<String>;

    fn layer_type(&self) -> PhysicalLayerType {
        PhysicalLayerType::Net
    }

    async fn open(&self, options: Self::OpenOptions) -> Result<(), ModbusError> {
        if *self.is_destroyed.lock().await {
            return Err(ModbusError::PortDestroyed);
        }
        {
            let opened = self.is_open.lock().await;
            let opening = self.is_opening.lock().await;
            if *opened || *opening {
                return Err(ModbusError::PortAlreadyOpen);
            }
        }
        *self.is_opening.lock().await = true;

        let addr = if let Some(addr) = options {
            addr
        } else {
            self.addr.lock().await.clone().unwrap_or_else(|| "127.0.0.1:502".to_string())
        };
        let stream = match TcpStream::connect(&addr).await {
            Ok(s) => s,
            Err(e) => {
                *self.is_opening.lock().await = false;
                return Err(ModbusError::ConnectionError(e.to_string()));
            }
        };
        let (mut read_half, write_half) = stream.into_split();
        *self.write_half.lock().await = Some(write_half);

        let conn_id: ConnectionId = Arc::from(gen_connection_id("tcp-client"));
        *self.connection_id.lock().await = Some(Arc::clone(&conn_id));

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let connection_close_tx = self.connection_close_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open_for_task = Arc::clone(&self.is_open);
        let write_half_for_task = Arc::clone(&self.write_half);
        let conn_id_for_task = Arc::clone(&conn_id);

        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let data = buf[..n].to_vec();
                        let wh = Arc::clone(&write_half_for_task);
                        let response: ResponseFn = Arc::new(move |data: Vec<u8>| {
                            let wh = Arc::clone(&wh);
                            Box::pin(async move {
                                let mut guard = wh.lock().await;
                                if let Some(ref mut w) = *guard {
                                    w.write_all(&data)
                                        .await
                                        .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
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
            }
            // Natural exit (peer closed or read error): emit close events
            // exactly once. The is_open=true->false transition is the gate so
            // close() and this task can't both fire the listeners for the
            // same session.
            let was_open = {
                let mut g = is_open_for_task.lock().await;
                let prev = *g;
                *g = false;
                prev
            };
            if was_open {
                let _ = connection_close_tx.send(conn_id_for_task);
                let _ = close_tx.send(());
            }
        });

        *self.read_task.lock().await = Some(handle);
        *self.is_open.lock().await = true;
        *self.is_opening.lock().await = false;
        Ok(())
    }

    async fn write(&self, data: &[u8]) -> Result<(), ModbusError> {
        if !*self.is_open.lock().await {
            return Err(ModbusError::PortNotOpen);
        }
        if let Some(ref mut w) = *self.write_half.lock().await {
            w.write_all(data)
                .await
                .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
            let _ = self.write_tx.send(data.to_vec());
            Ok(())
        } else {
            Err(ModbusError::PortNotOpen)
        }
    }

    async fn close(&self) -> Result<(), ModbusError> {
        let was_open = {
            let mut g = self.is_open.lock().await;
            let prev = *g;
            *g = false;
            prev
        };
        if !was_open {
            return Ok(());
        }
        // Abort the read task so its OwnedReadHalf drops promptly (closes
        // the TCP socket); otherwise reopen on the same port could collide
        // with the lingering half-open connection.
        if let Some(handle) = self.read_task.lock().await.take() {
            handle.abort();
        }
        *self.write_half.lock().await = None;
        let conn_id_opt = self.connection_id.lock().await.take();
        if let Some(conn_id) = conn_id_opt {
            let _ = self.connection_close_tx.send(conn_id);
        }
        let _ = self.close_tx.send(());
        Ok(())
    }

    async fn destroy(&self) {
        if *self.is_destroyed.lock().await {
            return;
        }
        *self.is_destroyed.lock().await = true;
        let _ = self.close().await;
    }

    fn is_open(&self) -> bool {
        if let Ok(guard) = self.is_open.try_lock() {
            *guard
        } else {
            false
        }
    }

    fn is_destroyed(&self) -> bool {
        if let Ok(guard) = self.is_destroyed.try_lock() {
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
