use crate::error::ModbusError;
use crate::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use crate::utils::gen_connection_id;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};

pub struct TcpClientPhysicalLayer {
    write_half: Arc<Mutex<Option<tokio::net::tcp::OwnedWriteHalf>>>,
    is_open: Arc<Mutex<bool>>,
    is_destroyed: Arc<Mutex<bool>>,
    pub(crate) addr: Arc<Mutex<Option<String>>>,
    connection_id: Arc<Mutex<Option<ConnectionId>>>,
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
            is_destroyed: Arc::new(Mutex::new(false)),
            addr: Arc::new(Mutex::new(None)),
            connection_id: Arc::new(Mutex::new(None)),
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
    fn layer_type(&self) -> PhysicalLayerType {
        PhysicalLayerType::Net
    }

    async fn open(&self) -> Result<(), ModbusError> {
        if *self.is_destroyed.lock().await {
            return Err(ModbusError::PortDestroyed);
        }
        let addr = self
            .addr
            .lock()
            .await
            .clone()
            .unwrap_or_else(|| "127.0.0.1:502".to_string());
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
        let (mut read_half, write_half) = stream.into_split();
        *self.write_half.lock().await = Some(write_half);
        *self.is_open.lock().await = true;

        let conn_id: ConnectionId = Arc::from(gen_connection_id("tcp-client"));
        *self.connection_id.lock().await = Some(Arc::clone(&conn_id));

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let connection_close_tx = self.connection_close_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open = Arc::clone(&self.is_open);
        let write_half = Arc::clone(&self.write_half);

        tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let data = buf[..n].to_vec();
                        let wh = Arc::clone(&write_half);
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
                            connection: Arc::clone(&conn_id),
                        });
                    }
                    Err(e) => {
                        let _ = error_tx.send(ModbusError::ConnectionError(e.to_string()));
                        break;
                    }
                }
            }
            *is_open.lock().await = false;
            let _ = connection_close_tx.send(conn_id);
            let _ = close_tx.send(());
        });

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
        *self.is_open.lock().await = false;
        *self.write_half.lock().await = None;
        Ok(())
    }

    async fn destroy(&self) {
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
