use crate::error::ModbusError;
use crate::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use crate::utils::gen_connection_id;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, Mutex};

pub struct UdpPhysicalLayer {
    pub(crate) socket: Arc<Mutex<Option<Arc<UdpSocket>>>>,
    is_open: Arc<Mutex<bool>>,
    is_destroyed: Arc<Mutex<bool>>,
    pub(crate) local_addr: Arc<Mutex<Option<String>>>,
    remote_addr: Arc<Mutex<Option<String>>>,
    is_server: bool,
    connection_id: ConnectionId,
    data_tx: broadcast::Sender<DataEvent>,
    write_tx: broadcast::Sender<Vec<u8>>,
    error_tx: broadcast::Sender<ModbusError>,
    connection_close_tx: broadcast::Sender<ConnectionId>,
    close_tx: broadcast::Sender<()>,
    _data_rx: Mutex<broadcast::Receiver<DataEvent>>,
    _write_rx: Mutex<broadcast::Receiver<Vec<u8>>>,
    _error_rx: Mutex<broadcast::Receiver<ModbusError>>,
    _connection_close_rx: Mutex<broadcast::Receiver<ConnectionId>>,
    _close_rx: Mutex<broadcast::Receiver<()>>,
}

impl UdpPhysicalLayer {
    fn build(is_server: bool, remote_addr: Option<String>) -> Arc<Self> {
        let (data_tx, data_rx) = broadcast::channel(16);
        let (write_tx, write_rx) = broadcast::channel(16);
        let (error_tx, error_rx) = broadcast::channel(16);
        let (connection_close_tx, connection_close_rx) = broadcast::channel(16);
        let (close_tx, close_rx) = broadcast::channel(16);
        Arc::new(Self {
            socket: Arc::new(Mutex::new(None)),
            is_open: Arc::new(Mutex::new(false)),
            is_destroyed: Arc::new(Mutex::new(false)),
            local_addr: Arc::new(Mutex::new(None)),
            remote_addr: Arc::new(Mutex::new(remote_addr)),
            is_server,
            connection_id: Arc::from(gen_connection_id(if is_server {
                "udp-server"
            } else {
                "udp-client"
            })),
            data_tx,
            write_tx,
            error_tx,
            connection_close_tx,
            close_tx,
            _data_rx: Mutex::new(data_rx),
            _write_rx: Mutex::new(write_rx),
            _error_rx: Mutex::new(error_rx),
            _connection_close_rx: Mutex::new(connection_close_rx),
            _close_rx: Mutex::new(close_rx),
        })
    }

    pub fn new_server() -> Arc<Self> {
        Self::build(true, None)
    }

    pub fn new_client(remote_addr: String) -> Arc<Self> {
        Self::build(false, Some(remote_addr))
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for UdpPhysicalLayer {
    fn layer_type(&self) -> PhysicalLayerType {
        PhysicalLayerType::Net
    }

    async fn open(&self) -> Result<(), ModbusError> {
        if *self.is_destroyed.lock().await {
            return Err(ModbusError::PortDestroyed);
        }
        let socket = if self.is_server {
            let addr = self
                .local_addr
                .lock()
                .await
                .clone()
                .unwrap_or_else(|| "[::]:502".to_string());
            UdpSocket::bind(&addr)
                .await
                .map_err(|e| ModbusError::ConnectionError(e.to_string()))?
        } else {
            let local = self
                .local_addr
                .lock()
                .await
                .clone()
                .unwrap_or_else(|| "0.0.0.0:0".to_string());
            UdpSocket::bind(&local)
                .await
                .map_err(|e| ModbusError::ConnectionError(e.to_string()))?
        };
        let socket = Arc::new(socket);
        *self.socket.lock().await = Some(Arc::clone(&socket));
        *self.is_open.lock().await = true;

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let connection_close_tx = self.connection_close_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open = Arc::clone(&self.is_open);
        let is_server = self.is_server;
        let conn_id = Arc::clone(&self.connection_id);

        tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, addr)) => {
                        let data = buf[..n].to_vec();
                        let socket = Arc::clone(&socket);
                        let response: ResponseFn = Arc::new(move |data: Vec<u8>| {
                            let socket = Arc::clone(&socket);
                            Box::pin(async move {
                                socket
                                    .send_to(&data, addr)
                                    .await
                                    .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
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
            let _ = connection_close_tx.send(Arc::clone(&conn_id));
            if is_server {
                let _ = close_tx.send(());
            }
        });

        Ok(())
    }

    async fn write(&self, data: &[u8]) -> Result<(), ModbusError> {
        if !*self.is_open.lock().await {
            return Err(ModbusError::PortNotOpen);
        }
        let socket = self.socket.lock().await.as_ref().unwrap().clone();
        match *self.remote_addr.lock().await {
            Some(ref remote) => {
                socket
                    .send_to(data, remote)
                    .await
                    .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
                let _ = self.write_tx.send(data.to_vec());
                Ok(())
            }
            None if self.is_server => Err(ModbusError::ConnectionError(
                "No remote address for server".to_string(),
            )),
            None => Err(ModbusError::ConnectionError(
                "No remote address configured for client".to_string(),
            )),
        }
    }

    async fn close(&self) -> Result<(), ModbusError> {
        *self.is_open.lock().await = false;
        *self.socket.lock().await = None;
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
