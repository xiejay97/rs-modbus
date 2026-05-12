use crate::error::ModbusError;
use crate::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use crate::utils::gen_connection_id;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

pub struct UdpPhysicalLayer {
    pub(crate) socket: Arc<Mutex<Option<Arc<UdpSocket>>>>,
    is_open: Arc<Mutex<bool>>,
    is_opening: Arc<Mutex<bool>>,
    is_destroyed: Arc<Mutex<bool>>,
    pub(crate) local_addr: Arc<Mutex<Option<String>>>,
    remote_addr: Arc<Mutex<Option<String>>>,
    is_server: bool,
    connection_id: Arc<Mutex<Option<ConnectionId>>>,
    recv_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    data_tx: broadcast::Sender<DataEvent>,
    write_tx: broadcast::Sender<Vec<u8>>,
    error_tx: broadcast::Sender<ModbusError>,
    connection_close_tx: broadcast::Sender<ConnectionId>,
    close_tx: broadcast::Sender<()>,
}

impl UdpPhysicalLayer {
    fn build(is_server: bool, remote_addr: Option<String>) -> Arc<Self> {
        let (data_tx, _) = broadcast::channel(16);
        let (write_tx, _) = broadcast::channel(16);
        let (error_tx, _) = broadcast::channel(16);
        let (connection_close_tx, _) = broadcast::channel(16);
        let (close_tx, _) = broadcast::channel(16);
        Arc::new(Self {
            socket: Arc::new(Mutex::new(None)),
            is_open: Arc::new(Mutex::new(false)),
            is_opening: Arc::new(Mutex::new(false)),
            is_destroyed: Arc::new(Mutex::new(false)),
            local_addr: Arc::new(Mutex::new(None)),
            remote_addr: Arc::new(Mutex::new(remote_addr)),
            is_server,
            connection_id: Arc::new(Mutex::new(None)),
            recv_task: Arc::new(Mutex::new(None)),
            data_tx,
            write_tx,
            error_tx,
            connection_close_tx,
            close_tx,
        })
    }

    pub fn new_server() -> Arc<Self> {
        Self::build(true, None)
    }

    pub fn new_client(remote_addr: String) -> Arc<Self> {
        Self::build(false, Some(remote_addr))
    }

    pub async fn set_local_addr(&self, addr: String) {
        *self.local_addr.lock().await = Some(addr);
    }

    /// Resolves to the currently bound local address (post-`open()`), or
    /// `None` if the socket isn't open.
    pub async fn local_addr(&self) -> Option<String> {
        let guard = self.socket.lock().await;
        guard.as_ref().and_then(|s| s.local_addr().ok().map(|a| a.to_string()))
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
        {
            let opened = self.is_open.lock().await;
            let opening = self.is_opening.lock().await;
            if *opened || *opening {
                return Err(ModbusError::PortAlreadyOpen);
            }
        }
        *self.is_opening.lock().await = true;

        let bind_addr = if self.is_server {
            self.local_addr
                .lock()
                .await
                .clone()
                .unwrap_or_else(|| "[::]:502".to_string())
        } else {
            self.local_addr
                .lock()
                .await
                .clone()
                .unwrap_or_else(|| "0.0.0.0:0".to_string())
        };
        let socket = match UdpSocket::bind(&bind_addr).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                *self.is_opening.lock().await = false;
                return Err(ModbusError::ConnectionError(e.to_string()));
            }
        };
        *self.socket.lock().await = Some(Arc::clone(&socket));

        let conn_id: ConnectionId = Arc::from(gen_connection_id(if self.is_server {
            "udp-server"
        } else {
            "udp-client"
        }));
        *self.connection_id.lock().await = Some(Arc::clone(&conn_id));

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let connection_close_tx = self.connection_close_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open_for_task = Arc::clone(&self.is_open);
        let conn_id_for_task = Arc::clone(&conn_id);
        let socket_for_task = Arc::clone(&socket);

        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                match socket_for_task.recv_from(&mut buf).await {
                    Ok((n, addr)) => {
                        let data = buf[..n].to_vec();
                        let socket = Arc::clone(&socket_for_task);
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
                            connection: Arc::clone(&conn_id_for_task),
                        });
                    }
                    Err(e) => {
                        let _ = error_tx.send(ModbusError::ConnectionError(e.to_string()));
                        break;
                    }
                }
            }
            // Natural exit (socket errored). Emit close + connection_close
            // exactly once via the is_open transition gate, so close() and
            // this task don't both fire the listeners on the same session.
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

        *self.recv_task.lock().await = Some(handle);
        *self.is_open.lock().await = true;
        *self.is_opening.lock().await = false;
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
        let was_open = {
            let mut g = self.is_open.lock().await;
            let prev = *g;
            *g = false;
            prev
        };
        if !was_open {
            return Ok(());
        }
        // Abort the recv task so the socket Arc drops promptly and the
        // bound port is released for the next open().
        if let Some(handle) = self.recv_task.lock().await.take() {
            handle.abort();
        }
        *self.socket.lock().await = None;
        let conn_id_opt = self.connection_id.lock().await.take();
        if let Some(conn_id) = conn_id_opt {
            let _ = self.connection_close_tx.send(conn_id);
        }
        let _ = self.close_tx.send(());
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
