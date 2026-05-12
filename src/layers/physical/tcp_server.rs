use crate::error::ModbusError;
use crate::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use crate::utils::gen_connection_id;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

pub struct TcpServerPhysicalLayer {
    is_open: Arc<Mutex<bool>>,
    is_destroyed: Arc<Mutex<bool>>,
    pub(crate) addr: Arc<Mutex<Option<String>>>,
    clients: Arc<Mutex<HashMap<ConnectionId, Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>>>>,
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

impl TcpServerPhysicalLayer {
    pub fn new() -> Arc<Self> {
        let (data_tx, data_rx) = broadcast::channel(16);
        let (write_tx, write_rx) = broadcast::channel(16);
        let (error_tx, error_rx) = broadcast::channel(16);
        let (connection_close_tx, connection_close_rx) = broadcast::channel(16);
        let (close_tx, close_rx) = broadcast::channel(16);
        Arc::new(Self {
            is_open: Arc::new(Mutex::new(false)),
            is_destroyed: Arc::new(Mutex::new(false)),
            addr: Arc::new(Mutex::new(None)),
            clients: Arc::new(Mutex::new(HashMap::new())),
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

    pub async fn set_addr(&self, addr: String) {
        *self.addr.lock().await = Some(addr);
    }

    pub async fn get_addr(&self) -> Option<String> {
        self.addr.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for TcpServerPhysicalLayer {
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
            .unwrap_or_else(|| "[::]:502".to_string());
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
        *self.addr.lock().await = Some(listener.local_addr().unwrap().to_string());
        *self.is_open.lock().await = true;

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let connection_close_tx = self.connection_close_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open = Arc::clone(&self.is_open);
        let clients = Arc::clone(&self.clients);

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let (mut read_half, write_half) = stream.into_split();
                        let write_half = Arc::new(Mutex::new(write_half));
                        let conn_id: ConnectionId =
                            Arc::from(gen_connection_id("tcp-server"));
                        clients
                            .lock()
                            .await
                            .insert(Arc::clone(&conn_id), Arc::clone(&write_half));
                        let data_tx = data_tx.clone();
                        let error_tx = error_tx.clone();
                        let connection_close_tx = connection_close_tx.clone();
                        let clients = Arc::clone(&clients);
                        let conn_id_for_task = Arc::clone(&conn_id);

                        tokio::spawn(async move {
                            let mut buf = vec![0u8; 1024];
                            loop {
                                match read_half.read(&mut buf).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        let data = buf[..n].to_vec();
                                        let wh = Arc::clone(&write_half);
                                        let response: ResponseFn =
                                            Arc::new(move |data: Vec<u8>| {
                                                let wh = Arc::clone(&wh);
                                                Box::pin(async move {
                                                    let mut s = wh.lock().await;
                                                    s.write_all(&data).await.map_err(|e| {
                                                        ModbusError::ConnectionError(e.to_string())
                                                    })?;
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
                                        let _ = error_tx
                                            .send(ModbusError::ConnectionError(e.to_string()));
                                        break;
                                    }
                                }
                            }
                            clients.lock().await.remove(&conn_id_for_task);
                            let _ = connection_close_tx.send(conn_id_for_task);
                        });
                    }
                    Err(e) => {
                        let _ = error_tx.send(ModbusError::ConnectionError(e.to_string()));
                        break;
                    }
                }
            }
            *is_open.lock().await = false;
            let _ = close_tx.send(());
        });

        Ok(())
    }

    async fn write(&self, _data: &[u8]) -> Result<(), ModbusError> {
        Err(ModbusError::NotSupported)
    }

    async fn close(&self) -> Result<(), ModbusError> {
        *self.is_open.lock().await = false;
        let mut clients = self.clients.lock().await;
        let drained: Vec<(ConnectionId, _)> = clients.drain().collect();
        drop(clients);
        for (conn_id, client) in drained {
            let mut guard = client.lock().await;
            let _ = guard.shutdown().await;
            drop(guard);
            let _ = self.connection_close_tx.send(conn_id);
        }
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
