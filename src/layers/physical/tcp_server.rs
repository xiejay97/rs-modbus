use crate::error::ModbusError;
use crate::layers::physical::{PhysicalLayer, ResponseFn};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

use std::collections::HashMap;

pub struct TcpServerPhysicalLayer {
    is_open: Arc<Mutex<bool>>,
    is_destroyed: Arc<Mutex<bool>>,
    pub(crate) addr: Arc<Mutex<Option<String>>>,
    clients: Arc<Mutex<HashMap<u64, Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>>>>,
    next_client_id: Arc<Mutex<u64>>,
    data_tx: broadcast::Sender<(Vec<u8>, ResponseFn)>,
    error_tx: broadcast::Sender<ModbusError>,
    close_tx: broadcast::Sender<()>,
    _data_rx: Mutex<broadcast::Receiver<(Vec<u8>, ResponseFn)>>,
    _error_rx: Mutex<broadcast::Receiver<ModbusError>>,
    _close_rx: Mutex<broadcast::Receiver<()>>,
}

impl TcpServerPhysicalLayer {
    pub fn new() -> Arc<Self> {
        let (data_tx, data_rx) = broadcast::channel(16);
        let (error_tx, error_rx) = broadcast::channel(16);
        let (close_tx, close_rx) = broadcast::channel(16);
        Arc::new(Self {
            is_open: Arc::new(Mutex::new(false)),
            is_destroyed: Arc::new(Mutex::new(false)),
            addr: Arc::new(Mutex::new(None)),
            clients: Arc::new(Mutex::new(HashMap::new())),
            next_client_id: Arc::new(Mutex::new(0)),
            data_tx,
            error_tx,
            close_tx,
            _data_rx: Mutex::new(data_rx),
            _error_rx: Mutex::new(error_rx),
            _close_rx: Mutex::new(close_rx),
        })
    }

    pub async fn set_addr(&self, addr: String) {
        *self.addr.lock().await = Some(addr);
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for TcpServerPhysicalLayer {
    async fn open(&self) -> Result<(), ModbusError> {
        if *self.is_destroyed.lock().await {
            return Err(ModbusError::PortDestroyed);
        }
        let addr = self
            .addr
            .lock()
            .await
            .clone()
            .unwrap_or_else(|| "0.0.0.0:502".to_string());
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
        *self.is_open.lock().await = true;

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open = Arc::clone(&self.is_open);
        let clients = Arc::clone(&self.clients);
        let next_client_id = Arc::clone(&self.next_client_id);

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let (mut read_half, write_half) = stream.into_split();
                        let write_half = Arc::new(Mutex::new(write_half));
                        let client_id = {
                            let mut id_guard = next_client_id.lock().await;
                            let id = *id_guard;
                            *id_guard += 1;
                            id
                        };
                        clients.lock().await.insert(client_id, Arc::clone(&write_half));
                        let data_tx = data_tx.clone();
                        let error_tx = error_tx.clone();
                        let clients = Arc::clone(&clients);

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
                                                let mut s = wh.lock().await;
                                                s.write_all(&data)
                                                    .await
                                                    .map_err(|e| {
                                                        ModbusError::ConnectionError(e.to_string())
                                                    })?;
                                                Ok(())
                                            })
                                        });
                                        let _ = data_tx.send((data, response));
                                    }
                                    Err(e) => {
                                        let _ = error_tx
                                            .send(ModbusError::ConnectionError(e.to_string()));
                                        break;
                                    }
                                }
                            }
                            clients.lock().await.remove(&client_id);
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
        for (_, client) in clients.drain() {
            let mut guard = client.lock().await;
            let _ = guard.shutdown().await;
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

    fn subscribe_data(&self) -> broadcast::Receiver<(Vec<u8>, ResponseFn)> {
        self.data_tx.subscribe()
    }

    fn subscribe_error(&self) -> broadcast::Receiver<ModbusError> {
        self.error_tx.subscribe()
    }

    fn subscribe_close(&self) -> broadcast::Receiver<()> {
        self.close_tx.subscribe()
    }
}
