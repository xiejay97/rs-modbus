use crate::error::ModbusError;
use crate::layers::physical::{PhysicalLayer, ResponseFn};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};

pub struct TcpClientPhysicalLayer {
    write_half: Arc<Mutex<Option<tokio::net::tcp::OwnedWriteHalf>>>,
    is_open: Arc<Mutex<bool>>,
    is_destroyed: Arc<Mutex<bool>>,
    pub(crate) addr: Arc<Mutex<Option<String>>>,
    data_tx: broadcast::Sender<(Vec<u8>, ResponseFn)>,
    error_tx: broadcast::Sender<ModbusError>,
    close_tx: broadcast::Sender<()>,
    _data_rx: Mutex<broadcast::Receiver<(Vec<u8>, ResponseFn)>>,
    _error_rx: Mutex<broadcast::Receiver<ModbusError>>,
    _close_rx: Mutex<broadcast::Receiver<()>>,
}

impl TcpClientPhysicalLayer {
    pub fn new() -> Arc<Self> {
        let (data_tx, data_rx) = broadcast::channel(16);
        let (error_tx, error_rx) = broadcast::channel(16);
        let (close_tx, close_rx) = broadcast::channel(16);
        Arc::new(Self {
            write_half: Arc::new(Mutex::new(None)),
            is_open: Arc::new(Mutex::new(false)),
            is_destroyed: Arc::new(Mutex::new(false)),
            addr: Arc::new(Mutex::new(None)),
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

impl Default for TcpClientPhysicalLayer {
    fn default() -> Self {
        let (data_tx, data_rx) = broadcast::channel(16);
        let (error_tx, error_rx) = broadcast::channel(16);
        let (close_tx, close_rx) = broadcast::channel(16);
        Self {
            write_half: Arc::new(Mutex::new(None)),
            is_open: Arc::new(Mutex::new(false)),
            is_destroyed: Arc::new(Mutex::new(false)),
            addr: Arc::new(Mutex::new(None)),
            data_tx,
            error_tx,
            close_tx,
            _data_rx: Mutex::new(data_rx),
            _error_rx: Mutex::new(error_rx),
            _close_rx: Mutex::new(close_rx),
        }
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for TcpClientPhysicalLayer {
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

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
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
                        let _ = data_tx.send((data, response));
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

    async fn write(&self, data: &[u8]) -> Result<(), ModbusError> {
        if !*self.is_open.lock().await {
            return Err(ModbusError::PortNotOpen);
        }
        if let Some(ref mut w) = *self.write_half.lock().await {
            w.write_all(data)
                .await
                .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
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
