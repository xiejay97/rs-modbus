use crate::error::ModbusError;
use crate::layers::physical::{PhysicalLayer, ResponseFn};
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct SerialPhysicalLayer {
    port: std::sync::Mutex<Option<Box<dyn serialport::SerialPort>>>,
    is_open: std::sync::Mutex<bool>,
    is_destroyed: std::sync::Mutex<bool>,
    settings: serialport::SerialPortSettings,
    path: String,
    data_tx: broadcast::Sender<(Vec<u8>, ResponseFn)>,
    error_tx: broadcast::Sender<ModbusError>,
    close_tx: broadcast::Sender<()>,
    _data_rx: std::sync::Mutex<broadcast::Receiver<(Vec<u8>, ResponseFn)>>,
    _error_rx: std::sync::Mutex<broadcast::Receiver<ModbusError>>,
    _close_rx: std::sync::Mutex<broadcast::Receiver<()>>,
}

impl SerialPhysicalLayer {
    pub fn new(path: String, baud_rate: u32) -> Arc<Self> {
        let (data_tx, data_rx) = broadcast::channel(16);
        let (error_tx, error_rx) = broadcast::channel(16);
        let (close_tx, close_rx) = broadcast::channel(16);
        let mut settings = serialport::SerialPortSettings::default();
        settings.baud_rate = baud_rate;
        Arc::new(Self {
            port: std::sync::Mutex::new(None),
            is_open: std::sync::Mutex::new(false),
            is_destroyed: std::sync::Mutex::new(false),
            settings,
            path,
            data_tx,
            error_tx,
            close_tx,
            _data_rx: std::sync::Mutex::new(data_rx),
            _error_rx: std::sync::Mutex::new(error_rx),
            _close_rx: std::sync::Mutex::new(close_rx),
        })
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for SerialPhysicalLayer {
    async fn open(&self) -> Result<(), ModbusError> {
        if *self.is_destroyed.lock().unwrap() {
            return Err(ModbusError::PortDestroyed);
        }
        let port = serialport::open_with_settings(&self.path, &self.settings)
            .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
        *self.port.lock().unwrap() = Some(port);
        *self.is_open.lock().unwrap() = true;

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open = self.is_open.clone();
        let port = self.port.clone();

        tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut buf = vec![0u8; 1024];
            loop {
                if let Ok(guard) = port.lock() {
                    if let Some(ref mut p) = *guard {
                        match p.read(&mut buf) {
                            Ok(0) => break,
                            Ok(n) => {
                                let data = buf[..n].to_vec();
                                let _ = data_tx
                                    .send((data, Arc::new(|_| Box::pin(async { Ok(()) }))));
                            }
                            Err(e) => {
                                let _ = error_tx.send(ModbusError::ConnectionError(e.to_string()));
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            if let Ok(mut guard) = is_open.lock() {
                *guard = false;
            }
            let _ = close_tx.send(());
        });

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
                Ok(())
            } else {
                Err(ModbusError::PortNotOpen)
            }
        } else {
            Err(ModbusError::PortNotOpen)
        }
    }

    async fn close(&self) -> Result<(), ModbusError> {
        *self.is_open.lock().unwrap() = false;
        *self.port.lock().unwrap() = None;
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
