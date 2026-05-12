use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationRole, Framing};
use crate::layers::physical::PhysicalLayer;
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

pub struct TcpApplicationLayer {
    role: Mutex<Option<ApplicationRole>>,
    transaction_id: AtomicU16,
    framing_tx: broadcast::Sender<Framing>,
    framing_error_tx: broadcast::Sender<ModbusError>,
    _framing_rx: Mutex<broadcast::Receiver<Framing>>,
    _framing_error_rx: Mutex<broadcast::Receiver<ModbusError>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl TcpApplicationLayer {
    pub fn new<P: PhysicalLayer + 'static>(physical: Arc<P>) -> Arc<Self> {
        let (framing_tx, framing_rx) = broadcast::channel(64);
        let (framing_error_tx, framing_error_rx) = broadcast::channel(64);
        let app = Arc::new(Self {
            role: Mutex::new(None),
            transaction_id: AtomicU16::new(0),
            framing_tx: framing_tx.clone(),
            framing_error_tx: framing_error_tx.clone(),
            _framing_rx: Mutex::new(framing_rx),
            _framing_error_rx: Mutex::new(framing_error_rx),
            task: Mutex::new(None),
        });

        let mut data_rx = physical.subscribe_data();
        let task = tokio::spawn(async move {
            while let Ok(event) = data_rx.recv().await {
                match decode_frame(&event.data) {
                    Ok(adu) => {
                        let _ = framing_tx.send(Framing {
                            adu,
                            raw: event.data,
                            response: event.response,
                            connection: event.connection,
                        });
                    }
                    Err(err) => {
                        let _ = framing_error_tx.send(err);
                    }
                }
            }
        });
        *app.task.lock().unwrap() = Some(task);
        app
    }
}

fn decode_frame(data: &[u8]) -> Result<ApplicationDataUnit, ModbusError> {
    if data.len() < 8 {
        return Err(ModbusError::InsufficientData);
    }
    if data[2] != 0 || data[3] != 0 {
        return Err(ModbusError::InvalidData);
    }
    let len = u16::from_be_bytes([data[4], data[5]]) as usize;
    if len + 6 != data.len() {
        return Err(ModbusError::InvalidData);
    }
    let transaction = u16::from_be_bytes([data[0], data[1]]);
    Ok(ApplicationDataUnit {
        transaction: Some(transaction),
        unit: data[6],
        fc: data[7],
        data: data[8..].to_vec(),
    })
}

#[async_trait::async_trait]
impl ApplicationLayer for TcpApplicationLayer {
    fn set_role(&self, role: ApplicationRole) -> Result<(), ModbusError> {
        let mut guard = self.role.lock().unwrap();
        match *guard {
            Some(existing) if existing == role => Ok(()),
            Some(existing) => Err(ModbusError::InvalidState(format!(
                "application layer role already set to {existing:?}, cannot change to {role:?}"
            ))),
            None => {
                *guard = Some(role);
                Ok(())
            }
        }
    }

    fn role(&self) -> Option<ApplicationRole> {
        *self.role.lock().unwrap()
    }

    fn encode(&self, adu: &ApplicationDataUnit) -> Vec<u8> {
        let data_len = adu.data.len();
        let mut buf = vec![0u8; data_len + 8];
        let tx = adu.transaction.unwrap_or_else(|| {
            let current = self.transaction_id.fetch_add(1, Ordering::Relaxed);
            if current == 0 {
                self.transaction_id.store(1, Ordering::Relaxed);
            }
            current.wrapping_add(1)
        });
        buf[0..2].copy_from_slice(&tx.to_be_bytes());
        buf[2..4].copy_from_slice(&[0x00, 0x00]);
        buf[4..6].copy_from_slice(&((data_len + 2) as u16).to_be_bytes());
        buf[6] = adu.unit;
        buf[7] = adu.fc;
        buf[8..].copy_from_slice(&adu.data);
        buf
    }

    fn decode(&self, data: &[u8]) -> Result<FramedDataUnit, ModbusError> {
        let adu = decode_frame(data)?;
        Ok(FramedDataUnit {
            adu,
            raw: data.to_vec(),
        })
    }

    fn flush(&self) {
        // TCP framing is stateless in this commit; per-connection reassembly
        // buffers arrive in commit 3.
    }

    fn subscribe_framing(&self) -> broadcast::Receiver<Framing> {
        self.framing_tx.subscribe()
    }

    fn subscribe_framing_error(&self) -> broadcast::Receiver<ModbusError> {
        self.framing_error_tx.subscribe()
    }

    async fn destroy(&self) {
        if let Some(task) = self.task.lock().unwrap().take() {
            task.abort();
        }
    }
}
