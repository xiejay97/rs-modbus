use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationRole, Framing};
use crate::layers::physical::{ConnectionId, PhysicalLayer, ResponseFn};
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

const MAX_TCP_FRAME: usize = 260;

pub struct TcpApplicationLayer {
    role: Mutex<Option<ApplicationRole>>,
    transaction_id: AtomicU16,
    framing_tx: broadcast::Sender<Framing>,
    framing_error_tx: broadcast::Sender<ModbusError>,
    buffers: Arc<Mutex<HashMap<ConnectionId, Vec<u8>>>>,
    _framing_rx: Mutex<broadcast::Receiver<Framing>>,
    _framing_error_rx: Mutex<broadcast::Receiver<ModbusError>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl TcpApplicationLayer {
    pub fn new<P: PhysicalLayer + 'static>(physical: Arc<P>) -> Arc<Self> {
        let (framing_tx, framing_rx) = broadcast::channel(64);
        let (framing_error_tx, framing_error_rx) = broadcast::channel(64);
        let buffers: Arc<Mutex<HashMap<ConnectionId, Vec<u8>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let app = Arc::new(Self {
            role: Mutex::new(None),
            transaction_id: AtomicU16::new(0),
            framing_tx: framing_tx.clone(),
            framing_error_tx: framing_error_tx.clone(),
            buffers: Arc::clone(&buffers),
            _framing_rx: Mutex::new(framing_rx),
            _framing_error_rx: Mutex::new(framing_error_rx),
            tasks: Mutex::new(Vec::new()),
        });

        // Data ingestion task.
        let mut data_rx = physical.subscribe_data();
        let buffers_for_data = Arc::clone(&buffers);
        let framing_tx_for_data = framing_tx.clone();
        let framing_error_tx_for_data = framing_error_tx.clone();
        let data_task = tokio::spawn(async move {
            while let Ok(event) = data_rx.recv().await {
                process_data_event(
                    &buffers_for_data,
                    &framing_tx_for_data,
                    &framing_error_tx_for_data,
                    event.data,
                    event.response,
                    event.connection,
                );
            }
        });

        // Connection-close janitor: discard the buffer for closed connections.
        let mut close_rx = physical.subscribe_connection_close();
        let buffers_for_close = Arc::clone(&buffers);
        let close_task = tokio::spawn(async move {
            while let Ok(conn_id) = close_rx.recv().await {
                buffers_for_close.lock().unwrap().remove(&conn_id);
            }
        });

        app.tasks.lock().unwrap().extend([data_task, close_task]);
        app
    }
}

fn process_data_event(
    buffers: &Arc<Mutex<HashMap<ConnectionId, Vec<u8>>>>,
    framing_tx: &broadcast::Sender<Framing>,
    framing_error_tx: &broadcast::Sender<ModbusError>,
    data: Vec<u8>,
    response: ResponseFn,
    connection: ConnectionId,
) {
    let mut guard = buffers.lock().unwrap();
    let buffer = guard.entry(Arc::clone(&connection)).or_default();
    buffer.extend_from_slice(&data);

    loop {
        match try_extract_frame(buffer) {
            ExtractResult::Frame(frame_bytes) => {
                let raw = frame_bytes.clone();
                buffer.drain(..frame_bytes.len());
                let _ = framing_tx.send(Framing {
                    adu: decode_inner(&frame_bytes).expect("checked by try_extract"),
                    raw,
                    response: Arc::clone(&response),
                    connection: Arc::clone(&connection),
                });
            }
            ExtractResult::Insufficient => break,
            ExtractResult::Invalid => {
                let _ = framing_error_tx.send(ModbusError::InvalidData);
                buffer.clear();
                break;
            }
        }
    }

    if buffer.is_empty() {
        guard.remove(&connection);
    }
}

enum ExtractResult {
    Frame(Vec<u8>),
    Insufficient,
    Invalid,
}

fn try_extract_frame(buffer: &[u8]) -> ExtractResult {
    if buffer.len() < 8 {
        return ExtractResult::Insufficient;
    }
    if buffer[2] != 0 || buffer[3] != 0 {
        return ExtractResult::Invalid;
    }
    let length = u16::from_be_bytes([buffer[4], buffer[5]]) as usize;
    let total = 6 + length;
    if total > MAX_TCP_FRAME || length < 2 {
        return ExtractResult::Invalid;
    }
    if buffer.len() < total {
        return ExtractResult::Insufficient;
    }
    ExtractResult::Frame(buffer[..total].to_vec())
}

fn decode_inner(data: &[u8]) -> Result<ApplicationDataUnit, ModbusError> {
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
        let adu = decode_inner(data)?;
        Ok(FramedDataUnit {
            adu,
            raw: data.to_vec(),
        })
    }

    fn flush(&self) {
        self.buffers.lock().unwrap().clear();
    }

    fn subscribe_framing(&self) -> broadcast::Receiver<Framing> {
        self.framing_tx.subscribe()
    }

    fn subscribe_framing_error(&self) -> broadcast::Receiver<ModbusError> {
        self.framing_error_tx.subscribe()
    }

    async fn destroy(&self) {
        let mut tasks = self.tasks.lock().unwrap();
        for task in tasks.drain(..) {
            task.abort();
        }
        self.buffers.lock().unwrap().clear();
    }
}
