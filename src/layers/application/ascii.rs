use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationRole, Framing};
use crate::layers::physical::{ConnectionId, PhysicalLayer, ResponseFn};
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use crate::utils::lrc;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

const HEX_ENCODE: [u8; 16] = *b"0123456789ABCDEF";
const COLON: u8 = b':';
const CR: u8 = b'\r';
const LF: u8 = b'\n';

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
enum FsmStatus {
    #[default]
    Idle,
    Reception,
    WaitingEnd,
}

#[derive(Default)]
struct ConnectionState {
    status: FsmStatus,
    frame: Vec<u8>,
}

fn hex_decode_byte(hi: u8, lo: u8) -> Option<u8> {
    let hi = match hi {
        b'0'..=b'9' => hi - b'0',
        b'A'..=b'F' => hi - b'A' + 10,
        b'a'..=b'f' => hi - b'a' + 10,
        _ => return None,
    };
    let lo = match lo {
        b'0'..=b'9' => lo - b'0',
        b'A'..=b'F' => lo - b'A' + 10,
        b'a'..=b'f' => lo - b'a' + 10,
        _ => return None,
    };
    Some((hi << 4) | lo)
}

pub struct AsciiApplicationLayer {
    role: Mutex<Option<ApplicationRole>>,
    framing_tx: broadcast::Sender<Framing>,
    framing_error_tx: broadcast::Sender<ModbusError>,
    states: Arc<Mutex<HashMap<ConnectionId, ConnectionState>>>,
    _framing_rx: Mutex<broadcast::Receiver<Framing>>,
    _framing_error_rx: Mutex<broadcast::Receiver<ModbusError>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl AsciiApplicationLayer {
    pub fn new<P: PhysicalLayer + 'static>(physical: Arc<P>) -> Arc<Self> {
        let (framing_tx, framing_rx) = broadcast::channel(64);
        let (framing_error_tx, framing_error_rx) = broadcast::channel(64);
        let states: Arc<Mutex<HashMap<ConnectionId, ConnectionState>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let app = Arc::new(Self {
            role: Mutex::new(None),
            framing_tx: framing_tx.clone(),
            framing_error_tx: framing_error_tx.clone(),
            states: Arc::clone(&states),
            _framing_rx: Mutex::new(framing_rx),
            _framing_error_rx: Mutex::new(framing_error_rx),
            tasks: Mutex::new(Vec::new()),
        });

        let mut data_rx = physical.subscribe_data();
        let states_for_data = Arc::clone(&states);
        let framing_tx_for_data = framing_tx.clone();
        let framing_error_tx_for_data = framing_error_tx.clone();
        let data_task = tokio::spawn(async move {
            while let Ok(event) = data_rx.recv().await {
                drive_fsm(
                    &states_for_data,
                    &framing_tx_for_data,
                    &framing_error_tx_for_data,
                    event.data,
                    event.response,
                    event.connection,
                );
            }
        });

        let mut close_rx = physical.subscribe_connection_close();
        let states_for_close = Arc::clone(&states);
        let close_task = tokio::spawn(async move {
            while let Ok(conn_id) = close_rx.recv().await {
                states_for_close.lock().unwrap().remove(&conn_id);
            }
        });

        app.tasks.lock().unwrap().extend([data_task, close_task]);
        app
    }
}

fn drive_fsm(
    states: &Arc<Mutex<HashMap<ConnectionId, ConnectionState>>>,
    framing_tx: &broadcast::Sender<Framing>,
    framing_error_tx: &broadcast::Sender<ModbusError>,
    data: Vec<u8>,
    response: ResponseFn,
    connection: ConnectionId,
) {
    let mut completed_frames: Vec<Vec<u8>> = Vec::new();
    {
        let mut guard = states.lock().unwrap();
        let state = guard.entry(Arc::clone(&connection)).or_default();
        for byte in data {
            match state.status {
                FsmStatus::Idle => {
                    if byte == COLON {
                        state.status = FsmStatus::Reception;
                        state.frame.clear();
                    }
                }
                FsmStatus::Reception => match byte {
                    COLON => {
                        state.frame.clear();
                    }
                    CR => {
                        state.status = FsmStatus::WaitingEnd;
                    }
                    other => state.frame.push(other),
                },
                FsmStatus::WaitingEnd => match byte {
                    COLON => {
                        state.status = FsmStatus::Reception;
                        state.frame.clear();
                    }
                    LF => {
                        completed_frames.push(std::mem::take(&mut state.frame));
                        state.status = FsmStatus::Idle;
                    }
                    _ => {
                        // CR not followed by LF: discard partial frame.
                        state.status = FsmStatus::Idle;
                        state.frame.clear();
                    }
                },
            }
        }
        // Cleanup: if FSM ended in idle and buffer empty, drop the entry.
        if matches!(state.status, FsmStatus::Idle) && state.frame.is_empty() {
            guard.remove(&connection);
        }
    }

    for ascii_payload in completed_frames {
        match decode_payload(&ascii_payload) {
            Ok((adu, raw)) => {
                let _ = framing_tx.send(Framing {
                    adu,
                    raw,
                    response: Arc::clone(&response),
                    connection: Arc::clone(&connection),
                });
            }
            Err(err) => {
                let _ = framing_error_tx.send(err);
            }
        }
    }
}

/// `payload` is the ASCII payload between `:` and `\r`, exclusive. Decode it
/// to bytes, verify LRC, and return the ADU plus the raw ASCII frame
/// (including the framing characters) for inclusion in `Framing.raw`.
fn decode_payload(payload: &[u8]) -> Result<(ApplicationDataUnit, Vec<u8>), ModbusError> {
    if payload.len() % 2 != 0 {
        return Err(ModbusError::InvalidData);
    }
    let mut bytes = Vec::with_capacity(payload.len() / 2);
    for chunk in payload.chunks(2) {
        let b = hex_decode_byte(chunk[0], chunk[1]).ok_or(ModbusError::InvalidData)?;
        bytes.push(b);
    }
    if bytes.len() < 3 {
        return Err(ModbusError::InsufficientData);
    }
    let frame_lrc = bytes[bytes.len() - 1];
    let computed = lrc(&bytes[..bytes.len() - 1]);
    if frame_lrc != computed {
        return Err(ModbusError::LrcCheckFailed);
    }
    let adu = ApplicationDataUnit {
        transaction: None,
        unit: bytes[0],
        fc: bytes[1],
        data: bytes[2..bytes.len() - 1].to_vec(),
    };
    let mut raw = Vec::with_capacity(payload.len() + 3);
    raw.push(COLON);
    raw.extend_from_slice(payload);
    raw.push(CR);
    raw.push(LF);
    Ok((adu, raw))
}

#[async_trait::async_trait]
impl ApplicationLayer for AsciiApplicationLayer {
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
        let mut buf = vec![adu.unit, adu.fc];
        buf.extend_from_slice(&adu.data);
        buf.push(lrc(&buf));
        let mut frame = Vec::with_capacity(1 + buf.len() * 2 + 2);
        frame.push(COLON);
        for b in &buf {
            frame.push(HEX_ENCODE[(b >> 4) as usize]);
            frame.push(HEX_ENCODE[(b & 0x0f) as usize]);
        }
        frame.extend_from_slice(b"\r\n");
        frame
    }

    fn decode(&self, data: &[u8]) -> Result<FramedDataUnit, ModbusError> {
        if data.len() < 10 {
            return Err(ModbusError::InsufficientData);
        }
        if data[0] != COLON || data[data.len() - 2] != CR || data[data.len() - 1] != LF {
            return Err(ModbusError::InvalidData);
        }
        let payload = &data[1..data.len() - 2];
        let (adu, _) = decode_payload(payload)?;
        Ok(FramedDataUnit {
            adu,
            raw: data.to_vec(),
        })
    }

    fn flush(&self) {
        self.states.lock().unwrap().clear();
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
        self.states.lock().unwrap().clear();
    }
}
