use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationProtocol, ApplicationRole, Framing};
use crate::layers::physical::{ConnectionId, PhysicalLayer, ResponseFn};
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use crate::utils::lrc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

const HEX_ENCODE: [u8; 16] = *b"0123456789ABCDEF";
const COLON: u8 = b':';
const CR: u8 = b'\r';
const LF: u8 = b'\n';
/// Maximum ASCII payload (between `:` and `\r`) we will buffer per connection.
/// A Modbus ASCII frame encodes at most 256 bytes as 512 hex chars; we cap
/// here so a peer that never sends CR cannot grow the buffer without bound.
const MAX_ASCII_PAYLOAD: usize = 512;

/// Tunables for [`AsciiApplicationLayer::with_options`]. Mirrors njs-modbus
/// `AsciiApplicationLayerOptions`.
#[derive(Clone, Copy, Debug, Default)]
pub struct AsciiApplicationLayerOptions {
    /// When `true`, accept both lowercase (`a-f`) and uppercase (`A-F`) hex
    /// characters in inbound frames. The Modbus V1.1b3 §2.2 reference only
    /// specifies uppercase; some legacy peers emit lowercase. Defaults to
    /// `false` (strict — uppercase only).
    pub lenient_hex: bool,
}

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

fn is_hex_char(b: u8, lenient: bool) -> bool {
    matches!(b, b'0'..=b'9' | b'A'..=b'F') || (lenient && matches!(b, b'a'..=b'f'))
}

pub struct AsciiApplicationLayer {
    role: Mutex<Option<ApplicationRole>>,
    framing_tx: broadcast::Sender<Framing>,
    framing_error_tx: broadcast::Sender<ModbusError>,
    states: Arc<Mutex<HashMap<ConnectionId, ConnectionState>>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    pub lenient_hex: bool,
    destroyed: AtomicBool,
}

impl AsciiApplicationLayer {
    pub fn new<P: PhysicalLayer + 'static>(physical: Arc<P>) -> Arc<Self> {
        Self::with_options(physical, AsciiApplicationLayerOptions::default())
    }

    pub fn with_options<P: PhysicalLayer + 'static>(
        physical: Arc<P>,
        options: AsciiApplicationLayerOptions,
    ) -> Arc<Self> {
        let (framing_tx, _) = broadcast::channel(64);
        let (framing_error_tx, _) = broadcast::channel(64);
        let states: Arc<Mutex<HashMap<ConnectionId, ConnectionState>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let lenient_hex = options.lenient_hex;
        let app = Arc::new(Self {
            role: Mutex::new(None),
            framing_tx: framing_tx.clone(),
            framing_error_tx: framing_error_tx.clone(),
            states: Arc::clone(&states),
            tasks: Mutex::new(Vec::new()),
            lenient_hex,
            destroyed: AtomicBool::new(false),
        });

        let mut data_rx = physical.subscribe_data();
        let states_for_data = Arc::clone(&states);
        let framing_tx_for_data = framing_tx.clone();
        let framing_error_tx_for_data = framing_error_tx.clone();
        let data_task = tokio::spawn(async move {
            loop {
                match data_rx.recv().await {
                    Ok(event) => drive_fsm(
                        &states_for_data,
                        &framing_tx_for_data,
                        &framing_error_tx_for_data,
                        event.data,
                        event.response,
                        event.connection,
                        lenient_hex,
                    ),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        let mut close_rx = physical.subscribe_connection_close();
        let states_for_close = Arc::clone(&states);
        let close_task = tokio::spawn(async move {
            loop {
                match close_rx.recv().await {
                    Ok(conn_id) => {
                        states_for_close.lock().unwrap().remove(&conn_id);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
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
    lenient_hex: bool,
) {
    let mut completed_frames: Vec<Vec<u8>> = Vec::new();
    let mut overflows: u32 = 0;
    let mut invalid_hex: u32 = 0;
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
                    other => {
                        if state.frame.len() >= MAX_ASCII_PAYLOAD {
                            state.status = FsmStatus::Idle;
                            state.frame.clear();
                            overflows += 1;
                        } else if !is_hex_char(other, lenient_hex) {
                            // Reject non-hex characters immediately at reception
                            // time. Otherwise `:01GZ00AA\r\n` would slip through
                            // to LRC check; with a 1/256 LRC collision the bogus
                            // frame would be routed at unit=0x00 / fc=0x00.
                            state.status = FsmStatus::Idle;
                            state.frame.clear();
                            invalid_hex += 1;
                        } else {
                            state.frame.push(other);
                        }
                    }
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
                        state.status = FsmStatus::Idle;
                        state.frame.clear();
                    }
                },
            }
        }
        if matches!(state.status, FsmStatus::Idle) && state.frame.is_empty() {
            guard.remove(&connection);
        }
    }

    for _ in 0..overflows {
        let _ = framing_error_tx.send(ModbusError::InvalidData);
    }
    for _ in 0..invalid_hex {
        let _ = framing_error_tx.send(ModbusError::InvalidHex);
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
        let b = hex_decode_byte(chunk[0], chunk[1]).ok_or(ModbusError::InvalidHex)?;
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
        crate::layers::application::set_role_impl(&mut self.role.lock().unwrap(), role)
    }

    fn role(&self) -> Option<ApplicationRole> {
        *self.role.lock().unwrap()
    }

    fn protocol(&self) -> ApplicationProtocol {
        ApplicationProtocol::Ascii
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
        if self.destroyed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut tasks = self.tasks.lock().unwrap();
        for task in tasks.drain(..) {
            task.abort();
        }
        self.states.lock().unwrap().clear();
    }
}
