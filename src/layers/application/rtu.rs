use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationRole, Framing};
use crate::layers::physical::{ConnectionId, PhysicalLayer, ResponseFn};
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use crate::utils::{crc, predict_rtu_frame_length};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

const MAX_FRAME_LENGTH: usize = 256;
const MIN_FRAME_LENGTH: usize = 4;

/// Inter-frame timing for RTU. Mirrors njs-modbus
/// `intervalBetweenFrames?: { unit: 'bit' | 'ms'; value: number }`.
#[derive(Clone, Copy, Debug)]
pub enum FrameInterval {
    /// Number of bit-times used as the 3.5T approximation. njs default is 48.
    Bits(u32),
    /// Direct millisecond override.
    Ms(u32),
}

pub struct RtuApplicationLayer {
    role: Arc<Mutex<Option<ApplicationRole>>>,
    framing_tx: broadcast::Sender<Framing>,
    framing_error_tx: broadcast::Sender<ModbusError>,
    buffers: Arc<Mutex<HashMap<ConnectionId, Vec<u8>>>>,
    _framing_rx: Mutex<broadcast::Receiver<Framing>>,
    _framing_error_rx: Mutex<broadcast::Receiver<ModbusError>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Computed millisecond timeout for the 3.5T inter-frame gap. `0` on Net
    /// transports. Currently unused at runtime (frame extraction is purely
    /// driven by data arrival and CRC), kept for future timer-based flush.
    #[allow(dead_code)]
    interval_ms: u32,
}

impl RtuApplicationLayer {
    /// Build an RTU application layer bound to `physical`.
    ///
    /// `baud_rate` is required when `physical.layer_type() == Serial` and
    /// `interval_between_frames` is `None` (so the layer can compute 3.5T from
    /// it). For network transports it is ignored.
    ///
    /// `interval_between_frames` overrides the default 3.5T computation:
    /// - `Some(FrameInterval::Ms(n))` — use `n` ms directly.
    /// - `Some(FrameInterval::Bits(n))` — use `n` bit-times instead of 48.
    /// - `None` on serial: 2 ms when `baud_rate > 19200`, else
    ///   `ceil((48 * 1000) / baud_rate)`.
    /// - `None` on net: 0 (flush every chunk immediately).
    pub fn new<P: PhysicalLayer + 'static>(
        physical: Arc<P>,
        baud_rate: Option<u32>,
        interval_between_frames: Option<FrameInterval>,
    ) -> Arc<Self> {
        let interval_ms = compute_interval_ms(
            physical.layer_type(),
            baud_rate,
            interval_between_frames,
        );

        let (framing_tx, framing_rx) = broadcast::channel(64);
        let (framing_error_tx, framing_error_rx) = broadcast::channel(64);
        let buffers: Arc<Mutex<HashMap<ConnectionId, Vec<u8>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let role: Arc<Mutex<Option<ApplicationRole>>> = Arc::new(Mutex::new(None));
        let app = Arc::new(Self {
            role: Arc::clone(&role),
            framing_tx: framing_tx.clone(),
            framing_error_tx: framing_error_tx.clone(),
            buffers: Arc::clone(&buffers),
            _framing_rx: Mutex::new(framing_rx),
            _framing_error_rx: Mutex::new(framing_error_rx),
            tasks: Mutex::new(Vec::new()),
            interval_ms,
        });

        // Data ingestion task.
        let mut data_rx = physical.subscribe_data();
        let buffers_for_data = Arc::clone(&buffers);
        let framing_tx_for_data = framing_tx.clone();
        let framing_error_tx_for_data = framing_error_tx.clone();
        let role_for_data = Arc::clone(&role);
        let data_task = tokio::spawn(async move {
            while let Ok(event) = data_rx.recv().await {
                let role_snapshot = *role_for_data.lock().unwrap();
                process_data_event(
                    &buffers_for_data,
                    &framing_tx_for_data,
                    &framing_error_tx_for_data,
                    role_snapshot,
                    event.data,
                    event.response,
                    event.connection,
                );
            }
        });

        // Connection-close janitor.
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

    fn role_snapshot(&self) -> Option<ApplicationRole> {
        *self.role.lock().unwrap()
    }
}

pub(crate) fn compute_interval_ms(
    layer_type: crate::layers::physical::PhysicalLayerType,
    baud_rate: Option<u32>,
    interval_between_frames: Option<FrameInterval>,
) -> u32 {
    use crate::layers::physical::PhysicalLayerType;
    match layer_type {
        PhysicalLayerType::Net => 0,
        PhysicalLayerType::Serial => match interval_between_frames {
            Some(FrameInterval::Ms(n)) => n,
            other => {
                let bits = match other {
                    Some(FrameInterval::Bits(n)) => n,
                    _ => 48,
                };
                let baud = baud_rate.unwrap_or(9600);
                if baud > 19200 {
                    2
                } else {
                    let exact = (bits as f64 * 1000.0) / baud as f64;
                    exact.ceil() as u32
                }
            }
        },
    }
}

fn process_data_event(
    buffers: &Arc<Mutex<HashMap<ConnectionId, Vec<u8>>>>,
    framing_tx: &broadcast::Sender<Framing>,
    framing_error_tx: &broadcast::Sender<ModbusError>,
    role: Option<ApplicationRole>,
    data: Vec<u8>,
    response: ResponseFn,
    connection: ConnectionId,
) {
    let mut guard = buffers.lock().unwrap();
    let buffer = guard.entry(Arc::clone(&connection)).or_default();
    buffer.extend_from_slice(&data);

    let is_response = matches!(role, Some(ApplicationRole::Master));

    loop {
        match try_extract(buffer, is_response) {
            ExtractResult::Frame { skip, frame_len } => {
                if skip > 0 {
                    buffer.drain(..skip);
                }
                let frame_bytes: Vec<u8> = buffer[..frame_len].to_vec();
                buffer.drain(..frame_len);
                let adu = decode_inner(&frame_bytes).expect("checked by try_extract");
                let _ = framing_tx.send(Framing {
                    adu,
                    raw: frame_bytes,
                    response: Arc::clone(&response),
                    connection: Arc::clone(&connection),
                });
            }
            ExtractResult::Skip => {
                buffer.drain(..1);
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
    Frame { skip: usize, frame_len: usize },
    Insufficient,
    Skip,
    Invalid,
}

fn try_extract(buffer: &[u8], is_response: bool) -> ExtractResult {
    if buffer.len() < MIN_FRAME_LENGTH {
        return ExtractResult::Insufficient;
    }
    if let Some(expected) = predict_rtu_frame_length(buffer, is_response) {
        if expected > MAX_FRAME_LENGTH {
            return ExtractResult::Invalid;
        }
        if buffer.len() < expected {
            return ExtractResult::Insufficient;
        }
        if crc_matches(buffer, expected) {
            return ExtractResult::Frame {
                skip: 0,
                frame_len: expected,
            };
        }
        // Predict matched but CRC failed — corruption or wrong alignment.
        // Drop one byte and retry.
        return ExtractResult::Skip;
    }
    sliding_extract(buffer)
}

fn sliding_extract(buffer: &[u8]) -> ExtractResult {
    let last_start = buffer.len().saturating_sub(MIN_FRAME_LENGTH);
    for start in 0..=last_start {
        let remaining = &buffer[start..];
        let max_len = remaining.len().min(MAX_FRAME_LENGTH);
        for len in MIN_FRAME_LENGTH..=max_len {
            if crc_matches(remaining, len) {
                return ExtractResult::Frame {
                    skip: start,
                    frame_len: len,
                };
            }
        }
    }
    if buffer.len() >= MAX_FRAME_LENGTH {
        ExtractResult::Skip
    } else {
        ExtractResult::Insufficient
    }
}

fn crc_matches(buffer: &[u8], length: usize) -> bool {
    if length < 2 || length > buffer.len() {
        return false;
    }
    let frame_crc = u16::from_le_bytes([buffer[length - 2], buffer[length - 1]]);
    let computed = crc(&buffer[..length - 2]);
    frame_crc == computed
}

fn decode_inner(data: &[u8]) -> Result<ApplicationDataUnit, ModbusError> {
    if data.len() < 4 {
        return Err(ModbusError::InsufficientData);
    }
    let frame_crc = u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]);
    let computed = crc(&data[..data.len() - 2]);
    if frame_crc != computed {
        return Err(ModbusError::CrcCheckFailed);
    }
    Ok(ApplicationDataUnit {
        transaction: None,
        unit: data[0],
        fc: data[1],
        data: data[2..data.len() - 2].to_vec(),
    })
}

#[async_trait::async_trait]
impl ApplicationLayer for RtuApplicationLayer {
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
        self.role_snapshot()
    }

    fn encode(&self, adu: &ApplicationDataUnit) -> Vec<u8> {
        let data_len = adu.data.len();
        let payload_len = data_len + 2;
        let mut buf = vec![0u8; payload_len + 2];
        buf[0] = adu.unit;
        buf[1] = adu.fc;
        buf[2..payload_len].copy_from_slice(&adu.data);
        let c = crc(&buf[..payload_len]);
        buf[payload_len..].copy_from_slice(&c.to_le_bytes());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::physical::PhysicalLayerType;

    #[test]
    fn test_compute_interval_ms_net_returns_zero() {
        assert_eq!(compute_interval_ms(PhysicalLayerType::Net, None, None), 0);
        assert_eq!(
            compute_interval_ms(PhysicalLayerType::Net, Some(9600), Some(FrameInterval::Ms(50))),
            0,
            "Net always ignores baud/interval inputs"
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_default_9600() {
        assert_eq!(
            compute_interval_ms(PhysicalLayerType::Serial, Some(9600), None),
            5
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_default_19200() {
        assert_eq!(
            compute_interval_ms(PhysicalLayerType::Serial, Some(19200), None),
            3
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_above_19200_uses_fixed() {
        assert_eq!(
            compute_interval_ms(PhysicalLayerType::Serial, Some(38400), None),
            2
        );
        assert_eq!(
            compute_interval_ms(PhysicalLayerType::Serial, Some(115200), None),
            2
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_explicit_ms() {
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Serial,
                Some(9600),
                Some(FrameInterval::Ms(20))
            ),
            20
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_explicit_bits() {
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Serial,
                Some(9600),
                Some(FrameInterval::Bits(96))
            ),
            10
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_default_baud_when_unspecified() {
        assert_eq!(compute_interval_ms(PhysicalLayerType::Serial, None, None), 5);
    }
}
