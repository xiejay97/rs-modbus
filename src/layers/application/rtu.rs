use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationProtocol, ApplicationRole, Framing};
use crate::layers::physical::{ConnectionId, PhysicalLayer, ResponseFn};
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use crate::utils::{crc, crc_with_seed, predict_rtu_frame_length};
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

        let (framing_tx, _) = broadcast::channel(64);
        let (framing_error_tx, _) = broadcast::channel(64);
        let buffers: Arc<Mutex<HashMap<ConnectionId, Vec<u8>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let role: Arc<Mutex<Option<ApplicationRole>>> = Arc::new(Mutex::new(None));
        let app = Arc::new(Self {
            role: Arc::clone(&role),
            framing_tx: framing_tx.clone(),
            framing_error_tx: framing_error_tx.clone(),
            buffers: Arc::clone(&buffers),
            tasks: Mutex::new(Vec::new()),
            interval_ms,
        });

        let mut data_rx = physical.subscribe_data();
        let buffers_for_data = Arc::clone(&buffers);
        let framing_tx_for_data = framing_tx.clone();
        let framing_error_tx_for_data = framing_error_tx.clone();
        let role_for_data = Arc::clone(&role);
        let data_task = tokio::spawn(async move {
            loop {
                match data_rx.recv().await {
                    Ok(event) => {
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
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        let mut close_rx = physical.subscribe_connection_close();
        let buffers_for_close = Arc::clone(&buffers);
        let close_task = tokio::spawn(async move {
            loop {
                match close_rx.recv().await {
                    Ok(conn_id) => {
                        buffers_for_close.lock().unwrap().remove(&conn_id);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
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
                let frame_bytes: Vec<u8> = buffer.drain(..frame_len).collect();
                let adu = ApplicationDataUnit {
                    transaction: None,
                    unit: frame_bytes[0],
                    fc: frame_bytes[1],
                    data: frame_bytes[2..frame_bytes.len() - 2].to_vec(),
                };
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
        // Running CRC over remaining[..len - 2]; advances by one byte per
        // length increment so the inner loop is O(max_len) instead of
        // O(max_len^2). Matches njs-modbus slidingExtract's runningCrc.
        let mut running_crc = crc(&remaining[..MIN_FRAME_LENGTH - 2]);
        for len in MIN_FRAME_LENGTH..=max_len {
            let frame_crc = u16::from_le_bytes([remaining[len - 2], remaining[len - 1]]);
            if frame_crc == running_crc {
                return ExtractResult::Frame {
                    skip: start,
                    frame_len: len,
                };
            }
            if len < max_len {
                running_crc = crc_with_seed(&remaining[len - 2..len - 1], running_crc);
            }
        }
    }
    // No frame found at any offset. If buffer hasn't reached MAX_FRAME_LENGTH
    // yet, wait for more data — an unpredictable-FC response (e.g. FC 43/14)
    // may still be arriving in fragments. Once buffer fills, dropping one
    // byte is the only way to make progress, bounding steady-state buffering
    // at ~MAX_FRAME_LENGTH per connection. Mirrors njs-modbus slidingExtract.
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

    fn protocol(&self) -> ApplicationProtocol {
        ApplicationProtocol::Rtu
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

    // ===== sliding_extract =====

    #[test]
    fn test_sliding_extract_waits_when_under_max_and_no_match() {
        // Unpredictable-FC frame still in transit — predict returned None
        // and the partial bytes don't form a valid CRC at any length yet.
        // Must NOT drop bytes; wait for more data so e.g. a fragmented
        // FC 43/14 response on TCP-RTU can be reassembled.
        let buffer = vec![0xaa; 100];
        assert!(matches!(
            sliding_extract(&buffer),
            ExtractResult::Insufficient
        ));
    }

    #[test]
    fn test_sliding_extract_skips_when_at_max_and_no_match() {
        // Once buffer reaches MAX_FRAME_LENGTH bytes with no valid CRC,
        // dropping one byte is the only way to make progress. This bounds
        // steady-state buffering at ~MAX_FRAME_LENGTH per connection.
        let buffer = vec![0xaa; MAX_FRAME_LENGTH];
        assert!(matches!(sliding_extract(&buffer), ExtractResult::Skip));
    }

    #[test]
    fn test_sliding_extract_returns_frame_at_offset_0() {
        // Unpredictable-FC frame (e.g. FC 43 response) sitting at offset 0.
        let mut frame = vec![0x01u8, 0x2b, 0x0e];
        let c = crate::utils::crc(&frame);
        frame.extend_from_slice(&c.to_le_bytes());
        match sliding_extract(&frame) {
            ExtractResult::Frame { skip, frame_len } => {
                assert_eq!(skip, 0);
                assert_eq!(frame_len, 5);
            }
            _ => panic!("expected Frame"),
        }
    }

    #[test]
    fn test_sliding_extract_finds_frame_at_later_offset() {
        // sliding_extract scans every starting offset (matching njs-modbus
        // slidingExtract). A valid frame buried after some prefix junk is
        // found in one call, with `skip` indicating how many leading bytes
        // to drop. This is the common recovery path for serial noise / TCP
        // segment seams in industrial deployments.
        let mut frame = vec![0x01u8, 0x2b, 0x0e];
        let c = crate::utils::crc(&frame);
        frame.extend_from_slice(&c.to_le_bytes());
        // Prepend 3 bytes of junk so the valid frame starts at offset 3.
        let mut buffer = vec![0xffu8, 0x00, 0xfe];
        buffer.extend_from_slice(&frame);
        match sliding_extract(&buffer) {
            ExtractResult::Frame { skip, frame_len } => {
                assert_eq!(skip, 3);
                assert_eq!(frame_len, 5);
            }
            _ => panic!("expected Frame at skip=3"),
        }
    }
}
