use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationProtocol, ApplicationRole, Framing};
use crate::layers::physical::{ConnectionId, PhysicalLayer, ResponseFn};
use crate::types::{ApplicationDataUnit, CustomFcPredict, CustomFunctionCode, FramedDataUnit};
use crate::utils::{crc, predict_rtu_frame_length, PredictResult};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

const MAX_FRAME_LENGTH: usize = 256;
const MIN_FRAME_LENGTH: usize = 4;
const POOL_SIZE: usize = MAX_FRAME_LENGTH * 2;

/// Inter-frame timing for RTU. Mirrors njs-modbus
/// `intervalBetweenFrames?: { unit: 'bit' | 'ms'; value: number }`.
#[derive(Clone, Copy, Debug)]
pub enum FrameInterval {
    /// Number of bit-times used as the 3.5T approximation.
    Bits(f64),
    /// Direct millisecond override.
    Ms(u32),
}

/// Options for [`RtuApplicationLayer`]. Mirrors njs-modbus
/// `RtuApplicationLayerOptions`.
///
/// **Breaking change (v2)**: the constructor no longer takes separate
/// positional arguments; all timing parameters live here.
#[derive(Clone, Copy, Debug)]
pub struct RtuApplicationLayerOptions {
    pub interval_between_frames: Option<FrameInterval>,
    pub inter_char_timeout: Option<FrameInterval>,
    pub baud_rate: Option<u32>,
}

impl Default for RtuApplicationLayerOptions {
    fn default() -> Self {
        Self {
            interval_between_frames: None,
            inter_char_timeout: None,
            baud_rate: None,
        }
    }
}

pub struct RtuApplicationLayer {
    role: Arc<Mutex<Option<ApplicationRole>>>,
    framing_tx: broadcast::Sender<Framing>,
    framing_error_tx: broadcast::Sender<ModbusError>,
    buffers: Arc<Mutex<HashMap<ConnectionId, RtuBuffer>>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// User-defined predictors for non-standard FCs.
    custom_function_codes: Mutex<HashMap<u8, CustomFunctionCode>>,
    /// Computed millisecond timeout for the 3.5T inter-frame gap. `0` on Net
    /// transports.
    interval_ms: u32,
    /// Computed millisecond timeout for the t1.5 inter-character gap.
    /// `0` when disabled.
    inter_char_ms: u32,
    destroyed: AtomicBool,
}

/// Fixed-size byte pool for per-connection RTU frame buffering.
///
/// Mirrors njs-modbus `state.pool` + `start`/`end` indices. A single large
/// inbound chunk (e.g. 80 frames × 8 bytes = 640 bytes) is loop-consumed
/// into the pool; whenever the pool fills, `flush()` is invoked to extract
/// any complete frames before copying resumes. This prevents unbounded
/// `Vec` growth and eliminates the silent-truncation hazard that existed
/// when a `Buffer.copy` target was smaller than the source.
struct RtuBuffer {
    pool: Box<[u8]>,
    start: usize,
    end: usize,
    timer: Option<JoinHandle<()>>,
    inter_char_timer: Option<JoinHandle<()>>,
    t15_expired: bool,
}

impl RtuBuffer {
    fn new() -> Self {
        Self {
            pool: vec![0u8; POOL_SIZE].into_boxed_slice(),
            start: 0,
            end: 0,
            timer: None,
            inter_char_timer: None,
            t15_expired: false,
        }
    }

    fn len(&self) -> usize {
        self.end - self.start
    }

    fn is_empty(&self) -> bool {
        self.start == self.end
    }

    fn as_slice(&self) -> &[u8] {
        &self.pool[self.start..self.end]
    }

    fn available(&self) -> usize {
        self.pool.len() - self.end
    }

    /// Copy up to `data.len()` bytes (or `available()` bytes, whichever is
    /// smaller) into the pool at `end`. Returns the number of bytes copied.
    fn extend_from_slice(&mut self, data: &[u8]) -> usize {
        let n = data.len().min(self.available());
        self.pool[self.end..self.end + n].copy_from_slice(&data[..n]);
        self.end += n;
        n
    }

    /// Advance `start` by `n` bytes.
    fn drain(&mut self, n: usize) {
        self.start += n;
    }

    /// Shift unconsumed bytes to the front of the pool so `available()`
    /// reflects the true free space.
    fn compact(&mut self) {
        if self.start > 0 {
            if self.start < self.end {
                let len = self.end - self.start;
                self.pool.copy_within(self.start..self.end, 0);
                self.start = 0;
                self.end = len;
            } else {
                self.start = 0;
                self.end = 0;
            }
        }
    }

    fn clear(&mut self) {
        self.start = 0;
        self.end = 0;
    }
}


impl RtuApplicationLayer {
    /// Build an RTU application layer bound to `physical`.
    ///
    /// Options (all optional; see [`RtuApplicationLayerOptions`] for defaults):
    /// - `interval_between_frames` — overrides the default 3.5T computation.
    ///   * `FrameInterval::Bits(n)` — `n` bit-times (default 38.5 = 3.5 char times).
    ///   * `FrameInterval::Ms(n)` — explicit milliseconds.
    ///   * On serial with `None`: baud > 19200 → 1.75 ms (spec fix), else
    ///     `ceil((38.5 * 1000) / baud)`.
    /// - `inter_char_timeout` — opt-in t1.5. Disabled by default. Same units.
    ///   On serial: baud > 19200 → 0.75 ms, else `ceil((16.5 * 1000) / baud)`.
    /// - `baud_rate` — defaults to 9600 for Serial. Ignored on Net.
    pub fn new<P: PhysicalLayer + 'static>(
        physical: Arc<P>,
        options: RtuApplicationLayerOptions,
    ) -> Arc<Self> {
        let (interval_ms, inter_char_ms) =
            compute_interval_ms(physical.layer_type(), options);

        let (framing_tx, _) = broadcast::channel(64);
        let (framing_error_tx, _) = broadcast::channel(64);
        let buffers: Arc<Mutex<HashMap<ConnectionId, RtuBuffer>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let role: Arc<Mutex<Option<ApplicationRole>>> = Arc::new(Mutex::new(None));
        let app = Arc::new(Self {
            role: Arc::clone(&role),
            framing_tx: framing_tx.clone(),
            framing_error_tx: framing_error_tx.clone(),
            buffers: Arc::clone(&buffers),
            tasks: Mutex::new(Vec::new()),
            custom_function_codes: Mutex::new(HashMap::new()),
            interval_ms,
            inter_char_ms,
            destroyed: AtomicBool::new(false),
        });

        let mut data_rx = physical.subscribe_data();
        let buffers_for_data = Arc::clone(&buffers);
        let framing_tx_for_data = framing_tx.clone();
        let framing_error_tx_for_data = framing_error_tx.clone();
        let app_for_data = Arc::clone(&app);
        let data_task = tokio::spawn(async move {
            loop {
                match data_rx.recv().await {
                    Ok(event) => {
                        process_data_event(
                            &app_for_data,
                            &buffers_for_data,
                            &framing_tx_for_data,
                            &framing_error_tx_for_data,
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

    /// Register a custom function code predictor. Required for any non-standard
    /// FC; without registration the frame is rejected with a framing error.
    pub fn add_custom_function_code(&self, cfc: CustomFunctionCode) {
        self.custom_function_codes.lock().unwrap().insert(cfc.fc, cfc);
    }

    pub fn remove_custom_function_code(&self, fc: u8) {
        self.custom_function_codes.lock().unwrap().remove(&fc);
    }
}

pub(crate) fn compute_interval_ms(
    layer_type: crate::layers::physical::PhysicalLayerType,
    options: RtuApplicationLayerOptions,
) -> (u32, u32) {
    use crate::layers::physical::PhysicalLayerType;
    use crate::utils::bits_to_ms;

    let RtuApplicationLayerOptions {
        interval_between_frames,
        inter_char_timeout,
        baud_rate,
    } = options;

    match layer_type {
        PhysicalLayerType::Net => (0, 0),
        PhysicalLayerType::Serial => {
            let baud = baud_rate.unwrap_or(9600);

            let three_point_five_t = match interval_between_frames {
                Some(FrameInterval::Ms(n)) => n as f64,
                other => {
                    let bits = match other {
                        Some(FrameInterval::Bits(n)) => n,
                        _ => 38.5,
                    };
                    if baud > 19200 {
                        1.75
                    } else {
                        bits_to_ms(baud, bits).ceil()
                    }
                }
            };

            let one_point_five_t = match inter_char_timeout {
                Some(FrameInterval::Ms(n)) => n as f64,
                Some(FrameInterval::Bits(n)) => {
                    if baud > 19200 {
                        0.75
                    } else {
                        bits_to_ms(baud, n).ceil()
                    }
                }
                None => 0.0,
            };

            (
                three_point_five_t.max(0.0) as u32,
                one_point_five_t.max(0.0) as u32,
            )
        }
    }
}

fn process_data_event(
    app: &Arc<RtuApplicationLayer>,
    buffers: &Arc<Mutex<HashMap<ConnectionId, RtuBuffer>>>,
    framing_tx: &broadcast::Sender<Framing>,
    framing_error_tx: &broadcast::Sender<ModbusError>,
    data: Vec<u8>,
    response: ResponseFn,
    connection: ConnectionId,
) {
    let strict = app.interval_ms > 0;

    let mut guard = buffers.lock().unwrap();
    let mut buffer = guard.entry(Arc::clone(&connection)).or_insert_with(RtuBuffer::new);

    // t1.5 expiry from previous gap: if new data arrives after t1.5 fired,
    // the in-progress frame is corrupt.
    if buffer.t15_expired && buffer.len() > 0 {
        buffer.start = 0;
        buffer.end = 0;
        buffer.t15_expired = false;
        drop(guard);
        let _ = framing_error_tx.send(ModbusError::T1_5Exceeded);
        guard = buffers.lock().unwrap();
        buffer = guard.entry(Arc::clone(&connection)).or_insert_with(RtuBuffer::new);
    } else {
        buffer.t15_expired = false;
    }

    // Cancel pending timers — new data resets the silence window.
    if let Some(t) = buffer.timer.take() {
        t.abort();
    }
    if let Some(t) = buffer.inter_char_timer.take() {
        t.abort();
    }

    // Loop-consume the inbound chunk into the fixed-size pool.
    let mut data_offset = 0;
    while data_offset < data.len() {
        let copied = buffer.extend_from_slice(&data[data_offset..]);
        if copied == 0 {
            drop(guard);
            flush_pool(
                app,
                buffers,
                framing_tx,
                framing_error_tx,
                &connection,
                &response,
                strict,
            );
            guard = buffers.lock().unwrap();
            buffer = guard.entry(Arc::clone(&connection)).or_insert_with(RtuBuffer::new);
            if buffer.available() == 0 {
                let _ = framing_error_tx.send(ModbusError::InvalidData);
                buffer.clear();
                data_offset = data.len();
            }
            continue;
        }
        data_offset += copied;
    }

    let len_after = buffer.len();
    drop(guard);

    // Net mode: flush immediately. Serial mode: defer to t3.5 timer
    // (or flush now if the pool is at capacity).
    if app.interval_ms == 0 || len_after >= MAX_FRAME_LENGTH {
        flush_pool(
            app,
            buffers,
            framing_tx,
            framing_error_tx,
            &connection,
            &response,
            strict,
        );
    }

    // Arm t3.5 / t1.5 timers for Serial transports.
    if app.interval_ms > 0 && len_after < MAX_FRAME_LENGTH {
        let interval = app.interval_ms;
        let inter_char = app.inter_char_ms;
        let buffers_t = Arc::clone(buffers);
        let framing_tx_t = framing_tx.clone();
        let framing_error_tx_t = framing_error_tx.clone();
        let conn_t = Arc::clone(&connection);
        let response_t = Arc::clone(&response);
        let app_t = Arc::clone(app);

        let timer = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(interval as u64)).await;
            flush_pool(
                &app_t,
                &buffers_t,
                &framing_tx_t,
                &framing_error_tx_t,
                &conn_t,
                &response_t,
                interval > 0,
            );
        });

        let mut guard = buffers.lock().unwrap();
        if let Some(b) = guard.get_mut(&connection) {
            b.timer = Some(timer);

            if inter_char > 0 {
                let buffers_ic = Arc::clone(buffers);
                let conn_ic = Arc::clone(&connection);
                let inter_char_timer = tokio::spawn(async move {
                    tokio::time::sleep(tokio::time::Duration::from_millis(inter_char as u64))
                        .await;
                    let mut guard = buffers_ic.lock().unwrap();
                    if let Some(b) = guard.get_mut(&conn_ic) {
                        b.t15_expired = true;
                    }
                });
                b.inter_char_timer = Some(inter_char_timer);
            }
        }
    }
}

/// Flush complete frames from the per-connection pool. After extraction,
/// compact the pool so unconsumed bytes are shifted to the front.
fn flush_pool(
    app: &Arc<RtuApplicationLayer>,
    buffers: &Arc<Mutex<HashMap<ConnectionId, RtuBuffer>>>,
    framing_tx: &broadcast::Sender<Framing>,
    framing_error_tx: &broadcast::Sender<ModbusError>,
    connection: &ConnectionId,
    response: &ResponseFn,
    strict: bool,
) {
    let mut guard = buffers.lock().unwrap();
    let buffer = match guard.get_mut(connection) {
        Some(b) => b,
        None => return,
    };

    let is_response = matches!(app.role_snapshot(), Some(ApplicationRole::Master));
    let custom_fcs = app.custom_function_codes.lock().unwrap().clone();

    while !buffer.is_empty() {
        match try_extract(buffer.as_slice(), is_response, &custom_fcs) {
            ExtractResult::Frame { skip, frame_len } => {
                if skip > 0 {
                    buffer.drain(skip);
                }
                let frame_bytes: Vec<u8> = buffer.as_slice()[..frame_len].to_vec();
                buffer.drain(frame_len);
                let adu = ApplicationDataUnit {
                    transaction: None,
                    unit: frame_bytes[0],
                    fc: frame_bytes[1],
                    data: frame_bytes[2..frame_bytes.len() - 2].to_vec(),
                };
                let _ = framing_tx.send(Framing {
                    adu,
                    raw: frame_bytes,
                    response: Arc::clone(response),
                    connection: Arc::clone(connection),
                });
            }
            ExtractResult::Skip => {
                if strict {
                    let _ = framing_error_tx.send(ModbusError::CrcCheckFailed);
                    buffer.clear();
                    break;
                }
                buffer.drain(1);
            }
            ExtractResult::Insufficient => {
                if buffer.len() >= MAX_FRAME_LENGTH {
                    buffer.drain(1);
                    continue;
                }
                if strict {
                    let err = if buffer.t15_expired {
                        ModbusError::T1_5Exceeded
                    } else {
                        ModbusError::IncompleteFrame
                    };
                    let _ = framing_error_tx.send(err);
                    buffer.clear();
                    buffer.t15_expired = false;
                    break;
                }
                if buffer.t15_expired {
                    let _ = framing_error_tx.send(ModbusError::T1_5Exceeded);
                    buffer.clear();
                    buffer.t15_expired = false;
                }
                break;
            }
            ExtractResult::Invalid => {
                let _ = framing_error_tx.send(ModbusError::InvalidData);
                buffer.clear();
                break;
            }
        }
    }

    buffer.compact();
    if buffer.is_empty() {
        guard.remove(connection);
    }
}

enum ExtractResult {
    Frame { skip: usize, frame_len: usize },
    Insufficient,
    Skip,
    Invalid,
}

fn try_extract(
    buffer: &[u8],
    is_response: bool,
    custom_fcs: &HashMap<u8, CustomFunctionCode>,
) -> ExtractResult {
    if buffer.len() < MIN_FRAME_LENGTH {
        return ExtractResult::Insufficient;
    }

    let fc = buffer[1];

    // 1. User-registered custom FC predictor takes priority.
    if let Some(cfc) = custom_fcs.get(&fc) {
        let predictor = if is_response {
            &cfc.predict_response_length
        } else {
            &cfc.predict_request_length
        };
        match predictor(buffer) {
            CustomFcPredict::NeedMore => return ExtractResult::Insufficient,
            CustomFcPredict::Length(n) => return check_expected(buffer, n),
        }
    }

    // 2. Built-in predictor.
    match predict_rtu_frame_length(buffer, is_response) {
        PredictResult::Length(n) => check_expected(buffer, n),
        PredictResult::NeedMore => ExtractResult::Insufficient,
        PredictResult::Unknown => {
            // Non-standard FC with no registered predictor → framing error.
            // (The old slidingExtract fallback has been removed — Item #10.)
            ExtractResult::Invalid
        }
    }
}

fn check_expected(buffer: &[u8], expected: usize) -> ExtractResult {
    if expected > MAX_FRAME_LENGTH || expected < MIN_FRAME_LENGTH {
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
    ExtractResult::Skip
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
        if self.destroyed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut tasks = self.tasks.lock().unwrap();
        for task in tasks.drain(..) {
            task.abort();
        }
        self.buffers.lock().unwrap().clear();
        self.custom_function_codes.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::physical::PhysicalLayerType;

    #[test]
    fn test_compute_interval_ms_net_returns_zero() {
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Net,
                RtuApplicationLayerOptions::default()
            ),
            (0, 0)
        );
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Net,
                RtuApplicationLayerOptions {
                    baud_rate: Some(9600),
                    interval_between_frames: Some(FrameInterval::Ms(50)),
                    ..Default::default()
                }
            ),
            (0, 0),
            "Net always ignores baud/interval inputs"
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_default_9600() {
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Serial,
                RtuApplicationLayerOptions {
                    baud_rate: Some(9600),
                    ..Default::default()
                }
            ),
            (5, 0)
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_default_19200() {
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Serial,
                RtuApplicationLayerOptions {
                    baud_rate: Some(19200),
                    ..Default::default()
                }
            ),
            (3, 0)
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_above_19200_uses_spec_fixed() {
        // baud > 19200 → spec fixed 1.75 ms for t3.5, 0.75 ms for t1.5
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Serial,
                RtuApplicationLayerOptions {
                    baud_rate: Some(38400),
                    ..Default::default()
                }
            ),
            (1, 0)
        );
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Serial,
                RtuApplicationLayerOptions {
                    baud_rate: Some(115200),
                    inter_char_timeout: Some(FrameInterval::Bits(16.5)),
                    ..Default::default()
                }
            ),
            (1, 0)
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_explicit_ms() {
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Serial,
                RtuApplicationLayerOptions {
                    baud_rate: Some(9600),
                    interval_between_frames: Some(FrameInterval::Ms(20)),
                    ..Default::default()
                }
            ),
            (20, 0)
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_explicit_bits() {
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Serial,
                RtuApplicationLayerOptions {
                    baud_rate: Some(9600),
                    interval_between_frames: Some(FrameInterval::Bits(96.0)),
                    ..Default::default()
                }
            ),
            (10, 0)
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_default_baud_when_unspecified() {
        assert_eq!(
            compute_interval_ms(
                PhysicalLayerType::Serial,
                RtuApplicationLayerOptions::default()
            ),
            (5, 0)
        );
    }

    #[test]
    fn test_compute_interval_ms_serial_with_inter_char_timeout() {
        let (t35, t15) = compute_interval_ms(
            PhysicalLayerType::Serial,
            RtuApplicationLayerOptions {
                baud_rate: Some(9600),
                inter_char_timeout: Some(FrameInterval::Bits(21.0)),
                ..Default::default()
            },
        );
        assert_eq!(t35, 5);
        assert_eq!(t15, 3); // ceil(21.0 * 1000 / 9600) = ceil(2.1875) = 3
    }
}
