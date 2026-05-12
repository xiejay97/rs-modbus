use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationRole, Framing};
use crate::layers::physical::PhysicalLayer;
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use crate::utils::crc;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

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
    role: Mutex<Option<ApplicationRole>>,
    framing_tx: broadcast::Sender<Framing>,
    framing_error_tx: broadcast::Sender<ModbusError>,
    _framing_rx: Mutex<broadcast::Receiver<Framing>>,
    _framing_error_rx: Mutex<broadcast::Receiver<ModbusError>>,
    task: Mutex<Option<JoinHandle<()>>>,
    /// Computed millisecond timeout for the 3.5T inter-frame gap. `0` on Net
    /// transports (no inter-frame timer; treat each `DataEvent` as one frame).
    /// Used by the stateful framing logic introduced in a later commit.
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
    /// - `None` on serial: 1.8 ms when `baud_rate > 19200`, else
    ///   `(48 * 1000) / baud_rate` rounded up.
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
        let app = Arc::new(Self {
            role: Mutex::new(None),
            framing_tx: framing_tx.clone(),
            framing_error_tx: framing_error_tx.clone(),
            _framing_rx: Mutex::new(framing_rx),
            _framing_error_rx: Mutex::new(framing_error_rx),
            task: Mutex::new(None),
            interval_ms,
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

impl Default for RtuApplicationLayer {
    fn default() -> Self {
        unreachable!("use RtuApplicationLayer::new(physical, ..) instead");
    }
}

fn compute_interval_ms(
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
                    // Modbus spec: at high baud rates the inter-frame delay is
                    // fixed at 1.75 ms (round up to 2 to be safe; njs uses
                    // ceil(1.8) = 2 as well).
                    2
                } else {
                    let exact = (bits as f64 * 1000.0) / baud as f64;
                    exact.ceil() as u32
                }
            }
        },
    }
}

fn decode_frame(data: &[u8]) -> Result<ApplicationDataUnit, ModbusError> {
    if data.len() < 4 {
        return Err(ModbusError::InsufficientData);
    }
    let frame_crc = u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]);
    let computed = crc(&data[..data.len() - 2]);
    if frame_crc != computed {
        return Err(ModbusError::CrcCheckFailed);
    }
    let unit = data[0];
    let fc = data[1];
    let payload = data[2..data.len() - 2].to_vec();
    Ok(ApplicationDataUnit {
        transaction: None,
        unit,
        fc,
        data: payload,
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
        *self.role.lock().unwrap()
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
        let adu = decode_frame(data)?;
        Ok(FramedDataUnit {
            adu,
            raw: data.to_vec(),
        })
    }

    fn flush(&self) {
        // Per-connection buffers + 3.5T timer arrive in commit 3.
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
