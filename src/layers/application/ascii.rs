use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationRole, Framing};
use crate::layers::physical::PhysicalLayer;
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use crate::utils::lrc;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

const HEX_ENCODE: [u8; 16] = *b"0123456789ABCDEF";

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
    _framing_rx: Mutex<broadcast::Receiver<Framing>>,
    _framing_error_rx: Mutex<broadcast::Receiver<ModbusError>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl AsciiApplicationLayer {
    pub fn new<P: PhysicalLayer + 'static>(physical: Arc<P>) -> Arc<Self> {
        let (framing_tx, framing_rx) = broadcast::channel(64);
        let (framing_error_tx, framing_error_rx) = broadcast::channel(64);
        let app = Arc::new(Self {
            role: Mutex::new(None),
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
    if data.len() < 10 {
        return Err(ModbusError::InsufficientData);
    }
    if data[0] != b':' || data[data.len() - 2] != b'\r' || data[data.len() - 1] != b'\n' {
        return Err(ModbusError::InvalidData);
    }
    let hex_len = data.len() - 3;
    if hex_len % 2 != 0 {
        return Err(ModbusError::InvalidData);
    }
    let mut bytes = Vec::with_capacity(hex_len / 2);
    for i in (0..hex_len).step_by(2) {
        let byte = hex_decode_byte(data[1 + i], data[2 + i]).ok_or(ModbusError::InvalidData)?;
        bytes.push(byte);
    }
    if bytes.len() < 3 {
        return Err(ModbusError::InsufficientData);
    }
    let frame_lrc = bytes[bytes.len() - 1];
    let computed = lrc(&bytes[..bytes.len() - 1]);
    if frame_lrc != computed {
        return Err(ModbusError::LrcCheckFailed);
    }
    Ok(ApplicationDataUnit {
        transaction: None,
        unit: bytes[0],
        fc: bytes[1],
        data: bytes[2..bytes.len() - 1].to_vec(),
    })
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
        frame.push(b':');
        for b in &buf {
            frame.push(HEX_ENCODE[(b >> 4) as usize]);
            frame.push(HEX_ENCODE[(b & 0xF) as usize]);
        }
        frame.extend_from_slice(b"\r\n");
        frame
    }

    fn decode(&self, data: &[u8]) -> Result<FramedDataUnit, ModbusError> {
        let adu = decode_frame(data)?;
        Ok(FramedDataUnit {
            adu,
            raw: data.to_vec(),
        })
    }

    fn flush(&self) {
        // Per-connection FSM state arrives in commit 3.
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
