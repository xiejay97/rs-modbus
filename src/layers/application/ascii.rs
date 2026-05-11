use crate::error::ModbusError;
use crate::layers::application::ApplicationLayer;
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use crate::utils::lrc;

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

#[derive(Default)]
pub struct AsciiApplicationLayer;

impl AsciiApplicationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl ApplicationLayer for AsciiApplicationLayer {
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
            let byte = hex_decode_byte(data[1 + i], data[2 + i])
                .ok_or(ModbusError::InvalidData)?;
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
        let unit = bytes[0];
        let fc = bytes[1];
        let payload = bytes[2..bytes.len() - 1].to_vec();
        Ok(FramedDataUnit {
            adu: ApplicationDataUnit {
                transaction: None,
                unit,
                fc,
                data: payload,
            },
            raw: data.to_vec(),
        })
    }
}
