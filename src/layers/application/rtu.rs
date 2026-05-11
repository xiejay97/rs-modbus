use crate::error::ModbusError;
use crate::layers::application::ApplicationLayer;
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use crate::utils::crc;

pub struct RtuApplicationLayer;

impl RtuApplicationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RtuApplicationLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplicationLayer for RtuApplicationLayer {
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
