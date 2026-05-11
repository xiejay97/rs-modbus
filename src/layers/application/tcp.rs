use crate::error::ModbusError;
use crate::layers::application::ApplicationLayer;
use crate::types::{ApplicationDataUnit, FramedDataUnit};
use std::sync::atomic::{AtomicU16, Ordering};

#[derive(Default)]
pub struct TcpApplicationLayer {
    transaction_id: AtomicU16,
}

impl TcpApplicationLayer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ApplicationLayer for TcpApplicationLayer {
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
        let unit = data[6];
        let fc = data[7];
        let payload = data[8..].to_vec();
        Ok(FramedDataUnit {
            adu: ApplicationDataUnit {
                transaction: Some(transaction),
                unit,
                fc,
                data: payload,
            },
            raw: data.to_vec(),
        })
    }
}
