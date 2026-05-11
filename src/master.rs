use crate::error::ModbusError;
use crate::layers::application::ApplicationLayer;
use crate::layers::physical::PhysicalLayer;
use crate::types::{
    ApplicationDataUnit, DeviceIdentification, DeviceObject, FramedDataUnit, ServerId,
};
use crate::utils::{parse_coils, parse_registers};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub enum PreCheckResult {
    Ok,
    NeedLength(usize),
    Fail,
}

pub type PreCheck = Arc<dyn Fn(&FramedDataUnit) -> Option<PreCheckResult> + Send + Sync>;

pub struct ModbusMaster<A: ApplicationLayer, P: PhysicalLayer> {
    application: Arc<A>,
    physical: Arc<P>,
    pub timeout_ms: u64,
}

impl<A: ApplicationLayer, P: PhysicalLayer> ModbusMaster<A, P> {
    pub fn new(application: Arc<A>, physical: Arc<P>, timeout_ms: u64) -> Self {
        Self {
            application,
            physical,
            timeout_ms,
        }
    }

    pub async fn open(&self) -> Result<(), ModbusError> {
        self.physical.open().await
    }

    pub async fn close(&self) -> Result<(), ModbusError> {
        self.physical.close().await
    }

    pub async fn destroy(&self) {
        let _ = self.physical.destroy().await;
    }

    fn check_unit_fc(unit: u8, fc: u8) -> PreCheck {
        Arc::new(move |f| {
            if f.adu.unit == unit && f.adu.fc == fc {
                Some(PreCheckResult::Ok)
            } else {
                Some(PreCheckResult::Fail)
            }
        })
    }

    fn check_length(expected: usize) -> PreCheck {
        Arc::new(move |_| Some(PreCheckResult::NeedLength(expected)))
    }

    fn check_byte_count(expected: usize) -> PreCheck {
        Arc::new(move |f| {
            if f.adu.data[0] as usize == expected {
                Some(PreCheckResult::Ok)
            } else {
                Some(PreCheckResult::Fail)
            }
        })
    }

    fn check_echo(expected: Vec<u8>) -> PreCheck {
        Arc::new(move |f| {
            if f.adu.data == expected {
                Some(PreCheckResult::Ok)
            } else {
                Some(PreCheckResult::Fail)
            }
        })
    }

    async fn wait_response(
        &self,
        request: &ApplicationDataUnit,
        checks: Vec<PreCheck>,
        timeout_ms: u64,
    ) -> Result<Option<FramedDataUnit>, ModbusError> {
        let data = self.application.encode(request);
        self.physical.write(&data).await?;

        if request.unit == 0 {
            return Ok(None);
        }

        let mut rx = self.physical.subscribe_data();
        let timeout = Duration::from_millis(timeout_ms);

        let result = tokio::time::timeout(timeout, async {
            loop {
                let (received, _) = rx.recv().await.map_err(|_| ModbusError::Timeout)?;
                let frame = self.application.decode(&received)?;

                let mut matched = true;
                for check in &checks {
                    match check(&frame) {
                        Some(PreCheckResult::Ok) => {}
                        Some(PreCheckResult::NeedLength(expected)) => {
                            if frame.adu.data.len() < expected {
                                matched = false;
                                break;
                            }
                            if frame.adu.data.len() != expected {
                                return Err(ModbusError::InvalidResponse);
                            }
                        }
                        Some(PreCheckResult::Fail) => {
                            return Err(ModbusError::InvalidResponse);
                        }
                        None => {
                            return Err(ModbusError::InsufficientData);
                        }
                    }
                }
                if matched {
                    return Ok(frame);
                }
            }
        })
        .await;

        match result {
            Ok(r) => Ok(Some(r?)),
            Err(_) => Err(ModbusError::Timeout),
        }
    }

    // FC1 - Read Coils
    pub async fn read_coils(
        &self,
        unit: u8,
        address: u16,
        length: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<bool>>, ModbusError> {
        let fc = 0x01;
        let byte_count = ((length + 7) / 8) as usize;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&length.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + byte_count),
                    Self::check_byte_count(byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_coils(&f.adu.data, length))),
            None => Ok(None),
        }
    }

    // FC2 - Read Discrete Inputs
    pub async fn read_discrete_inputs(
        &self,
        unit: u8,
        address: u16,
        length: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<bool>>, ModbusError> {
        let fc = 0x02;
        let byte_count = ((length + 7) / 8) as usize;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&length.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + byte_count),
                    Self::check_byte_count(byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_coils(&f.adu.data, length))),
            None => Ok(None),
        }
    }

    // FC3 - Read Holding Registers
    pub async fn read_holding_registers(
        &self,
        unit: u8,
        address: u16,
        length: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<u16>>, ModbusError> {
        let fc = 0x03;
        let byte_count = (length * 2) as usize;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&length.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + byte_count),
                    Self::check_byte_count(byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_registers(&f.adu.data, length))),
            None => Ok(None),
        }
    }

    // FC4 - Read Input Registers
    pub async fn read_input_registers(
        &self,
        unit: u8,
        address: u16,
        length: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<u16>>, ModbusError> {
        let fc = 0x04;
        let byte_count = (length * 2) as usize;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&length.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + byte_count),
                    Self::check_byte_count(byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_registers(&f.adu.data, length))),
            None => Ok(None),
        }
    }

    // FC5 - Write Single Coil
    pub async fn write_single_coil(
        &self,
        unit: u8,
        address: u16,
        value: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Option<bool>, ModbusError> {
        let fc = 0x05;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        let value_u16: u16 = if value { 0xff00 } else { 0x0000 };
        buf[2..4].copy_from_slice(&value_u16.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf.clone());

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(4),
                    Self::check_echo(buf),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    // FC6 - Write Single Register
    pub async fn write_single_register(
        &self,
        unit: u8,
        address: u16,
        value: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<u16>, ModbusError> {
        let fc = 0x06;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&value.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf.clone());

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(4),
                    Self::check_echo(buf),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    // FC15 - Write Multiple Coils
    pub async fn write_multiple_coils(
        &self,
        unit: u8,
        address: u16,
        values: &[bool],
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<bool>>, ModbusError> {
        let fc = 0x0f;
        let byte_count = ((values.len() + 7) / 8) as u8;

        let mut buf = vec![0u8; 5 + byte_count as usize];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&(values.len() as u16).to_be_bytes());
        buf[4] = byte_count;
        for (byte_idx, chunk) in values.chunks(8).enumerate() {
            let mut byte = 0u8;
            for (bit_idx, &v) in chunk.iter().enumerate() {
                if v {
                    byte |= 1 << bit_idx;
                }
            }
            buf[5 + byte_idx] = byte;
        }

        let tx_buf = buf.clone();
        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(4),
                    Self::check_echo(tx_buf[..4].to_vec()),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some(values.to_vec())),
            None => Ok(None),
        }
    }

    // FC16 - Write Multiple Registers
    pub async fn write_multiple_registers(
        &self,
        unit: u8,
        address: u16,
        values: &[u16],
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<u16>>, ModbusError> {
        let fc = 0x10;
        let byte_count = (values.len() * 2) as u8;

        let mut buf = vec![0u8; 5 + byte_count as usize];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&(values.len() as u16).to_be_bytes());
        buf[4] = byte_count;
        for (i, &v) in values.iter().enumerate() {
            buf[5 + i * 2..7 + i * 2].copy_from_slice(&v.to_be_bytes());
        }

        let tx_buf = buf.clone();
        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(4),
                    Self::check_echo(tx_buf[..4].to_vec()),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some(values.to_vec())),
            None => Ok(None),
        }
    }

    // FC17 - Report Server ID
    pub async fn report_server_id(
        &self,
        unit: u8,
        timeout_ms: Option<u64>,
    ) -> Result<Option<ServerId>, ModbusError> {
        let fc = 0x11;
        let request = ApplicationDataUnit::new(unit, fc, vec![]);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Arc::new(|f| {
                        if !f.adu.data.is_empty() {
                            let len = 1 + f.adu.data[0] as usize;
                            Some(PreCheckResult::NeedLength(len))
                        } else {
                            None
                        }
                    }),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(ServerId {
                server_id: f.adu.data[1],
                run_indicator_status: f.adu.data[2] == 0xff,
                additional_data: f.adu.data[3..].to_vec(),
            })),
            None => Ok(None),
        }
    }

    // FC22 - Mask Write Register
    pub async fn mask_write_register(
        &self,
        unit: u8,
        address: u16,
        and_mask: u16,
        or_mask: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<(u16, u16)>, ModbusError> {
        let fc = 0x16;

        let mut buf = vec![0u8; 6];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&and_mask.to_be_bytes());
        buf[4..6].copy_from_slice(&or_mask.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf.clone());

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(6),
                    Self::check_echo(buf),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some((and_mask, or_mask))),
            None => Ok(None),
        }
    }

    // FC23 - Read/Write Multiple Registers
    pub async fn read_and_write_multiple_registers(
        &self,
        unit: u8,
        read_address: u16,
        read_length: u16,
        write_address: u16,
        write_values: &[u16],
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<u16>>, ModbusError> {
        let fc = 0x17;
        let byte_count = (write_values.len() * 2) as u8;

        let mut buf = vec![0u8; 9 + byte_count as usize];
        buf[0..2].copy_from_slice(&read_address.to_be_bytes());
        buf[2..4].copy_from_slice(&read_length.to_be_bytes());
        buf[4..6].copy_from_slice(&write_address.to_be_bytes());
        buf[6..8].copy_from_slice(&(write_values.len() as u16).to_be_bytes());
        buf[8] = byte_count;
        for (i, &v) in write_values.iter().enumerate() {
            buf[9 + i * 2..11 + i * 2].copy_from_slice(&v.to_be_bytes());
        }

        let request = ApplicationDataUnit::new(unit, fc, buf);
        let read_byte_count = (read_length * 2) as usize;

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + read_byte_count),
                    Self::check_byte_count(read_byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_registers(&f.adu.data, read_length))),
            None => Ok(None),
        }
    }

    // FC43/14 - Read Device Identification
    pub async fn read_device_identification(
        &self,
        unit: u8,
        read_device_id_code: u8,
        object_id: u8,
        timeout_ms: Option<u64>,
    ) -> Result<Option<DeviceIdentification>, ModbusError> {
        let fc = 0x2b;
        let request =
            ApplicationDataUnit::new(unit, fc, vec![0x0e, read_device_id_code, object_id]);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Arc::new(move |f| {
                        if f.adu.data.len() >= 6
                            && f.adu.data[0] == 0x0e
                            && f.adu.data[1] == read_device_id_code
                        {
                            let num_objects = f.adu.data[5] as usize;
                            let mut total = 6usize;
                            let mut idx = 6;
                            for _ in 0..num_objects {
                                if idx + 2 > f.adu.data.len() {
                                    return None;
                                }
                                let obj_len = f.adu.data[idx + 1] as usize;
                                total += 2 + obj_len;
                                idx += 2 + obj_len;
                            }
                            Some(PreCheckResult::NeedLength(total))
                        } else {
                            Some(PreCheckResult::Fail)
                        }
                    }),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => {
                let mut objects = Vec::new();
                let num_objects = f.adu.data[5] as usize;
                let mut idx = 6;
                for _ in 0..num_objects {
                    let obj_id = f.adu.data[idx];
                    let obj_len = f.adu.data[idx + 1] as usize;
                    let obj_value =
                        String::from_utf8_lossy(&f.adu.data[idx + 2..idx + 2 + obj_len])
                            .to_string();
                    objects.push(DeviceObject {
                        id: obj_id,
                        value: obj_value,
                    });
                    idx += 2 + obj_len;
                }
                Ok(Some(DeviceIdentification {
                    read_device_id_code: f.adu.data[1],
                    conformity_level: f.adu.data[2],
                    more_follows: f.adu.data[3] == 0xff,
                    next_object_id: f.adu.data[4],
                    objects,
                }))
            }
            None => Ok(None),
        }
    }
}
