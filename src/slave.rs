use crate::error::{get_code_by_error, get_error_by_code, ErrorCode, ModbusError};
use crate::layers::application::{ApplicationLayer, ApplicationRole};
use crate::layers::physical::{PhysicalLayer, ResponseFn};
use crate::types::{AddressRange, ApplicationDataUnit, FramedDataUnit, ServerId};
use crate::utils::{check_range, pack_coils, pack_registers};
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

type SlaveResponseFn = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), ModbusError>> + Send>> + Send + Sync,
>;

#[async_trait]
pub trait ModbusSlaveModel: Send + Sync {
    fn unit(&self) -> u8;
    fn address_range(&self) -> AddressRange;

    async fn intercept(&self, _fc: u8, _data: &[u8]) -> Result<Option<Vec<u8>>, ModbusError> {
        Ok(None)
    }

    async fn read_coils(&self, _address: u16, _length: u16) -> Result<Vec<bool>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    async fn write_single_coil(&self, _address: u16, _value: bool) -> Result<(), ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    async fn write_multiple_coils(
        &self,
        _address: u16,
        _values: &[bool],
    ) -> Result<(), ModbusError> {
        Err(ModbusError::IllegalFunction)
    }

    async fn read_discrete_inputs(
        &self,
        _address: u16,
        _length: u16,
    ) -> Result<Vec<bool>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }

    async fn read_holding_registers(
        &self,
        _address: u16,
        _length: u16,
    ) -> Result<Vec<u16>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    async fn write_single_register(&self, _address: u16, _value: u16) -> Result<(), ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    async fn write_multiple_registers(
        &self,
        _address: u16,
        _values: &[u16],
    ) -> Result<(), ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    async fn mask_write_register(
        &self,
        _address: u16,
        _and_mask: u16,
        _or_mask: u16,
    ) -> Result<(), ModbusError> {
        Err(ModbusError::IllegalFunction)
    }

    async fn read_input_registers(
        &self,
        _address: u16,
        _length: u16,
    ) -> Result<Vec<u16>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }

    async fn report_server_id(&self) -> Result<ServerId, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    async fn read_device_identification(&self) -> Result<HashMap<u8, String>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
}

pub struct ModbusSlave<A: ApplicationLayer, P: PhysicalLayer> {
    application: Arc<A>,
    physical: Arc<P>,
    pub models: Arc<Mutex<HashMap<u8, Box<dyn ModbusSlaveModel>>>>,
    tx: Mutex<Option<mpsc::Sender<(FramedDataUnit, ResponseFn)>>>,
}

impl<A: ApplicationLayer + 'static, P: PhysicalLayer + 'static> ModbusSlave<A, P> {
    pub fn new(application: Arc<A>, physical: Arc<P>) -> Self {
        let _ = application.set_role(ApplicationRole::Slave);
        Self {
            application,
            physical,
            models: Arc::new(Mutex::new(HashMap::new())),
            tx: Mutex::new(None),
        }
    }

    pub async fn add(&self, model: Box<dyn ModbusSlaveModel>) {
        let unit = model.unit();
        self.models.lock().await.insert(unit, model);
    }

    pub async fn remove(&self, unit: u8) {
        self.models.lock().await.remove(&unit);
    }

    pub async fn open(&self) -> Result<(), ModbusError> {
        self.physical.open().await?;

        let (tx, mut rx) = mpsc::channel::<(FramedDataUnit, ResponseFn)>(100);
        *self.tx.lock().await = Some(tx);

        let application = Arc::clone(&self.application);
        let models = Arc::clone(&self.models);

        tokio::spawn(async move {
            while let Some((frame, response_fn)) = rx.recv().await {
                Self::process_frame(&application, &models, frame, response_fn).await;
            }
        });

        let application = Arc::clone(&self.application);
        let tx = self.tx.lock().await.clone().unwrap();
        let mut data_rx = self.physical.subscribe_data();

        tokio::spawn(async move {
            while let Ok(event) = data_rx.recv().await {
                if let Ok(frame) = application.decode(&event.data) {
                    let _ = tx.send((frame, event.response)).await;
                }
            }
        });

        Ok(())
    }

    pub async fn close(&self) -> Result<(), ModbusError> {
        *self.tx.lock().await = None;
        self.physical.close().await
    }

    pub async fn destroy(&self) {
        *self.tx.lock().await = None;
        let _ = self.physical.destroy().await;
    }

    async fn process_frame(
        application: &Arc<A>,
        models: &Arc<Mutex<HashMap<u8, Box<dyn ModbusSlaveModel>>>>,
        frame: FramedDataUnit,
        response_fn: ResponseFn,
    ) {
        let unit = frame.adu.unit;
        let models_guard = models.lock().await;

        let model_refs: Vec<u8> = if unit == 0 {
            models_guard.keys().copied().collect()
        } else {
            match models_guard.get(&unit) {
                Some(_) => vec![unit],
                None => return,
            }
        };

        for model_unit in model_refs {
            let model = match models_guard.get(&model_unit) {
                Some(m) => m.as_ref(),
                None => continue,
            };

            let response: SlaveResponseFn = if unit == 0 {
                Arc::new(|_| {
                    Box::pin(async { Ok(()) })
                        as Pin<Box<dyn Future<Output = Result<(), ModbusError>> + Send>>
                })
            } else {
                let response_fn = Arc::clone(&response_fn);
                Arc::new(move |data| {
                    let response_fn = Arc::clone(&response_fn);
                    Box::pin(async move { response_fn(data).await })
                        as Pin<Box<dyn Future<Output = Result<(), ModbusError>> + Send>>
                })
            };

            match model.intercept(frame.adu.fc, &frame.adu.data).await {
                Ok(Some(data)) => {
                    let response_adu = ApplicationDataUnit {
                        transaction: frame.adu.transaction,
                        unit: frame.adu.unit,
                        fc: frame.adu.fc,
                        data,
                    };
                    let encoded = application.encode(&response_adu);
                    let _ = response(encoded).await;
                    continue;
                }
                Ok(None) => {}
                Err(err) => {
                    let _ = Self::response_error(application, &frame.adu, response, &err).await;
                    continue;
                }
            }

            let result = match frame.adu.fc {
                0x01 => {
                    Self::handle_fc1(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x02 => {
                    Self::handle_fc2(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x03 => {
                    Self::handle_fc3(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x04 => {
                    Self::handle_fc4(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x05 => {
                    Self::handle_fc5(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x06 => {
                    Self::handle_fc6(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x0f => {
                    Self::handle_fc15(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x10 => {
                    Self::handle_fc16(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x11 => {
                    Self::handle_fc17(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x16 => {
                    Self::handle_fc22(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x17 => {
                    Self::handle_fc23(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x2b => {
                    Self::handle_fc43_14(application, model, &frame.adu, Arc::clone(&response))
                        .await
                }
                _ => {
                    Self::response_error(
                        application,
                        &frame.adu,
                        Arc::clone(&response),
                        &get_error_by_code(ErrorCode::IllegalFunction),
                    )
                    .await
                }
            };

            if let Err(e) = result {
                let _ = Self::response_error(application, &frame.adu, response, &e).await;
            }
        }
    }

    async fn response_error(
        application: &Arc<A>,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
        error: &ModbusError,
    ) -> Result<(), ModbusError> {
        let error_code = get_code_by_error(error) as u8;
        let response_adu = ApplicationDataUnit {
            transaction: adu.transaction,
            unit: adu.unit,
            fc: adu.fc | 0x80,
            data: vec![error_code],
        };
        let encoded = application.encode(&response_adu);
        response(encoded).await
    }

    // FC1 - Read Coils
    async fn handle_fc1(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !(1..=0x07d0).contains(&length) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(&[address, address + length], &model.address_range().coils) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.read_coils(address, length).await {
            Ok(coils) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_coils(&coils, length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC2 - Read Discrete Inputs
    async fn handle_fc2(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !(1..=0x07d0).contains(&length) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[address, address + length],
            &model.address_range().discrete_inputs,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.read_discrete_inputs(address, length).await {
            Ok(inputs) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_coils(&inputs, length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC3 - Read Holding Registers
    async fn handle_fc3(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !(1..=0x007d).contains(&length) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[address, address + length],
            &model.address_range().holding_registers,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.read_holding_registers(address, length).await {
            Ok(registers) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_registers(&registers, length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC4 - Read Input Registers
    async fn handle_fc4(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !(1..=0x007d).contains(&length) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[address, address + length],
            &model.address_range().input_registers,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.read_input_registers(address, length).await {
            Ok(registers) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_registers(&registers, length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC5 - Write Single Coil
    async fn handle_fc5(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let value = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if value != 0x0000 && value != 0xff00 {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(&[address], &model.address_range().coils) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.write_single_coil(address, value == 0xff00).await {
            Ok(()) => response(application.encode(adu)).await,
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC6 - Write Single Register
    async fn handle_fc6(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let value = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !check_range(&[address], &model.address_range().holding_registers) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.write_single_register(address, value).await {
            Ok(()) => response(application.encode(adu)).await,
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC15 - Write Multiple Coils
    async fn handle_fc15(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() < 6 || adu.data.len() != 5 + adu.data[4] as usize {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);
        let byte_count = adu.data[4];

        if !(1..=0x07b0).contains(&length) || byte_count as u16 != (length + 7) / 8 {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(&[address, address + length], &model.address_range().coils) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        let values: Vec<bool> = (0..length)
            .map(|i| (adu.data[5 + i as usize / 8] >> (i % 8)) & 1 == 1)
            .collect();

        let result = model.write_multiple_coils(address, &values).await;

        match result {
            Ok(()) => {
                let mut data = vec![0u8; 4];
                data[0..2].copy_from_slice(&address.to_be_bytes());
                data[2..4].copy_from_slice(&length.to_be_bytes());
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data,
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC16 - Write Multiple Registers
    async fn handle_fc16(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() < 6 || adu.data.len() != 5 + adu.data[4] as usize {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);
        let byte_count = adu.data[4];

        if !(1..=0x007b).contains(&length) || byte_count as u16 != length * 2 {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[address, address + length],
            &model.address_range().holding_registers,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        let values: Vec<u16> = (0..length)
            .map(|i| {
                u16::from_be_bytes([adu.data[5 + i as usize * 2], adu.data[6 + i as usize * 2]])
            })
            .collect();

        let result = model.write_multiple_registers(address, &values).await;

        match result {
            Ok(()) => {
                let mut data = vec![0u8; 4];
                data[0..2].copy_from_slice(&address.to_be_bytes());
                data[2..4].copy_from_slice(&length.to_be_bytes());
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data,
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC17 - Report Server ID
    async fn handle_fc17(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if !adu.data.is_empty() {
            return Ok(());
        }

        match model.report_server_id().await {
            Ok(server_id) => {
                let mut data = vec![2 + server_id.additional_data.len() as u8];
                data.push(server_id.server_id);
                data.push(if server_id.run_indicator_status {
                    0xff
                } else {
                    0x00
                });
                data.extend_from_slice(&server_id.additional_data);
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data,
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC22 - Mask Write Register
    async fn handle_fc22(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 6 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let and_mask = u16::from_be_bytes([adu.data[2], adu.data[3]]);
        let or_mask = u16::from_be_bytes([adu.data[4], adu.data[5]]);

        if !check_range(&[address], &model.address_range().holding_registers) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        let result = model.mask_write_register(address, and_mask, or_mask).await;

        match result {
            Ok(()) => response(application.encode(adu)).await,
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC23 - Read/Write Multiple Registers
    async fn handle_fc23(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() < 10 || adu.data.len() != 9 + adu.data[8] as usize {
            return Ok(());
        }
        let read_address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let read_length = u16::from_be_bytes([adu.data[2], adu.data[3]]);
        let write_address = u16::from_be_bytes([adu.data[4], adu.data[5]]);
        let write_length = u16::from_be_bytes([adu.data[6], adu.data[7]]);
        let byte_count = adu.data[8];

        if !(1..=0x007d).contains(&read_length)
            || !(1..=0x0079).contains(&write_length)
            || byte_count as u16 != write_length * 2
        {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[
                read_address,
                read_address + read_length,
                write_address,
                write_address + write_length,
            ],
            &model.address_range().holding_registers,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        let write_values: Vec<u16> = (0..write_length)
            .map(|i| {
                u16::from_be_bytes([adu.data[9 + i as usize * 2], adu.data[10 + i as usize * 2]])
            })
            .collect();

        if let Err(e) = model
            .write_multiple_registers(write_address, &write_values)
            .await
        {
            return Self::response_error(application, adu, response, &e).await;
        }

        match model
            .read_holding_registers(read_address, read_length)
            .await
        {
            Ok(registers) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_registers(&registers, read_length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC43/14 - Read Device Identification
    async fn handle_fc43_14(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 3 || adu.data[0] != 0x0e {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalFunction),
            )
            .await;
        }

        let read_device_id_code = adu.data[1];
        let mut object_id = adu.data[2];

        match read_device_id_code {
            0x01 => {
                if object_id > 0x02 || (object_id > 0x06 && object_id < 0x80) {
                    object_id = 0x00;
                }
            }
            0x02 => {
                if object_id > 0x06 {
                    object_id = 0x00;
                }
            }
            0x03 => {
                if object_id > 0x06 && object_id < 0x80 {
                    object_id = 0x00;
                }
            }
            0x04 => {
                if object_id > 0x06 && object_id < 0x80 {
                    return Self::response_error(
                        application,
                        adu,
                        response,
                        &get_error_by_code(ErrorCode::IllegalDataAddress),
                    )
                    .await;
                }
            }
            _ => {
                return Self::response_error(
                    application,
                    adu,
                    response,
                    &get_error_by_code(ErrorCode::IllegalDataValue),
                )
                .await;
            }
        }

        match model.read_device_identification().await {
            Ok(identification) => {
                let mut objects: Vec<(u8, String)> = vec![
                    (0x00, "null".to_string()),
                    (0x01, "null".to_string()),
                    (0x02, "null".to_string()),
                ];
                for (k, v) in identification {
                    if let Some(pos) = objects.iter().position(|(id, _)| *id == k) {
                        objects[pos] = (k, v);
                    } else {
                        objects.push((k, v));
                    }
                }
                objects.sort_by_key(|(id, _)| *id);

                let has_object_id = objects.iter().any(|(id, _)| *id == object_id);
                if !has_object_id {
                    if read_device_id_code == 0x04 {
                        return Self::response_error(
                            application,
                            adu,
                            response,
                            &get_error_by_code(ErrorCode::IllegalDataAddress),
                        )
                        .await;
                    }
                    object_id = 0x00;
                }

                let max_id = objects.last().map(|(id, _)| *id).unwrap_or(0);
                let conformity_level = if max_id > 0x80 {
                    0x83
                } else if max_id > 0x02 {
                    0x82
                } else {
                    0x81
                };

                let mut ids = Vec::new();
                let mut total_length = 10usize;
                let mut last_id = 0u8;

                for &(id, ref value) in &objects {
                    if id < object_id {
                        continue;
                    }
                    if value.len() > 245 {
                        return Self::response_error(
                            application,
                            adu,
                            response,
                            &get_error_by_code(ErrorCode::ServerDeviceFailure),
                        )
                        .await;
                    }
                    if total_length + 2 + value.len() > 253 {
                        if last_id == 0 {
                            last_id = id;
                        }
                        continue;
                    }
                    total_length += 2 + value.len();
                    ids.push(id);
                    if read_device_id_code == 0x04 {
                        break;
                    }
                }

                let mut data = vec![
                    0x0e,
                    read_device_id_code,
                    conformity_level,
                    if last_id == 0 { 0x00 } else { 0xff },
                    last_id,
                    ids.len() as u8,
                ];
                for id in ids {
                    if let Some((_, value)) = objects.iter().find(|(oid, _)| *oid == id) {
                        data.push(id);
                        data.push(value.len() as u8);
                        data.extend_from_slice(value.as_bytes());
                    }
                }

                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data,
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }
}
