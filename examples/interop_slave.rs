use async_trait::async_trait;
use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::TcpServerPhysicalLayer;
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
use rs_modbus::types::{AddressRange, ServerId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

struct SimpleModel {
    coils: Arc<Mutex<HashMap<u16, bool>>>,
    holding_registers: Arc<Mutex<HashMap<u16, u16>>>,
    input_registers: Arc<Mutex<HashMap<u16, u16>>>,
    discrete_inputs: Arc<Mutex<HashMap<u16, bool>>>,
}

impl SimpleModel {
    fn new() -> Self {
        Self {
            coils: Arc::new(Mutex::new(HashMap::new())),
            holding_registers: Arc::new(Mutex::new(HashMap::new())),
            input_registers: Arc::new(Mutex::new(HashMap::new())),
            discrete_inputs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ModbusSlaveModel for SimpleModel {
    fn unit(&self) -> u8 { 1 }

    fn address_range(&self) -> AddressRange {
        AddressRange {
            coils: vec![(0, 65535)],
            discrete_inputs: vec![(0, 65535)],
            holding_registers: vec![(0, 65535)],
            input_registers: vec![(0, 65535)],
        }
    }

    async fn read_coils(&self, address: u16, length: u16) -> Result<Vec<bool>, rs_modbus::error::ModbusError> {
        let guard = self.coils.lock().await;
        Ok((0..length).map(|i| *guard.get(&(address + i)).unwrap_or(&false)).collect())
    }

    async fn write_single_coil(&self, address: u16, value: bool) -> Result<(), rs_modbus::error::ModbusError> {
        self.coils.lock().await.insert(address, value);
        Ok(())
    }

    async fn write_multiple_coils(&self, address: u16, values: &[bool]) -> Result<(), rs_modbus::error::ModbusError> {
        for (i, v) in values.iter().enumerate() {
            self.coils.lock().await.insert(address + i as u16, *v);
        }
        Ok(())
    }

    async fn read_discrete_inputs(&self, address: u16, length: u16) -> Result<Vec<bool>, rs_modbus::error::ModbusError> {
        let guard = self.discrete_inputs.lock().await;
        Ok((0..length).map(|i| *guard.get(&(address + i)).unwrap_or(&false)).collect())
    }

    async fn read_holding_registers(&self, address: u16, length: u16) -> Result<Vec<u16>, rs_modbus::error::ModbusError> {
        let guard = self.holding_registers.lock().await;
        Ok((0..length).map(|i| *guard.get(&(address + i)).unwrap_or(&0)).collect())
    }

    async fn write_single_register(&self, address: u16, value: u16) -> Result<(), rs_modbus::error::ModbusError> {
        self.holding_registers.lock().await.insert(address, value);
        Ok(())
    }

    async fn write_multiple_registers(&self, address: u16, values: &[u16]) -> Result<(), rs_modbus::error::ModbusError> {
        for (i, v) in values.iter().enumerate() {
            self.holding_registers.lock().await.insert(address + i as u16, *v);
        }
        Ok(())
    }

    async fn read_input_registers(&self, address: u16, length: u16) -> Result<Vec<u16>, rs_modbus::error::ModbusError> {
        let guard = self.input_registers.lock().await;
        Ok((0..length).map(|i| *guard.get(&(address + i)).unwrap_or(&0)).collect())
    }

    async fn mask_write_register(&self, address: u16, and_mask: u16, or_mask: u16) -> Result<(), rs_modbus::error::ModbusError> {
        let current = *self.holding_registers.lock().await.get(&address).unwrap_or(&0);
        self.holding_registers.lock().await.insert(address, (current & and_mask) | (or_mask & !and_mask));
        Ok(())
    }


    async fn report_server_id(&self) -> Result<ServerId, rs_modbus::error::ModbusError> {
        Ok(ServerId {
            server_id: vec![self.unit()],
            run_indicator_status: true,
            additional_data: vec![1, 2, 3],
        })
    }

    async fn read_device_identification(&self) -> Result<HashMap<u8, String>, rs_modbus::error::ModbusError> {
        let mut map = HashMap::new();
        map.insert(0x00, "VendorName".to_string());
        map.insert(0x01, "ProductCode".to_string());
        map.insert(0x02, "MajorMinorRevision".to_string());
        Ok(map)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let physical = TcpServerPhysicalLayer::new();
    physical.set_addr("127.0.0.1:11502".to_string()).await;

    let application = TcpApplicationLayer::new(physical.clone());
    let slave = ModbusSlave::new(application, physical);

    let model = SimpleModel::new();
    // Pre-populate some data
    model.holding_registers.lock().await.insert(0, 0x1234);
    model.holding_registers.lock().await.insert(1, 0x5678);
    model.coils.lock().await.insert(0, true);
    model.coils.lock().await.insert(1, false);
    model.input_registers.lock().await.insert(0, 0xabcd);
    model.discrete_inputs.lock().await.insert(0, true);

    slave.add(Box::new(model));
    slave.open(None).await?;

    println!("Interop slave listening on 127.0.0.1:11502");

    // Keep running until interrupted
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    slave.destroy().await;
    Ok(())
}
