// Run with: cargo run --example rtu_slave --features serial
#[cfg(feature = "serial")]
use async_trait::async_trait;
#[cfg(feature = "serial")]
use rs_modbus::layers::application::RtuApplicationLayer;
#[cfg(feature = "serial")]
use rs_modbus::layers::physical::SerialPhysicalLayer;
#[cfg(feature = "serial")]
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
#[cfg(feature = "serial")]
use rs_modbus::types::{AddressRange, ServerId};
#[cfg(feature = "serial")]
use std::collections::HashMap;
#[cfg(feature = "serial")]
use std::sync::Arc;
#[cfg(feature = "serial")]
use tokio::sync::Mutex;

#[cfg(feature = "serial")]
struct SimpleModel {
    coils: Arc<Mutex<HashMap<u16, bool>>>,
    holding_registers: Arc<Mutex<HashMap<u16, u16>>>,
}

#[cfg(feature = "serial")]
impl SimpleModel {
    fn new() -> Self {
        Self {
            coils: Arc::new(Mutex::new(HashMap::new())),
            holding_registers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[cfg(feature = "serial")]
#[async_trait]
impl ModbusSlaveModel for SimpleModel {
    fn unit(&self) -> u8 {
        1
    }

    fn address_range(&self) -> AddressRange {
        AddressRange {
            coils: vec![(0, 65535)],
            holding_registers: vec![(0, 65535)],
            ..Default::default()
        }
    }

    async fn read_coils(
        &self,
        address: u16,
        length: u16,
    ) -> Result<Vec<bool>, rs_modbus::error::ModbusError> {
        let guard = self.coils.lock().await;
        Ok((0..length)
            .map(|i| *guard.get(&(address + i)).unwrap_or(&false))
            .collect())
    }

    async fn write_single_coil(
        &self,
        address: u16,
        value: bool,
    ) -> Result<(), rs_modbus::error::ModbusError> {
        self.coils.lock().await.insert(address, value);
        Ok(())
    }

    async fn read_holding_registers(
        &self,
        address: u16,
        length: u16,
    ) -> Result<Vec<u16>, rs_modbus::error::ModbusError> {
        let guard = self.holding_registers.lock().await;
        Ok((0..length)
            .map(|i| *guard.get(&(address + i)).unwrap_or(&0))
            .collect())
    }

    async fn write_single_register(
        &self,
        address: u16,
        value: u16,
    ) -> Result<(), rs_modbus::error::ModbusError> {
        self.holding_registers.lock().await.insert(address, value);
        Ok(())
    }

    async fn report_server_id(&self) -> Result<ServerId, rs_modbus::error::ModbusError> {
        Ok(ServerId {
            server_id: self.unit(),
            run_indicator_status: true,
            additional_data: vec![1, 2, 3],
        })
    }

    async fn read_device_identification(
        &self,
    ) -> Result<HashMap<u8, String>, rs_modbus::error::ModbusError> {
        let mut map = HashMap::new();
        map.insert(0x00, "VendorName".to_string());
        map.insert(0x01, "ProductCode".to_string());
        map.insert(0x02, "MajorMinorRevision".to_string());
        Ok(map)
    }
}

#[cfg(feature = "serial")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open serial port at 9600 baud (adjust path/baud for your hardware)
    let physical = SerialPhysicalLayer::new("COM1".to_string(), 9600);
    let application = RtuApplicationLayer::new(physical.clone(), Some(9600), None);
    let slave = ModbusSlave::new(application, physical);

    let model = SimpleModel::new();
    model.holding_registers.lock().await.insert(0, 0x1234);
    model.holding_registers.lock().await.insert(1, 0x5678);
    model.coils.lock().await.insert(0, true);

    slave.add(Box::new(model)).await;
    slave.open().await?;

    println!("RTU Slave listening on COM1 @ 9600 baud");

    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    slave.destroy().await;
    Ok(())
}

#[cfg(not(feature = "serial"))]
fn main() {
    eprintln!("This example requires the 'serial' feature.");
    eprintln!("Run with: cargo run --example rtu_slave --features serial");
}
