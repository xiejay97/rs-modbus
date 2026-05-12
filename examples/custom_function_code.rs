use async_trait::async_trait;
use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::TcpServerPhysicalLayer;
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
use rs_modbus::types::AddressRange;

/// A custom function code handler.
///
/// This example shows how to intercept and handle custom function codes
/// that are not part of the standard Modbus protocol.
struct CustomModel;

#[async_trait]
impl ModbusSlaveModel for CustomModel {
    fn unit(&self) -> u8 {
        1
    }

    fn address_range(&self) -> AddressRange {
        AddressRange::default()
    }

    /// Intercept custom function code 0x64 (100).
    /// If intercepted, return Some(data) to send a custom response.
    /// If not interested, return Ok(None) to let the default handler process it.
    async fn intercept(
        &self,
        fc: u8,
        data: &[u8],
    ) -> Result<Option<Vec<u8>>, rs_modbus::error::ModbusError> {
        match fc {
            0x64 => {
                // Custom FC 100: echo back the received data + a signature byte
                let mut response = data.to_vec();
                response.push(0xAB); // signature
                Ok(Some(response))
            }
            _ => Ok(None),
        }
    }

    async fn read_holding_registers(
        &self,
        _address: u16,
        length: u16,
    ) -> Result<Vec<u16>, rs_modbus::error::ModbusError> {
        Ok(vec![0; length as usize])
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let physical = TcpServerPhysicalLayer::new();
    let application = TcpApplicationLayer::new(physical.clone());
    let slave = ModbusSlave::new(application, physical);

    slave.add(Box::new(CustomModel)).await;
    slave.open().await?;

    println!("Slave listening on [::]:502 (default)");
    println!("Custom FC 0x64 (100) will echo back data + 0xAB signature");

    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    slave.destroy().await;
    Ok(())
}
