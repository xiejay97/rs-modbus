use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::TcpClientPhysicalLayer;
use rs_modbus::master::ModbusMaster;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let physical = TcpClientPhysicalLayer::new();
    physical.set_addr("127.0.0.1:502".to_string()).await;
    let application = TcpApplicationLayer::new();
    let master = ModbusMaster::new(Arc::new(application), physical, 5000);

    master.open().await?;

    // Read 10 holding registers from unit 1, starting at address 0
    let registers = master.read_holding_registers(1, 0, 10, None).await?;
    println!("Read holding registers: {:?}", registers);

    // Write a single register
    let result = master.write_single_register(1, 5, 0x1234, None).await?;
    println!("Write single register: {:?}", result);

    // Read coils
    let coils = master.read_coils(1, 0, 8, None).await?;
    println!("Read coils: {:?}", coils);

    master.destroy().await;
    Ok(())
}
