use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::TcpClientPhysicalLayer;
use rs_modbus::master::{ModbusMaster, ModbusMasterOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let physical = TcpClientPhysicalLayer::new();
    let application = TcpApplicationLayer::new(physical.clone());
    let master = ModbusMaster::new(
        application,
        physical,
        ModbusMasterOptions {
            timeout_ms: 5000,
            concurrent: false,
        },
    );

    master.open(None).await?;

    // Read 10 holding registers from unit 1, starting at address 0
    let registers = master.read_holding_registers(1, 0, 10, None).await?;
    println!("Read holding registers: {:?}", registers.map(|r| r.data));

    // Write a single register
    let result = master.write_single_register(1, 5, 0x1234, None).await?;
    println!("Write single register: {:?}", result.map(|r| r.data));

    // Read coils
    let coils = master.read_coils(1, 0, 8, None).await?;
    println!("Read coils: {:?}", coils.map(|r| r.data));

    master.destroy().await;
    Ok(())
}
