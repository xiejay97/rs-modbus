// Run with: cargo run --example rtu_master --features serial
use rs_modbus::layers::application::{RtuApplicationLayer, RtuApplicationLayerOptions};
use rs_modbus::layers::physical::{SerialPhysicalLayer, SerialPhysicalLayerOptions};
use rs_modbus::master::{ModbusMaster, ModbusMasterOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open serial port at 9600 baud (adjust path/baud for your hardware)
    let physical = SerialPhysicalLayer::new(SerialPhysicalLayerOptions {
        path: "COM1".to_string(),
        baud_rate: 9600,
        ..Default::default()
    });
    let application = RtuApplicationLayer::new(
        physical.clone(),
        RtuApplicationLayerOptions {
            baud_rate: Some(9600),
            ..Default::default()
        },
    );
    let master = ModbusMaster::new(
        application,
        physical,
        ModbusMasterOptions {
            timeout_ms: 1000,
            concurrent: false,
        },
    );

    master.open(()).await?;

    // Read 10 holding registers from unit 1, starting at address 0
    let registers = master.read_holding_registers(1, 0, 10, None).await?;
    println!("Read holding registers: {:?}", registers.map(|r| r.data));

    // Write a single register
    let result = master.write_single_register(1, 5, 0x1234, None).await?;
    println!("Write single register: {:?}", result.map(|r| r.data));

    master.destroy().await;
    Ok(())
}
