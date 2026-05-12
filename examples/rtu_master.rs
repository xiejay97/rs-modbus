// Run with: cargo run --example rtu_master --features serial
#[cfg(feature = "serial")]
use rs_modbus::layers::application::RtuApplicationLayer;
#[cfg(feature = "serial")]
use rs_modbus::layers::physical::SerialPhysicalLayer;
#[cfg(feature = "serial")]
use rs_modbus::master::ModbusMaster;

#[cfg(feature = "serial")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open serial port at 9600 baud (adjust path/baud for your hardware)
    let physical = SerialPhysicalLayer::new("COM1".to_string(), 9600);
    let application = RtuApplicationLayer::new(physical.clone(), Some(9600), None);
    let master = ModbusMaster::new(application, physical, 1000);

    master.open().await?;

    // Read 10 holding registers from unit 1, starting at address 0
    let registers = master.read_holding_registers(1, 0, 10, None).await?;
    println!("Read holding registers: {:?}", registers);

    // Write a single register
    let result = master.write_single_register(1, 5, 0x1234, None).await?;
    println!("Write single register: {:?}", result);

    master.destroy().await;
    Ok(())
}

#[cfg(not(feature = "serial"))]
fn main() {
    eprintln!("This example requires the 'serial' feature.");
    eprintln!("Run with: cargo run --example rtu_master --features serial");
}
