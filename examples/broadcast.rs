use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::TcpClientPhysicalLayer;
use rs_modbus::master::ModbusMaster;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let physical = TcpClientPhysicalLayer::new();
    physical.set_addr("127.0.0.1:502".to_string()).await;
    let application = TcpApplicationLayer::new(physical.clone());
    let master = ModbusMaster::new(application, physical, 5000);

    master.open().await?;

    // Broadcast write (unit = 0) - no response expected
    let result = master.write_single_register(0, 100, 0x9999, None).await?;
    println!("Broadcast result (should be None): {:?}", result);

    // Broadcast multiple coils
    let result = master
        .write_multiple_coils(0, 200, &[true, false, true], None)
        .await?;
    println!("Broadcast coils result (should be None): {:?}", result);

    master.destroy().await;
    Ok(())
}
