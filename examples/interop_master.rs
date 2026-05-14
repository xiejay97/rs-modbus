use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::TcpClientPhysicalLayer;
use rs_modbus::master::{ModbusMaster, ModbusMasterOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let physical = TcpClientPhysicalLayer::new();
    physical.set_addr("127.0.0.1:11503".to_string()).await;

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
    println!("Connected to njs-modbus slave at 127.0.0.1:11503");

    // Read holding registers
    let registers = master.read_holding_registers(1, 0, 2, None).await?;
    println!("Read holding registers: {:?}", registers);

    // Write single register
    let result = master.write_single_register(1, 5, 0xbeef, None).await?;
    println!("Write single register: {:?}", result);

    // Read back
    let registers2 = master.read_holding_registers(1, 5, 1, None).await?;
    println!("Read back register[5]: {:?}", registers2);

    // Read coils
    let coils = master.read_coils(1, 0, 2, None).await?;
    println!("Read coils: {:?}", coils);

    // Write coil
    let _ = master.write_single_coil(1, 0, true, None).await?;
    println!("Write coil OK");

    // Read back coils
    let coils2 = master.read_coils(1, 0, 2, None).await?;
    println!("Read back coils: {:?}", coils2);

    // Read discrete inputs
    let di = master.read_discrete_inputs(1, 0, 2, None).await?;
    println!("Read discrete inputs: {:?}", di);

    // Read input registers
    let ir = master.read_input_registers(1, 0, 2, None).await?;
    println!("Read input registers: {:?}", ir);

    // Write multiple registers
    let _ = master
        .write_multiple_registers(1, 10, &[0x1111, 0x2222, 0x3333], None)
        .await?;
    println!("Write multiple registers OK");

    // Read back
    let mr = master.read_holding_registers(1, 10, 3, None).await?;
    println!("Read back multiple registers: {:?}", mr);

    // Mask write register
    let _ = master
        .mask_write_register(1, 20, 0x00FF, 0xFF00, None)
        .await?;
    println!("Mask write register OK");

    // Read back
    let mwr = master.read_holding_registers(1, 20, 1, None).await?;
    println!("Read back after mask write: {:?}", mwr);

    // Read and write multiple registers
    let raw = master
        .read_and_write_multiple_registers(1, 10, 2, 12, &[0xaaaa, 0xbbbb], None)
        .await?;
    println!("Read-write multiple registers read: {:?}", raw);

    // Read back write addresses
    let rwr = master.read_holding_registers(1, 12, 2, None).await?;
    println!("Read back after read-write: {:?}", rwr);

    // Report server ID
    let sid = master.report_server_id(1, 1, None).await?;
    println!("Report server ID: {:?}", sid);

    // Read device identification
    let dev = master
        .read_device_identification(1, 0x01, 0x00, None)
        .await?;
    println!("Read device identification: {:?}", dev);

    println!("\n=== ALL INTEROP TESTS PASSED ===");

    master.destroy().await;
    Ok(())
}
