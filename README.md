# rs-modbus

A pure Rust implementation of MODBUS protocol.

## Introduction

`rs-modbus` is designed as a layered architecture, including the physical layer and the application layer:

- **Physical layer** implements Serial Port, TCP/IP and UDP/IP.
- **Application layer** implements RTU, ASCII and TCP.

`rs-modbus` provide both client and server.

## Features

- Full Modbus standard protocol implementation
- Support for custom function codes
- Support broadcasting
- Very lightweight project
- Full async/await support via tokio

### Supported function codes

| Code  | Name                           |
| ----- | ------------------------------ |
| 01    | Read Coils                     |
| 02    | Read Discrete Inputs           |
| 03    | Read Holding Registers         |
| 04    | Read Input Registers           |
| 05    | Write Single Coil              |
| 06    | Write Single Register          |
| 15    | Write Multiple Coils           |
| 16    | Write Multiple Registers       |
| 17    | Report Server ID               |
| 22    | Mask Write Register            |
| 23    | Read/Write Multiple Registers  |
| 43/14 | Read Device Identification     |

### Supported protocols

- Modbus RTU
- Modbus ASCII
- Modbus TCP/IP
- Modbus UDP/IP
- Modbus RTU/ASCII Over TCP/IP
- Modbus RTU/ASCII Over UDP/IP

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rs-modbus = "0.1"
```

For serial port support, enable the `serial` feature:

```toml
[dependencies]
rs-modbus = { version = "0.1", features = ["serial"] }
```

## Examples

### Modbus TCP Master

```rust
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
    let res = master.read_holding_registers(1, 0, 10, None).await?;
    println!("{:?}", res);
    master.destroy().await;

    Ok(())
}
```

### Modbus TCP Slave

```rust
use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::TcpServerPhysicalLayer;
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
use rs_modbus::types::AddressRange;
use async_trait::async_trait;
use std::sync::Arc;

struct SimpleModel;

#[async_trait]
impl ModbusSlaveModel for SimpleModel {
    fn unit(&self) -> u8 { 1 }
    fn address_range(&self) -> AddressRange {
        AddressRange::default()
    }
    async fn read_holding_registers(
        &self, _address: u16, length: u16,
    ) -> Result<Vec<u16>, rs_modbus::error::ModbusError> {
        Ok(vec![0; length as usize])
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let physical = TcpServerPhysicalLayer::new();
    physical.set_addr("0.0.0.0:502".to_string()).await;
    let application = TcpApplicationLayer::new();
    let slave = ModbusSlave::new(Arc::new(application), physical);

    slave.add(Box::new(SimpleModel)).await;
    slave.open().await?;

    Ok(())
}
```

## License

MIT
