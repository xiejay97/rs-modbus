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
    let application = TcpApplicationLayer::new(physical.clone());
    let slave = ModbusSlave::new(application, physical);

    slave.add(Box::new(SimpleModel)).await;
    slave.open().await?;

    Ok(())
}
```

## Broadcasts (unit = 0)

Slaves never respond to broadcast requests, so the master's write methods with `unit = 0`
return as soon as the bytes are flushed to the wire.

If you broadcast over serial (RTU or ASCII), per Modbus over Serial Line V1.02 §2.4.1
you must wait a **turnaround delay** before sending the next request — slow slaves need
time to apply the broadcast write that produced no response. The library does not insert
this delay automatically because the right value is workload-specific (fast sensors vs.
PLCs writing to flash differ by orders of magnitude). Insert it yourself at the call site:

```rust
master.write_single_register(0, 0, 0x1234, None).await?; // broadcast
sleep(Duration::from_millis(100)).await;                   // turnaround — tune per devices
master.write_single_register(1, 0, 0x5678, None).await?; // unicast to next slave
```

A safe lower bound is the RTU t3.5 inter-frame silence (e.g. ~4 ms at 9600 baud,
~1.75 ms above 19200 baud). Many real-world PLCs need 50–100 ms after a broadcast write.
Modbus TCP/UDP do not require this delay (TCP gives synchronous acks; broadcasting on TCP
is uncommon anyway).

## License

MIT
