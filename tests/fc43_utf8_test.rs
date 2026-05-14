//! FC43/14 UTF-8 multibyte object values (035dbd1)
//!
//! Verifies that non-ASCII characters (e.g. accented letters) round-trip
//! correctly between slave encoding and master parsing. In Rust `String::len()`
//! returns UTF-8 byte count, which matches the wire length, so this is
//! primarily a regression guard.

use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::{TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::master::{ModbusMaster, ModbusMasterOptions};
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
use rs_modbus::types::AddressRange;
use std::collections::HashMap;

const UNIT: u8 = 1;

struct Utf8IdentModel {
    ident: HashMap<u8, String>,
}

#[async_trait::async_trait]
impl ModbusSlaveModel for Utf8IdentModel {
    fn unit(&self) -> u8 {
        UNIT
    }

    fn address_range(&self) -> AddressRange {
        AddressRange::default()
    }

    async fn read_device_identification(
        &self,
    ) -> Result<HashMap<u8, String>, rs_modbus::error::ModbusError> {
        Ok(self.ident.clone())
    }
}

#[tokio::test]
async fn round_trip_multibyte_utf8_value() {
    let server = TcpServerPhysicalLayer::new();
    server.set_addr("127.0.0.1:0".to_string()).await;

    let app = TcpApplicationLayer::new(server.clone());
    let slave = ModbusSlave::new(app.clone(), server.clone());

    let mut ident = HashMap::new();
    ident.insert(0x00, "Vend\u{e9}r".to_string()); // é = 2 UTF-8 bytes
    ident.insert(0x01, "ProductCode".to_string());
    ident.insert(0x02, "MajorMinorRevision".to_string());
    slave.add(Box::new(Utf8IdentModel { ident }));
    slave.open(None).await.unwrap();
    let addr = server.get_addr().await.unwrap();

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    let master_app = TcpApplicationLayer::new(client.clone());
    let master = ModbusMaster::new(
        master_app,
        client,
        ModbusMasterOptions {
            timeout_ms: 1000,
            concurrent: false,
        },
    );
    master.open(None).await.unwrap();

    let result = master
        .read_device_identification(UNIT, 0x01, 0x00, None)
        .await;

    let device = result
        .expect("should receive device identification")
        .expect("non-empty response");
    assert_eq!(device.data.objects.len(), 3);
    assert_eq!(device.data.objects[0].id, 0x00);
    assert_eq!(device.data.objects[0].value, "Vend\u{e9}r");

    master.destroy().await;
    slave.destroy().await;
}
