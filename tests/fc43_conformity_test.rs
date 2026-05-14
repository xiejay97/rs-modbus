//! Mirrors `njs-modbus/test/fc43-conformity.test.ts`. Covers the FC43/14
//! `conformityLevel` boundary: any registered object at id >= 0x80 must
//! promote the device to 0x83 (Extended). The pre-fix code used `id > 0x80`
//! and under-reported as 0x82 when the only Extended object was exactly
//! at 0x80.

use async_trait::async_trait;
use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::{TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::master::{ModbusMaster, ModbusMasterOptions};
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
use rs_modbus::types::AddressRange;
use std::collections::HashMap;
use std::sync::Arc;

const UNIT: u8 = 1;

struct IdentModel {
    objects: HashMap<u8, String>,
}

#[async_trait]
impl ModbusSlaveModel for IdentModel {
    fn unit(&self) -> u8 {
        UNIT
    }

    fn address_range(&self) -> AddressRange {
        AddressRange::default()
    }

    async fn read_device_identification(&self) -> Result<HashMap<u8, String>, ModbusError> {
        Ok(self.objects.clone())
    }
}

async fn fetch_conformity_level(objects: HashMap<u8, String>) -> u8 {
    let server_phy = TcpServerPhysicalLayer::new();
    server_phy.set_addr("127.0.0.1:0".to_string()).await;
    let server_app = TcpApplicationLayer::new(server_phy.clone());
    let slave = ModbusSlave::new(server_app, Arc::clone(&server_phy));
    slave.add(Box::new(IdentModel { objects }));
    slave.open(None).await.unwrap();

    let addr = server_phy.get_addr().await.unwrap();
    let client_phy = TcpClientPhysicalLayer::new();
    client_phy.set_addr(addr).await;
    let client_app = TcpApplicationLayer::new(client_phy.clone());
    let master = ModbusMaster::new(
        client_app,
        client_phy,
        ModbusMasterOptions {
            timeout_ms: 1000,
            concurrent: false,
        },
    );
    master.open(None).await.unwrap();

    let res = master
        .read_device_identification(UNIT, 0x01, 0x00, None)
        .await
        .expect("read should succeed")
        .expect("identification should be present");
    let level = res.data.conformity_level;

    master.destroy().await;
    slave.destroy().await;
    level
}

#[tokio::test]
async fn basic_only_reports_0x81() {
    let mut objects = HashMap::new();
    objects.insert(0x00u8, "VendorName".to_string());
    objects.insert(0x01u8, "ProductCode".to_string());
    objects.insert(0x02u8, "MajorMinorRevision".to_string());
    let level = fetch_conformity_level(objects).await;
    assert_eq!(level, 0x81, "basic-only must report 0x81");
}

#[tokio::test]
async fn basic_plus_regular_reports_0x82() {
    let mut objects = HashMap::new();
    objects.insert(0x00u8, "VendorName".to_string());
    objects.insert(0x01u8, "ProductCode".to_string());
    objects.insert(0x02u8, "MajorMinorRevision".to_string());
    objects.insert(0x05u8, "ModelName".to_string());
    let level = fetch_conformity_level(objects).await;
    assert_eq!(level, 0x82, "basic + regular must report 0x82");
}

#[tokio::test]
async fn basic_plus_extended_at_0x80_boundary_reports_0x83() {
    // The bug: id >0x80 instead of id >=0x80, so an Extended object
    // exactly at the 0x80 boundary used to under-report as 0x82.
    let mut objects = HashMap::new();
    objects.insert(0x00u8, "VendorName".to_string());
    objects.insert(0x01u8, "ProductCode".to_string());
    objects.insert(0x02u8, "MajorMinorRevision".to_string());
    objects.insert(0x80u8, "VendorSpecific0x80".to_string());
    let level = fetch_conformity_level(objects).await;
    assert_eq!(
        level, 0x83,
        "Extended object at exactly 0x80 must promote to 0x83"
    );
}

#[tokio::test]
async fn basic_plus_extended_at_0x81_reports_0x83() {
    let mut objects = HashMap::new();
    objects.insert(0x00u8, "VendorName".to_string());
    objects.insert(0x01u8, "ProductCode".to_string());
    objects.insert(0x02u8, "MajorMinorRevision".to_string());
    objects.insert(0x81u8, "VendorSpecific0x81".to_string());
    let level = fetch_conformity_level(objects).await;
    assert_eq!(level, 0x83);
}

#[tokio::test]
async fn basic_plus_regular_plus_extended_at_0x80_and_0xff_reports_0x83() {
    let mut objects = HashMap::new();
    objects.insert(0x00u8, "VendorName".to_string());
    objects.insert(0x01u8, "ProductCode".to_string());
    objects.insert(0x02u8, "MajorMinorRevision".to_string());
    objects.insert(0x05u8, "ModelName".to_string());
    objects.insert(0x80u8, "VendorSpecific0x80".to_string());
    objects.insert(0xffu8, "VendorSpecific0xFF".to_string());
    let level = fetch_conformity_level(objects).await;
    assert_eq!(level, 0x83);
}
