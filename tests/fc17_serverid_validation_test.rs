//! FC17 Server ID validation tests (Item #13)

use async_trait::async_trait;
use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::{TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::master::{ModbusMaster, ModbusMasterOptions};
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
use rs_modbus::types::{AddressRange, ServerId};

struct TestModel {
    server_id: Vec<u8>,
    run_indicator: bool,
    additional_data: Vec<u8>,
}

#[async_trait]
impl ModbusSlaveModel for TestModel {
    fn unit(&self) -> u8 {
        1
    }

    fn address_range(&self) -> AddressRange {
        AddressRange::default()
    }

    async fn report_server_id(&self) -> Result<ServerId, ModbusError> {
        Ok(ServerId {
            server_id: self.server_id.clone(),
            run_indicator_status: self.run_indicator,
            additional_data: self.additional_data.clone(),
        })
    }
}

async fn setup() -> (
    ModbusSlave<TcpApplicationLayer, TcpServerPhysicalLayer>,
    ModbusMaster<TcpApplicationLayer, TcpClientPhysicalLayer>,
) {
    let server_physical = TcpServerPhysicalLayer::new();
    server_physical.set_addr("127.0.0.1:0".to_string()).await;

    let server_app = TcpApplicationLayer::new(server_physical.clone());
    let slave = ModbusSlave::new(server_app, server_physical.clone());

    slave
        .add(Box::new(TestModel {
            server_id: vec![1],
            run_indicator: true,
            additional_data: vec![],
        }))
        .await;
    slave.open().await.unwrap();

    let addr = server_physical.get_addr().await.unwrap();

    let client_physical = TcpClientPhysicalLayer::new();
    client_physical.set_addr(addr).await;

    let client_app = TcpApplicationLayer::new(client_physical.clone());
    let master = ModbusMaster::new(
        client_app,
        client_physical,
        ModbusMasterOptions {
            timeout_ms: 1000,
            concurrent: false,
        },
    );

    master.open().await.unwrap();
    (slave, master)
}

#[tokio::test]
async fn accepts_multi_byte_server_id() {
    let (slave, master) = setup().await;

    slave
        .add(Box::new(TestModel {
            server_id: vec![0x01, 0x02, 0x03],
            run_indicator: true,
            additional_data: vec![0xab, 0xcd],
        }))
        .await;

    let res = master.report_server_id(1, 3, None).await.unwrap();
    assert!(res.is_some());
    let sid = res.unwrap();
    assert_eq!(sid.server_id, vec![0x01, 0x02, 0x03]);
    assert!(sid.run_indicator_status);
    assert_eq!(sid.additional_data, vec![0xab, 0xcd]);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn rejects_bytecount_overflow() {
    let (slave, master) = setup().await;

    // server_id(1) + run_status(1) + additional_data(254) = 256 > 255
    slave
        .add(Box::new(TestModel {
            server_id: vec![1],
            run_indicator: true,
            additional_data: vec![0; 254],
        }))
        .await;

    let res = master.report_server_id(1, 1, None).await;
    assert!(
        res.is_err(),
        "expected error for byteCount overflow, got {:?}",
        res
    );

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn accepts_large_additional_data_within_tcp_limit() {
    let (slave, master) = setup().await;

    let n = 200;
    slave
        .add(Box::new(TestModel {
            server_id: vec![1],
            run_indicator: true,
            additional_data: vec![0xab; n],
        }))
        .await;

    let res = master.report_server_id(1, 1, None).await.unwrap();
    assert!(res.is_some());
    let sid = res.unwrap();
    assert_eq!(sid.additional_data.len(), n);
    assert_eq!(sid.additional_data[0], 0xab);
    assert_eq!(sid.additional_data[n - 1], 0xab);

    master.destroy().await;
    slave.destroy().await;
}
