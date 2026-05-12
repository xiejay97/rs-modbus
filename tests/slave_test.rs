use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::{TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::master::ModbusMaster;
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
use rs_modbus::types::{AddressRange, ServerId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const UNIT: u8 = 1;

struct TestModel {
    unit: u8,
    coils: Arc<Mutex<HashMap<u16, bool>>>,
    discrete_inputs: Arc<Mutex<HashMap<u16, bool>>>,
    holding_registers: Arc<Mutex<HashMap<u16, u16>>>,
    input_registers: Arc<Mutex<HashMap<u16, u16>>>,
}

#[async_trait::async_trait]
impl ModbusSlaveModel for TestModel {
    fn unit(&self) -> u8 {
        self.unit
    }

    fn address_range(&self) -> AddressRange {
        AddressRange {
            discrete_inputs: vec![(0, 65535)],
            coils: vec![(0, 65535)],
            input_registers: vec![(0, 65535)],
            holding_registers: vec![(0, 65535)],
        }
    }

    async fn read_coils(
        &self,
        address: u16,
        length: u16,
    ) -> Result<Vec<bool>, rs_modbus::error::ModbusError> {
        let guard = self.coils.lock().await;
        Ok((0..length)
            .map(|i| *guard.get(&(address + i)).unwrap_or(&false))
            .collect())
    }

    async fn read_discrete_inputs(
        &self,
        address: u16,
        length: u16,
    ) -> Result<Vec<bool>, rs_modbus::error::ModbusError> {
        let guard = self.discrete_inputs.lock().await;
        Ok((0..length)
            .map(|i| *guard.get(&(address + i)).unwrap_or(&false))
            .collect())
    }

    async fn read_holding_registers(
        &self,
        address: u16,
        length: u16,
    ) -> Result<Vec<u16>, rs_modbus::error::ModbusError> {
        let guard = self.holding_registers.lock().await;
        Ok((0..length)
            .map(|i| *guard.get(&(address + i)).unwrap_or(&0))
            .collect())
    }

    async fn read_input_registers(
        &self,
        address: u16,
        length: u16,
    ) -> Result<Vec<u16>, rs_modbus::error::ModbusError> {
        let guard = self.input_registers.lock().await;
        Ok((0..length)
            .map(|i| *guard.get(&(address + i)).unwrap_or(&0))
            .collect())
    }

    async fn write_single_coil(
        &self,
        address: u16,
        value: bool,
    ) -> Result<(), rs_modbus::error::ModbusError> {
        self.coils.lock().await.insert(address, value);
        Ok(())
    }

    async fn write_single_register(
        &self,
        address: u16,
        value: u16,
    ) -> Result<(), rs_modbus::error::ModbusError> {
        self.holding_registers.lock().await.insert(address, value);
        Ok(())
    }

    async fn write_multiple_coils(
        &self,
        address: u16,
        values: &[bool],
    ) -> Result<(), rs_modbus::error::ModbusError> {
        let mut guard = self.coils.lock().await;
        for (i, &v) in values.iter().enumerate() {
            guard.insert(address + i as u16, v);
        }
        Ok(())
    }

    async fn write_multiple_registers(
        &self,
        address: u16,
        values: &[u16],
    ) -> Result<(), rs_modbus::error::ModbusError> {
        let mut guard = self.holding_registers.lock().await;
        for (i, &v) in values.iter().enumerate() {
            guard.insert(address + i as u16, v);
        }
        Ok(())
    }

    async fn mask_write_register(
        &self,
        address: u16,
        and_mask: u16,
        or_mask: u16,
    ) -> Result<(), rs_modbus::error::ModbusError> {
        let mut guard = self.holding_registers.lock().await;
        let current = *guard.get(&address).unwrap_or(&0);
        guard.insert(address, (current & and_mask) | (or_mask & !and_mask));
        Ok(())
    }

    async fn report_server_id(&self) -> Result<ServerId, rs_modbus::error::ModbusError> {
        Ok(ServerId {
            server_id: self.unit,
            run_indicator_status: true,
            additional_data: vec![1, 2, 3],
        })
    }

    async fn read_device_identification(
        &self,
    ) -> Result<HashMap<u8, String>, rs_modbus::error::ModbusError> {
        let mut map = HashMap::new();
        map.insert(0x00, "VendorName".to_string());
        map.insert(0x01, "ProductCode".to_string());
        map.insert(0x02, "MajorMinorRevision".to_string());
        Ok(map)
    }
}

async fn create_slave() -> (
    ModbusSlave<TcpApplicationLayer, TcpServerPhysicalLayer>,
    Arc<TcpServerPhysicalLayer>,
    Arc<Mutex<HashMap<u16, bool>>>,
    Arc<Mutex<HashMap<u16, bool>>>,
    Arc<Mutex<HashMap<u16, u16>>>,
    Arc<Mutex<HashMap<u16, u16>>>,
) {
    let physical = TcpServerPhysicalLayer::new();
    physical.set_addr("127.0.0.1:0".to_string()).await;
    let application = TcpApplicationLayer::new();
    let slave = ModbusSlave::new(Arc::new(application), Arc::clone(&physical));

    let coils = Arc::new(Mutex::new(HashMap::new()));
    let discrete_inputs = Arc::new(Mutex::new(HashMap::new()));
    let holding_registers = Arc::new(Mutex::new(HashMap::new()));
    let input_registers = Arc::new(Mutex::new(HashMap::new()));

    let model = TestModel {
        unit: UNIT,
        coils: Arc::clone(&coils),
        discrete_inputs: Arc::clone(&discrete_inputs),
        holding_registers: Arc::clone(&holding_registers),
        input_registers: Arc::clone(&input_registers),
    };

    slave.add(Box::new(model)).await;
    slave.open().await.unwrap();

    (
        slave,
        physical,
        coils,
        discrete_inputs,
        holding_registers,
        input_registers,
    )
}

async fn create_master(
    server: &TcpServerPhysicalLayer,
) -> ModbusMaster<TcpApplicationLayer, TcpClientPhysicalLayer> {
    let addr = server.get_addr().await.unwrap();
    let physical = TcpClientPhysicalLayer::new();
    physical.set_addr(addr).await;
    let application = TcpApplicationLayer::new();
    let master = ModbusMaster::new(Arc::new(application), physical, 1000);
    master.open().await.unwrap();
    master
}

#[tokio::test]
async fn test_fc1_read_coils() {
    let (slave, server, coils, _di, _hr, _ir) = create_slave().await;
    let master = create_master(&server).await;

    coils.lock().await.insert(0, true);
    coils.lock().await.insert(1, false);
    coils.lock().await.insert(2, true);

    let res = master.read_coils(UNIT, 0, 3, None).await.unwrap().unwrap();
    assert_eq!(res, vec![true, false, true]);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc2_read_discrete_inputs() {
    let (slave, server, _coils, discrete_inputs, _hr, _ir) = create_slave().await;
    let master = create_master(&server).await;

    discrete_inputs.lock().await.insert(10, true);
    discrete_inputs.lock().await.insert(11, true);
    discrete_inputs.lock().await.insert(12, false);

    let res = master
        .read_discrete_inputs(UNIT, 10, 3, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, vec![true, true, false]);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc3_read_holding_registers() {
    let (slave, server, _coils, _di, holding_registers, _ir) = create_slave().await;
    let master = create_master(&server).await;

    holding_registers.lock().await.insert(20, 0x1234);
    holding_registers.lock().await.insert(21, 0x5678);

    let res = master
        .read_holding_registers(UNIT, 20, 2, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, vec![0x1234, 0x5678]);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc4_read_input_registers() {
    let (slave, server, _coils, _di, _hr, input_registers) = create_slave().await;
    let master = create_master(&server).await;

    input_registers.lock().await.insert(30, 0xabcd);
    input_registers.lock().await.insert(31, 0xef01);

    let res = master
        .read_input_registers(UNIT, 30, 2, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, vec![0xabcd, 0xef01]);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc5_write_single_coil() {
    let (slave, server, coils, _di, _hr, _ir) = create_slave().await;
    let master = create_master(&server).await;

    let res = master
        .write_single_coil(UNIT, 40, true, None)
        .await
        .unwrap();
    assert_eq!(res, Some(true));
    assert!(*coils.lock().await.get(&40).unwrap());

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc6_write_single_register() {
    let (slave, server, _coils, _di, holding_registers, _ir) = create_slave().await;
    let master = create_master(&server).await;

    let res = master
        .write_single_register(UNIT, 50, 0xdead, None)
        .await
        .unwrap();
    assert_eq!(res, Some(0xdead));
    assert_eq!(*holding_registers.lock().await.get(&50).unwrap(), 0xdead);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc15_write_multiple_coils() {
    let (slave, server, coils, _di, _hr, _ir) = create_slave().await;
    let master = create_master(&server).await;

    let res = master
        .write_multiple_coils(UNIT, 60, &[true, false, true, true], None)
        .await
        .unwrap();
    assert_eq!(res, Some(vec![true, false, true, true]));

    let guard = coils.lock().await;
    assert!(*guard.get(&60).unwrap());
    assert!(!*guard.get(&61).unwrap());
    assert!(*guard.get(&62).unwrap());
    assert!(*guard.get(&63).unwrap());

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc16_write_multiple_registers() {
    let (slave, server, _coils, _di, holding_registers, _ir) = create_slave().await;
    let master = create_master(&server).await;

    let res = master
        .write_multiple_registers(UNIT, 70, &[0x1111, 0x2222, 0x3333], None)
        .await
        .unwrap();
    assert_eq!(res, Some(vec![0x1111, 0x2222, 0x3333]));

    let guard = holding_registers.lock().await;
    assert_eq!(*guard.get(&70).unwrap(), 0x1111);
    assert_eq!(*guard.get(&71).unwrap(), 0x2222);
    assert_eq!(*guard.get(&72).unwrap(), 0x3333);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc17_report_server_id() {
    let (slave, server, _coils, _di, _hr, _ir) = create_slave().await;
    let master = create_master(&server).await;

    let res = master.report_server_id(UNIT, None).await.unwrap().unwrap();
    assert_eq!(res.server_id, UNIT);
    assert!(res.run_indicator_status);
    assert_eq!(res.additional_data, vec![1, 2, 3]);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc22_mask_write_register() {
    let (slave, server, _coils, _di, holding_registers, _ir) = create_slave().await;
    let master = create_master(&server).await;

    holding_registers.lock().await.insert(80, 0b11110000);

    let res = master
        .mask_write_register(UNIT, 80, 0b00001111, 0b10101010, None)
        .await
        .unwrap();
    assert_eq!(res, Some((0b00001111, 0b10101010)));

    #[allow(clippy::identity_op)]
    let expected =
        (0b11110000u16 & 0b00001111u16) | (0b10101010u16 & !0b00001111u16);
    assert_eq!(*holding_registers.lock().await.get(&80).unwrap(), expected);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc23_read_and_write_multiple_registers() {
    let (slave, server, _coils, _di, holding_registers, _ir) = create_slave().await;
    let master = create_master(&server).await;

    holding_registers.lock().await.insert(90, 0xaaaa);
    holding_registers.lock().await.insert(91, 0xbbbb);

    let res = master
        .read_and_write_multiple_registers(UNIT, 90, 2, 92, &[0xcccc, 0xdddd], None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, vec![0xaaaa, 0xbbbb]);

    let guard = holding_registers.lock().await;
    assert_eq!(*guard.get(&92).unwrap(), 0xcccc);
    assert_eq!(*guard.get(&93).unwrap(), 0xdddd);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_fc43_14_read_device_identification() {
    let (slave, server, _coils, _di, _hr, _ir) = create_slave().await;
    let master = create_master(&server).await;

    let res = master
        .read_device_identification(UNIT, 0x01, 0x00, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res.read_device_id_code, 0x01);
    assert_eq!(res.conformity_level, 0x81);
    assert!(!res.more_follows);
    assert_eq!(res.objects.len(), 3);
    assert_eq!(res.objects[0].id, 0x00);
    assert_eq!(res.objects[0].value, "VendorName");
    assert_eq!(res.objects[1].id, 0x01);
    assert_eq!(res.objects[1].value, "ProductCode");
    assert_eq!(res.objects[2].id, 0x02);
    assert_eq!(res.objects[2].value, "MajorMinorRevision");

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_broadcast_write() {
    let (slave, server, _coils, _di, holding_registers, _ir) = create_slave().await;
    let master = create_master(&server).await;

    let res = master.write_single_register(0, 100, 0x9999, None).await;
    assert_eq!(res.unwrap(), None); // broadcast returns None

    // Wait a bit for the slave to process
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    assert_eq!(*holding_registers.lock().await.get(&100).unwrap(), 0x9999);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_queue_ordering() {
    let (slave, server, _coils, _di, holding_registers, _ir) = create_slave().await;
    let master = create_master(&server).await;

    holding_registers.lock().await.insert(200, 0x0001);
    holding_registers.lock().await.insert(201, 0x0002);
    holding_registers.lock().await.insert(202, 0x0003);

    let res1 = master
        .read_holding_registers(UNIT, 200, 1, None)
        .await
        .unwrap()
        .unwrap();
    let res2 = master
        .read_holding_registers(UNIT, 201, 1, None)
        .await
        .unwrap()
        .unwrap();
    let res3 = master
        .read_holding_registers(UNIT, 202, 1, None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(res1, vec![0x0001]);
    assert_eq!(res2, vec![0x0002]);
    assert_eq!(res3, vec![0x0003]);

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_illegal_data_value() {
    let (slave, server, _coils, _di, _hr, _ir) = create_slave().await;
    let master = create_master(&server).await;

    // length=0 is out of spec, should timeout (slave drops invalid request)
    let res = master.read_coils(UNIT, 0, 0, Some(200)).await;
    assert!(res.is_err() || res.unwrap().is_none());

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_illegal_data_address() {
    let (slave, server, _coils, _di, _hr, _ir) = create_slave().await;
    let master = create_master(&server).await;

    // Add a second model with restricted range
    let restricted_model = TestModel {
        unit: 2,
        coils: Arc::new(Mutex::new(HashMap::new())),
        discrete_inputs: Arc::new(Mutex::new(HashMap::new())),
        holding_registers: Arc::new(Mutex::new(HashMap::new())),
        input_registers: Arc::new(Mutex::new(HashMap::new())),
    };

    // Override address range for unit 2
    struct RestrictedModel(TestModel);
    #[async_trait::async_trait]
    impl ModbusSlaveModel for RestrictedModel {
        fn unit(&self) -> u8 {
            self.0.unit
        }
        fn address_range(&self) -> AddressRange {
            AddressRange {
                coils: vec![(100, 200)],
                ..Default::default()
            }
        }
        async fn read_coils(
            &self,
            address: u16,
            length: u16,
        ) -> Result<Vec<bool>, rs_modbus::error::ModbusError> {
            self.0.read_coils(address, length).await
        }
    }

    slave.add(Box::new(RestrictedModel(restricted_model))).await;

    // Request to unit 2 with address out of range should get exception
    let res = master.read_coils(2, 0, 10, Some(200)).await;
    // Slave returns exception, Master preCheck doesn't match exception FC,
    // so it should timeout or fail
    assert!(res.is_err() || res.unwrap().is_none());

    master.destroy().await;
    slave.destroy().await;
}

#[tokio::test]
async fn test_timeout() {
    let (slave, server, _coils, _di, _hr, _ir) = create_slave().await;
    let master = create_master(&server).await;

    // Request to non-existent unit should timeout
    let res = master.read_coils(99, 0, 1, Some(100)).await;
    assert!(matches!(res, Err(rs_modbus::error::ModbusError::Timeout)));

    master.destroy().await;
    slave.destroy().await;
}
