//! Mirrors `njs-modbus/test/slave-multi-connection.test.ts`. Covers
//! Item #4: per-connection FIFO at the slave (so a slow handler on one
//! TCP client cannot block another client's heartbeat) and TCP opt-in
//! concurrent mode (pipelined intra-connection dispatch).

use async_trait::async_trait;
use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::ApplicationLayer;
use rs_modbus::layers::application::{
    AsciiApplicationLayer, RtuApplicationLayer, TcpApplicationLayer,
};
use rs_modbus::layers::physical::{PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::master::{ModbusMaster, ModbusMasterOptions};
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel, ModbusSlaveOptions};
use rs_modbus::types::{AddressRange, ApplicationDataUnit};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const UNIT: u8 = 1;

struct ToggleableLatencyModel {
    latency_by_address: Arc<Mutex<HashMap<u16, u64>>>,
    processed: Arc<Mutex<Vec<u16>>>,
}

#[async_trait]
impl ModbusSlaveModel for ToggleableLatencyModel {
    fn unit(&self) -> u8 {
        UNIT
    }

    fn address_range(&self) -> AddressRange {
        AddressRange {
            holding_registers: vec![(0, 65535)],
            ..Default::default()
        }
    }

    async fn read_holding_registers(
        &self,
        address: u16,
        length: u16,
    ) -> Result<Vec<u16>, ModbusError> {
        let latency_ms = self
            .latency_by_address
            .lock()
            .await
            .get(&address)
            .copied()
            .unwrap_or(0);
        if latency_ms > 0 {
            tokio::time::sleep(Duration::from_millis(latency_ms)).await;
        }
        self.processed.lock().await.push(address);
        Ok((0..length).map(|i| address.wrapping_add(i)).collect())
    }
}

struct SlaveEnv {
    slave: ModbusSlave<TcpApplicationLayer, TcpServerPhysicalLayer>,
    physical: Arc<TcpServerPhysicalLayer>,
    latency_by_address: Arc<Mutex<HashMap<u16, u64>>>,
    processed: Arc<Mutex<Vec<u16>>>,
}

async fn create_slave(options: ModbusSlaveOptions) -> SlaveEnv {
    let physical = TcpServerPhysicalLayer::new();
    physical.set_addr("127.0.0.1:0".to_string()).await;
    let application = TcpApplicationLayer::new(physical.clone());
    let slave = ModbusSlave::with_options(application, Arc::clone(&physical), options);

    let latency_by_address: Arc<Mutex<HashMap<u16, u64>>> = Arc::new(Mutex::new(HashMap::new()));
    let processed: Arc<Mutex<Vec<u16>>> = Arc::new(Mutex::new(Vec::new()));
    slave
        .add(Box::new(ToggleableLatencyModel {
            latency_by_address: Arc::clone(&latency_by_address),
            processed: Arc::clone(&processed),
        }))
        .await;
    slave.open().await.unwrap();
    SlaveEnv {
        slave,
        physical,
        latency_by_address,
        processed,
    }
}

async fn create_master(
    server: &TcpServerPhysicalLayer,
    options: ModbusMasterOptions,
) -> ModbusMaster<TcpApplicationLayer, TcpClientPhysicalLayer> {
    let addr = server.get_addr().await.unwrap();
    let physical = TcpClientPhysicalLayer::new();
    physical.set_addr(addr).await;
    let application = TcpApplicationLayer::new(physical.clone());
    let master = ModbusMaster::new(application, physical, options);
    master.open().await.unwrap();
    master
}

// ===== Per-connection FIFO: a slow handler on one connection does NOT block another =====

#[tokio::test]
async fn slow_handler_on_one_connection_does_not_block_another() {
    let env = create_slave(ModbusSlaveOptions::default()).await;
    env.latency_by_address.lock().await.insert(100, 800);
    env.latency_by_address.lock().await.insert(200, 0);

    let a = create_master(
        &env.physical,
        ModbusMasterOptions {
            timeout_ms: 2000,
            concurrent: false,
        },
    )
    .await;
    let b = create_master(
        &env.physical,
        ModbusMasterOptions {
            timeout_ms: 2000,
            concurrent: false,
        },
    )
    .await;

    let slow_start = Instant::now();
    let a_task = tokio::spawn(async move {
        let res = a.read_holding_registers(UNIT, 100, 1, None).await;
        (res, a)
    });

    tokio::time::sleep(Duration::from_millis(30)).await;

    let fast_start = Instant::now();
    let fast = b
        .read_holding_registers(UNIT, 200, 1, None)
        .await
        .unwrap()
        .unwrap();
    let fast_elapsed = fast_start.elapsed();

    assert_eq!(fast, vec![200u16]);
    assert!(
        fast_elapsed < Duration::from_millis(400),
        "fast read should not wait for slow handler (got {:?})",
        fast_elapsed
    );

    let (slow_res, a_master) = a_task.await.unwrap();
    let slow_elapsed = slow_start.elapsed();
    let slow = slow_res.unwrap().unwrap();
    assert_eq!(slow, vec![100u16]);
    assert!(
        slow_elapsed >= Duration::from_millis(750),
        "slow read must still incur its full latency (got {:?})",
        slow_elapsed
    );

    a_master.destroy().await;
    b.destroy().await;
    env.slave.destroy().await;
}

// ===== Single connection: requests stay FIFO (descending latencies still process in order) =====

#[tokio::test]
async fn single_connection_requests_stay_fifo() {
    let env = create_slave(ModbusSlaveOptions::default()).await;
    {
        let mut latency = env.latency_by_address.lock().await;
        latency.insert(10, 100);
        latency.insert(11, 50);
        latency.insert(12, 0);
    }

    let master = create_master(
        &env.physical,
        ModbusMasterOptions {
            timeout_ms: 2000,
            concurrent: false,
        },
    )
    .await;

    // Issue three reads on one connection. Default master is FIFO so this
    // serializes at the master end too; the test still asserts the slave
    // processes them in dispatch order via its per-connection queue.
    let r1 = master
        .read_holding_registers(UNIT, 10, 1, None)
        .await
        .unwrap()
        .unwrap();
    let r2 = master
        .read_holding_registers(UNIT, 11, 1, None)
        .await
        .unwrap()
        .unwrap();
    let r3 = master
        .read_holding_registers(UNIT, 12, 1, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r1, vec![10u16]);
    assert_eq!(r2, vec![11u16]);
    assert_eq!(r3, vec![12u16]);
    assert_eq!(env.processed.lock().await.clone(), vec![10, 11, 12]);

    master.destroy().await;
    env.slave.destroy().await;
}

// ===== Concurrent mode (TCP): 5 in-flight requests on one connection run in parallel =====

#[tokio::test]
async fn concurrent_mode_pipelines_5_in_flight_requests() {
    let env = create_slave(ModbusSlaveOptions { concurrent: true }).await;
    for i in 0u16..5 {
        env.latency_by_address.lock().await.insert(500 + i, 150);
    }

    let master = create_master(
        &env.physical,
        ModbusMasterOptions {
            timeout_ms: 2000,
            concurrent: true,
        },
    )
    .await;

    let start = Instant::now();
    let (r0, r1, r2, r3, r4) = tokio::join!(
        master.read_holding_registers(UNIT, 500, 1, None),
        master.read_holding_registers(UNIT, 501, 1, None),
        master.read_holding_registers(UNIT, 502, 1, None),
        master.read_holding_registers(UNIT, 503, 1, None),
        master.read_holding_registers(UNIT, 504, 1, None),
    );
    let elapsed = start.elapsed();

    for (i, res) in [r0, r1, r2, r3, r4].into_iter().enumerate() {
        let v = res.unwrap().unwrap();
        assert_eq!(v, vec![500u16 + i as u16]);
    }
    assert!(
        elapsed < Duration::from_millis(500),
        "5 × 150ms pipelined should finish well under serial 750ms (got {:?})",
        elapsed
    );

    master.destroy().await;
    env.slave.destroy().await;
}

// ===== Concurrent mode requires TCP =====

#[tokio::test]
async fn concurrent_with_rtu_application_layer_panics() {
    let phy = TcpServerPhysicalLayer::new();
    let app = RtuApplicationLayer::new(phy.clone(), None, None);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = ModbusSlave::with_options(
            app.clone(),
            phy.clone(),
            ModbusSlaveOptions { concurrent: true },
        );
    }));
    assert!(result.is_err(), "RTU + concurrent slave should panic");
}

#[tokio::test]
async fn concurrent_with_ascii_application_layer_panics() {
    let phy = TcpServerPhysicalLayer::new();
    let app = AsciiApplicationLayer::new(phy.clone());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = ModbusSlave::with_options(
            app.clone(),
            phy.clone(),
            ModbusSlaveOptions { concurrent: true },
        );
    }));
    assert!(result.is_err(), "ASCII + concurrent slave should panic");
}

// ===== Drop queued requests on connection-close =====

#[tokio::test]
async fn queued_items_dropped_on_connection_close() {
    let env = create_slave(ModbusSlaveOptions::default()).await;
    env.latency_by_address.lock().await.insert(900, 200);

    // Hand-encode three FC3 reads on the same TCP client so the slave's
    // per-connection queue has #901 / #902 still waiting while #900 is
    // being processed. The master's FIFO wrapper would otherwise serialize
    // at the master side; we go through the application/physical layers
    // directly to dispatch back-to-back.
    let client_phy = TcpClientPhysicalLayer::new();
    client_phy
        .set_addr(env.physical.get_addr().await.unwrap())
        .await;
    let client_app = TcpApplicationLayer::new(client_phy.clone());
    client_phy.open().await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mk_read = |address: u16, tid: u16| -> Vec<u8> {
        let mut data = vec![0u8; 4];
        data[0..2].copy_from_slice(&address.to_be_bytes());
        data[2..4].copy_from_slice(&1u16.to_be_bytes());
        let adu = ApplicationDataUnit::new(UNIT, 0x03, data).with_transaction(tid);
        client_app.encode(&adu)
    };

    client_phy.write(&mk_read(900, 1)).await.unwrap();
    client_phy.write(&mk_read(901, 2)).await.unwrap();
    client_phy.write(&mk_read(902, 3)).await.unwrap();

    // Wait long enough for the slave to start processing #900 but not finish it.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Disconnect mid-processing.
    client_phy.destroy().await;
    client_app.destroy().await;

    // Wait for the slave's per-connection drain to wrap up.
    tokio::time::sleep(Duration::from_millis(350)).await;

    let processed = env.processed.lock().await.clone();
    assert!(
        processed.contains(&900),
        "in-flight request 900 should have been processed, got {:?}",
        processed
    );
    assert!(
        !processed.contains(&901),
        "queued #901 must be dropped on disconnect, got {:?}",
        processed
    );
    assert!(
        !processed.contains(&902),
        "queued #902 must be dropped on disconnect, got {:?}",
        processed
    );

    env.slave.destroy().await;
}
