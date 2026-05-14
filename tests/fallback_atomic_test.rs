//! FC22/FC23 fallback atomicity — per-address lock (Item #17)
//!
//! When the slave model does **not** implement `mask_write_register` or
//! `write_multiple_registers`, the default trait implementation falls back to
//! `readHoldingRegisters` + `writeSingleRegister`. Under concurrent access from
//! two connections these fallback paths must be serialized per-address so that
//! the read-modify-write is atomic.

use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::TcpApplicationLayer;
use rs_modbus::layers::physical::{PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::master::{ModbusMaster, ModbusMasterOptions};
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
use rs_modbus::types::AddressRange;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const UNIT: u8 = 1;

#[derive(Clone)]
struct AtomicTestModel {
    unit: u8,
    registers: Arc<Mutex<HashMap<u16, u16>>>,
    concurrent_reads: Arc<Mutex<u32>>,
    max_concurrent_reads: Arc<Mutex<u32>>,
    concurrent_writes: Arc<Mutex<u32>>,
    max_concurrent_writes: Arc<Mutex<u32>>,
}

#[async_trait::async_trait]
impl ModbusSlaveModel for AtomicTestModel {
    fn unit(&self) -> u8 {
        self.unit
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
        let mut reads = self.concurrent_reads.lock().await;
        *reads += 1;
        let mut max = self.max_concurrent_reads.lock().await;
        *max = (*max).max(*reads);
        drop(max);
        drop(reads);

        // Hold the "critical section" long enough for overlap detection.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let guard = self.registers.lock().await;
        let result: Vec<u16> = (0..length)
            .map(|i| *guard.get(&(address + i)).unwrap_or(&0))
            .collect();
        drop(guard);

        let mut reads = self.concurrent_reads.lock().await;
        *reads -= 1;
        Ok(result)
    }

    async fn write_single_register(&self, address: u16, value: u16) -> Result<(), ModbusError> {
        let mut writes = self.concurrent_writes.lock().await;
        *writes += 1;
        let mut max = self.max_concurrent_writes.lock().await;
        *max = (*max).max(*writes);
        drop(max);
        drop(writes);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let mut guard = self.registers.lock().await;
        guard.insert(address, value);
        drop(guard);

        let mut writes = self.concurrent_writes.lock().await;
        *writes -= 1;
        Ok(())
    }

    // Intentionally do NOT implement mask_write_register or
    // write_multiple_registers so the slave falls back to the trait defaults.
}

async fn setup_slave() -> (
    Arc<TcpServerPhysicalLayer>,
    Arc<TcpApplicationLayer>,
    Arc<ModbusSlave<TcpApplicationLayer, TcpServerPhysicalLayer>>,
    Arc<AtomicTestModel>,
    String,
) {
    let server = TcpServerPhysicalLayer::new();
    server.set_addr("127.0.0.1:0".to_string()).await;

    let app = TcpApplicationLayer::new(server.clone());
    let slave = Arc::new(ModbusSlave::new(app.clone(), server.clone()));

    let model = Arc::new(AtomicTestModel {
        unit: UNIT,
        registers: Arc::new(Mutex::new(HashMap::new())),
        concurrent_reads: Arc::new(Mutex::new(0)),
        max_concurrent_reads: Arc::new(Mutex::new(0)),
        concurrent_writes: Arc::new(Mutex::new(0)),
        max_concurrent_writes: Arc::new(Mutex::new(0)),
    });

    slave.add(Box::new((*model).clone()));
    slave.open(None).await.unwrap();
    let addr = server.get_addr().await.unwrap();

    (server, app, slave, model, addr)
}

async fn setup_master(addr: &str) -> ModbusMaster<TcpApplicationLayer, TcpClientPhysicalLayer> {
    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr.to_string()).await;

    let app = TcpApplicationLayer::new(client.clone());
    let master = ModbusMaster::new(
        app,
        client.clone(),
        ModbusMasterOptions {
            timeout_ms: 2000,
            concurrent: false,
        },
    );
    master.open(None).await.unwrap();
    master
}

#[tokio::test]
async fn fc22_fallback_two_concurrent_requests_same_address_serializes_reads() {
    let (server, _app, _slave, model, addr) = setup_slave().await;
    let a = Arc::new(setup_master(&addr).await);
    let b = Arc::new(setup_master(&addr).await);

    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let (r1, r2) = tokio::join!(
        async move { a2.mask_write_register(UNIT, 0, 0xffff, 0x1234, None).await },
        async move { b2.mask_write_register(UNIT, 0, 0xffff, 0x5678, None).await },
    );

    // Both should succeed.
    assert!(r1.is_ok(), "first mask_write failed: {:?}", r1);
    assert!(r2.is_ok(), "second mask_write failed: {:?}", r2);

    // With per-address locking the two readHoldingRegisters calls must not
    // overlap. Without locking they would both enter concurrently.
    let max_reads = *model.max_concurrent_reads.lock().await;
    assert_eq!(
        max_reads, 1,
        "expected maxConcurrentReads = 1 (locked), got {}",
        max_reads
    );

    a.destroy().await;
    b.destroy().await;
    server.destroy().await;
}

#[tokio::test]
async fn fc23_fallback_two_concurrent_requests_serializes_writes() {
    let (server, _app, _slave, model, addr) = setup_slave().await;
    let a = Arc::new(setup_master(&addr).await);
    let b = Arc::new(setup_master(&addr).await);

    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let (r1, r2) = tokio::join!(
        async move {
            a2.read_and_write_multiple_registers(UNIT, 10, 1, 0, &[0xaaaa, 0x1111], None)
                .await
        },
        async move {
            b2.read_and_write_multiple_registers(UNIT, 10, 1, 0, &[0xbbbb, 0x2222], None)
                .await
        },
    );

    assert!(r1.is_ok(), "first r/w failed: {:?}", r1);
    assert!(r2.is_ok(), "second r/w failed: {:?}", r2);

    // Both requests write to addresses [0, 1]. With per-address locking
    // their writeSingleRegister calls must not overlap.
    let max_writes = *model.max_concurrent_writes.lock().await;
    assert_eq!(
        max_writes, 1,
        "expected maxConcurrentWrites = 1 (locked), got {}",
        max_writes
    );

    a.destroy().await;
    b.destroy().await;
    server.destroy().await;
}
