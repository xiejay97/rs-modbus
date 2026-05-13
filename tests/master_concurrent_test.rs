//! Mirrors `njs-modbus/test/master-concurrent.test.ts`. Each `mod` group
//! corresponds to a `describe` block in the TypeScript suite.
//!
//! Concurrent mode + FIFO + TID validation + close-rejects-inflight.

use async_trait::async_trait;
use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::{
    AsciiApplicationLayer, RtuApplicationLayer, RtuApplicationLayerOptions, TcpApplicationLayer,
};
use rs_modbus::layers::physical::{TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::master::{ModbusMaster, ModbusMasterOptions};
use rs_modbus::slave::{ModbusSlave, ModbusSlaveModel};
use rs_modbus::types::AddressRange;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const UNIT: u8 = 1;

struct SlowSlaveModel {
    latency: Duration,
    holding_registers: Arc<Mutex<HashMap<u16, u16>>>,
}

#[async_trait]
impl ModbusSlaveModel for SlowSlaveModel {
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
        tokio::time::sleep(self.latency).await;
        let guard = self.holding_registers.lock().await;
        Ok((0..length)
            .map(|i| {
                let a = address.wrapping_add(i);
                *guard.get(&a).unwrap_or(&a)
            })
            .collect())
    }

    async fn write_single_register(&self, address: u16, value: u16) -> Result<(), ModbusError> {
        self.holding_registers.lock().await.insert(address, value);
        Ok(())
    }
}

async fn create_slow_slave(
    latency: Duration,
) -> (
    ModbusSlave<TcpApplicationLayer, TcpServerPhysicalLayer>,
    Arc<TcpServerPhysicalLayer>,
    Arc<Mutex<HashMap<u16, u16>>>,
) {
    let physical = TcpServerPhysicalLayer::new();
    physical.set_addr("127.0.0.1:0".to_string()).await;
    let application = TcpApplicationLayer::new(physical.clone());
    let slave = ModbusSlave::new(application, Arc::clone(&physical));

    let holding_registers = Arc::new(Mutex::new(HashMap::new()));
    slave
        .add(Box::new(SlowSlaveModel {
            latency,
            holding_registers: Arc::clone(&holding_registers),
        }))
        .await;
    slave.open().await.unwrap();
    (slave, physical, holding_registers)
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

// ===== concurrent mode requires TCP =====

#[tokio::test]
async fn concurrent_rejects_rtu_application_layer() {
    let phy = TcpClientPhysicalLayer::new();
    let app = RtuApplicationLayer::new(phy.clone(), RtuApplicationLayerOptions::default());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = ModbusMaster::new(
            app.clone(),
            phy.clone(),
            ModbusMasterOptions {
                timeout_ms: 1000,
                concurrent: true,
            },
        );
    }));
    assert!(
        result.is_err(),
        "constructing concurrent master with RTU app layer should panic"
    );
    let payload = result.unwrap_err();
    let msg = payload
        .downcast_ref::<&'static str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default();
    assert!(
        msg.contains("concurrent mode requires a Modbus TCP application layer"),
        "panic message should describe the constraint, got: {msg:?}"
    );
}

#[tokio::test]
async fn concurrent_rejects_ascii_application_layer() {
    let phy = TcpClientPhysicalLayer::new();
    let app = AsciiApplicationLayer::new(phy.clone());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = ModbusMaster::new(
            app.clone(),
            phy.clone(),
            ModbusMasterOptions {
                timeout_ms: 1000,
                concurrent: true,
            },
        );
    }));
    assert!(
        result.is_err(),
        "constructing concurrent master with ASCII app layer should panic"
    );
}

#[tokio::test]
async fn concurrent_accepts_tcp_application_layer() {
    let phy = TcpClientPhysicalLayer::new();
    let app = TcpApplicationLayer::new(phy.clone());
    let master = ModbusMaster::new(
        app,
        phy,
        ModbusMasterOptions {
            timeout_ms: 1000,
            concurrent: true,
        },
    );
    assert!(master.concurrent, "concurrent flag should be exposed");
}

// ===== FIFO mode (default): parallel calls must serialize and return their own values =====

#[tokio::test]
async fn fifo_serializes_parallel_calls() {
    let (slave, server, _hr) = create_slow_slave(Duration::from_millis(10)).await;
    let master = create_master(
        &server,
        ModbusMasterOptions {
            timeout_ms: 1000,
            concurrent: false,
        },
    )
    .await;

    // Five reads issued in parallel — must all resolve to their own
    // address-derived value. With the old single-slot session and no
    // FIFO queue, the second caller would overwrite the first one's
    // waiter and at least one read would fail.
    let (r0, r1, r2, r3, r4) = tokio::join!(
        master.read_holding_registers(UNIT, 0, 1, None),
        master.read_holding_registers(UNIT, 1, 1, None),
        master.read_holding_registers(UNIT, 2, 1, None),
        master.read_holding_registers(UNIT, 3, 1, None),
        master.read_holding_registers(UNIT, 4, 1, None),
    );
    assert_eq!(r0.unwrap().unwrap(), vec![0u16]);
    assert_eq!(r1.unwrap().unwrap(), vec![1u16]);
    assert_eq!(r2.unwrap().unwrap(), vec![2u16]);
    assert_eq!(r3.unwrap().unwrap(), vec![3u16]);
    assert_eq!(r4.unwrap().unwrap(), vec![4u16]);

    master.destroy().await;
    slave.destroy().await;
}

// ===== FIFO mode (TCP) TID validation: stale response cannot pollute next call =====

#[tokio::test]
async fn fifo_tid_validation_drops_stale_response() {
    // 200ms slave latency vs 50ms timeout: first request times out, but
    // its response (TID=1) eventually arrives at the socket. The second
    // request gets a fresh TID (=2). Without TID validation, the stale
    // TID=1 response would resolve the second waiter.
    let (slave, server, _hr) = create_slow_slave(Duration::from_millis(200)).await;
    let master = create_master(
        &server,
        ModbusMasterOptions {
            timeout_ms: 50,
            concurrent: false,
        },
    )
    .await;

    // First request: timeout.
    let first = master.read_holding_registers(UNIT, 7, 1, None).await;
    assert!(
        matches!(first, Err(ModbusError::Timeout)),
        "expected first request to time out, got: {first:?}"
    );

    // Let the stale slow response actually arrive at the socket buffer.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Second request: with a 500ms ceiling so the slow slave can respond
    // in time. Should return the second request's own value (42), not 7.
    let second = master
        .read_holding_registers(UNIT, 42, 1, Some(500))
        .await
        .expect("second read should not error")
        .expect("second read should not be None");
    assert_eq!(
        second,
        vec![42u16],
        "second request must return its own value, not the stale 7"
    );

    master.destroy().await;
    slave.destroy().await;
}

// ===== close() rejects every in-flight request immediately =====

#[tokio::test]
async fn close_rejects_inflight_requests() {
    // 500ms slave latency in concurrent mode: both reads are in-flight on
    // the socket. Calling close() in the middle must reject both futures
    // with "Master closed" rather than letting them hang or time out.
    let (slave, server, _hr) = create_slow_slave(Duration::from_millis(500)).await;
    let master = Arc::new(
        create_master(
            &server,
            ModbusMasterOptions {
                timeout_ms: 5000,
                concurrent: true,
            },
        )
        .await,
    );

    let m1 = Arc::clone(&master);
    let h1 = tokio::spawn(async move { m1.read_holding_registers(UNIT, 0, 1, None).await });
    let m2 = Arc::clone(&master);
    let h2 = tokio::spawn(async move { m2.read_holding_registers(UNIT, 1, 1, None).await });

    // Give the writes a moment to actually go out.
    tokio::time::sleep(Duration::from_millis(30)).await;
    master.close().await.expect("close should not error");

    let r1 = h1.await.expect("task panicked");
    let r2 = h2.await.expect("task panicked");
    for (i, res) in [r1, r2].into_iter().enumerate() {
        match res {
            Err(ModbusError::InvalidState(ref s)) if s == "Master closed" => {}
            other => {
                panic!("read #{i} should have been rejected with \"Master closed\", got: {other:?}")
            }
        }
    }

    master.destroy().await;
    slave.destroy().await;
}

// ===== concurrent mode (TCP): parallel requests pipelined without waiter clobber =====

#[tokio::test]
async fn concurrent_mode_dispatches_10_parallel_reads() {
    // 30ms latency per request — without true concurrency, 10 serialized
    // reads would take >=300ms. With pipelined concurrency, all 10 are in
    // flight at once. The functional check below is "each read gets back
    // its OWN value, not someone else's response", which only passes if
    // the master keys waiters by TID instead of using a single slot.
    let (slave, server, _hr) = create_slow_slave(Duration::from_millis(30)).await;
    let master = create_master(
        &server,
        ModbusMasterOptions {
            timeout_ms: 5000,
            concurrent: true,
        },
    )
    .await;

    let (r0, r1, r2, r3, r4, r5, r6, r7, r8, r9) = tokio::join!(
        master.read_holding_registers(UNIT, 100, 1, None),
        master.read_holding_registers(UNIT, 101, 1, None),
        master.read_holding_registers(UNIT, 102, 1, None),
        master.read_holding_registers(UNIT, 103, 1, None),
        master.read_holding_registers(UNIT, 104, 1, None),
        master.read_holding_registers(UNIT, 105, 1, None),
        master.read_holding_registers(UNIT, 106, 1, None),
        master.read_holding_registers(UNIT, 107, 1, None),
        master.read_holding_registers(UNIT, 108, 1, None),
        master.read_holding_registers(UNIT, 109, 1, None),
    );
    let results = [r0, r1, r2, r3, r4, r5, r6, r7, r8, r9];
    for (i, res) in results.into_iter().enumerate() {
        let value = res.unwrap().unwrap();
        let expected = 100u16 + i as u16;
        assert_eq!(value, vec![expected], "concurrent read #{i} mismatch");
    }

    master.destroy().await;
    slave.destroy().await;
}

// ===== FIFO close() rejects queued requests waiting their turn =====

#[tokio::test]
async fn fifo_close_rejects_queued_requests() {
    // Three FIFO requests issued in parallel against a slow slave. The
    // first holds the lock; the other two queue up. After a short delay
    // we call close() — every still-pending request must reject with
    // "Master closed" rather than hang forever waiting on a now-dead lock.
    let (slave, server, _hr) = create_slow_slave(Duration::from_millis(200)).await;
    let master = Arc::new(
        create_master(
            &server,
            ModbusMasterOptions {
                timeout_ms: 5000,
                concurrent: false,
            },
        )
        .await,
    );

    let m0 = Arc::clone(&master);
    let h0 = tokio::spawn(async move { m0.read_holding_registers(UNIT, 0, 1, None).await });
    let m1 = Arc::clone(&master);
    let h1 = tokio::spawn(async move { m1.read_holding_registers(UNIT, 1, 1, None).await });
    let m2 = Arc::clone(&master);
    let h2 = tokio::spawn(async move { m2.read_holding_registers(UNIT, 2, 1, None).await });

    tokio::time::sleep(Duration::from_millis(10)).await;
    master.close().await.expect("close should not error");

    for (i, fut) in [h0, h1, h2].into_iter().enumerate() {
        let res = fut.await.expect("task panicked");
        match res {
            Err(ModbusError::InvalidState(ref s)) if s == "Master closed" => {}
            other => panic!("FIFO read #{i} should reject with \"Master closed\", got: {other:?}"),
        }
    }

    master.destroy().await;
    slave.destroy().await;
}

// ===== open() after close() resets the closed flag (no "Master closed" surprise) =====

#[tokio::test]
async fn reopen_after_close_allows_new_requests() {
    // Bug 1 of njs commit 9be1165: `closed` is set in close() but never reset
    // in open(), so any request after a close()+open() cycle was rejected
    // with "Master closed". The fix is `closed.store(false, Ordering::...)`
    // at the top of open().
    let (slave, server, _hr) = create_slow_slave(Duration::from_millis(5)).await;
    let master = create_master(
        &server,
        ModbusMasterOptions {
            timeout_ms: 1000,
            concurrent: false,
        },
    )
    .await;

    let first = master
        .read_holding_registers(UNIT, 0, 1, None)
        .await
        .expect("first read should not error")
        .expect("first read should not be None");
    assert_eq!(first, vec![0u16], "pre-close read works as a sanity check");

    master.close().await.expect("close should not error");
    master.open().await.expect("reopen should succeed");

    let second = master.read_holding_registers(UNIT, 7, 1, None).await;
    match second {
        Ok(Some(v)) => assert_eq!(
            v,
            vec![7u16],
            "post-reopen read should return its own value"
        ),
        other => panic!(
            "post-reopen read must succeed (not reject with \"Master closed\"), got: {other:?}"
        ),
    }

    master.destroy().await;
    slave.destroy().await;
}
