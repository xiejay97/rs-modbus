//! RTU Serial timing tests — t3.5 strict mode + t1.5 inter-character timeout.
//! TDD: write tests first, then implement.

use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::{
    ApplicationLayer, ApplicationRole, FrameInterval, RtuApplicationLayer,
    RtuApplicationLayerOptions,
};
use rs_modbus::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use rs_modbus::utils::crc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout};

/// Fake serial physical layer that returns `PhysicalLayerType::Serial` so
/// the RTU layer arms t3.5 / t1.5 timers.
struct FakeSerialPhysicalLayer {
    data_tx: broadcast::Sender<DataEvent>,
    connection_close_tx: broadcast::Sender<ConnectionId>,
    close_tx: broadcast::Sender<()>,
}

impl FakeSerialPhysicalLayer {
    fn new() -> Arc<Self> {
        let (data_tx, _) = broadcast::channel(16);
        let (connection_close_tx, _) = broadcast::channel(16);
        let (close_tx, _) = broadcast::channel(16);
        Arc::new(Self {
            data_tx,
            connection_close_tx,
            close_tx,
        })
    }

    fn inject(&self,
        data: Vec<u8>,
        connection: ConnectionId,
    ) {
        let response: ResponseFn = Arc::new(|_| Box::pin(async { Ok(()) }));
        let _ = self.data_tx.send(DataEvent {
            data,
            response,
            connection,
        });
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for FakeSerialPhysicalLayer {
    fn layer_type(&self) -> PhysicalLayerType {
        PhysicalLayerType::Serial
    }

    async fn open(&self) -> Result<(), ModbusError> {
        Ok(())
    }

    async fn write(&self, _data: &[u8]) -> Result<(), ModbusError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), ModbusError> {
        Ok(())
    }

    async fn destroy(&self) {}

    fn is_open(&self) -> bool {
        true
    }

    fn is_destroyed(&self) -> bool {
        false
    }

    fn subscribe_data(&self) -> broadcast::Receiver<DataEvent> {
        self.data_tx.subscribe()
    }

    fn subscribe_write(&self) -> broadcast::Receiver<Vec<u8>> {
        let (_, rx) = broadcast::channel(1);
        rx
    }

    fn subscribe_error(&self) -> broadcast::Receiver<ModbusError> {
        let (_, rx) = broadcast::channel(1);
        rx
    }

    fn subscribe_connection_close(&self) -> broadcast::Receiver<ConnectionId> {
        self.connection_close_tx.subscribe()
    }

    fn subscribe_close(&self) -> broadcast::Receiver<()> {
        self.close_tx.subscribe()
    }
}

/// Build an RTU request: `[unit, fc, ...payload, crc_lo, crc_hi]`.
fn rtu_frame(unit: u8, fc: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + payload.len() + 2);
    buf.push(unit);
    buf.push(fc);
    buf.extend_from_slice(payload);
    let c = crc(&buf);
    buf.extend_from_slice(&c.to_le_bytes());
    buf
}

// ===== t3.5 strict mode =====

#[tokio::test]
async fn strict_t35_emits_framing_error_for_incomplete_frame() {
    let physical = FakeSerialPhysicalLayer::new();
    let app = RtuApplicationLayer::new(
        physical.clone(),
        RtuApplicationLayerOptions {
            interval_between_frames: Some(FrameInterval::Ms(50)),
            ..Default::default()
        },
    );
    app.set_role(ApplicationRole::Slave).unwrap();

    let mut framing_rx = app.subscribe_framing();
    let mut error_rx = app.subscribe_framing_error();

    // Inject only 4 bytes of an 8-byte frame.
    let frame = rtu_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    physical.inject(frame[..4].to_vec(), Arc::from("fake-conn"));

    // Wait for t3.5 (50ms) + margin.
    let err = timeout(Duration::from_millis(200), error_rx.recv())
        .await
        .expect("error within 200ms")
        .expect("channel open");
    assert!(
        matches!(err, ModbusError::IncompleteFrame),
        "expected IncompleteFrame, got {:?}",
        err
    );

    // No frame should have been delivered.
    assert!(
        timeout(Duration::from_millis(50), framing_rx.recv())
            .await
            .is_err(),
        "no framing expected"
    );

    app.destroy().await;
}

#[tokio::test]
async fn strict_t35_emits_framing_error_on_crc_mismatch_and_drops_buffer() {
    let physical = FakeSerialPhysicalLayer::new();
    let app = RtuApplicationLayer::new(
        physical.clone(),
        RtuApplicationLayerOptions {
            interval_between_frames: Some(FrameInterval::Ms(50)),
            ..Default::default()
        },
    );
    app.set_role(ApplicationRole::Slave).unwrap();

    let mut framing_rx = app.subscribe_framing();
    let mut error_rx = app.subscribe_framing_error();

    let mut bad = rtu_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    // Tamper CRC: flip the last byte.
    let last = bad.len() - 1;
    bad[last] ^= 0xFF;

    physical.inject(bad, Arc::from("fake-conn"));

    let err = timeout(Duration::from_millis(200), error_rx.recv())
        .await
        .expect("error within 200ms")
        .expect("channel open");
    assert!(
        matches!(err, ModbusError::CrcCheckFailed),
        "expected CrcCheckFailed, got {:?}",
        err
    );

    assert!(
        timeout(Duration::from_millis(50), framing_rx.recv())
            .await
            .is_err(),
        "no framing expected"
    );

    app.destroy().await;
}

#[tokio::test]
async fn strict_t35_recover_after_crc_fail_next_valid_frame_delivers() {
    let physical = FakeSerialPhysicalLayer::new();
    let app = RtuApplicationLayer::new(
        physical.clone(),
        RtuApplicationLayerOptions {
            interval_between_frames: Some(FrameInterval::Ms(50)),
            ..Default::default()
        },
    );
    app.set_role(ApplicationRole::Slave).unwrap();

    let mut framing_rx = app.subscribe_framing();
    let mut error_rx = app.subscribe_framing_error();

    let mut bad = rtu_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    let last = bad.len() - 1;
    bad[last] ^= 0xFF;
    physical.inject(bad, Arc::from("fake-conn"));

    let err = timeout(Duration::from_millis(200), error_rx.recv())
        .await
        .expect("error within 200ms")
        .expect("channel open");
    assert!(matches!(err, ModbusError::CrcCheckFailed));

    // After t3.5 + margin, inject a valid frame.
    sleep(Duration::from_millis(100)).await;
    let good = rtu_frame(1, 0x04, &[0x00, 0x21, 0x00, 0x01]);
    physical.inject(good.clone(), Arc::from("fake-conn"));

    let f = timeout(Duration::from_millis(200), framing_rx.recv())
        .await
        .expect("framing within 200ms")
        .expect("channel open");
    assert_eq!(f.adu.fc, 0x04);

    app.destroy().await;
}

// ===== t1.5 inter-character timeout =====

#[tokio::test]
async fn t15_emits_framing_error_when_mid_frame_gap_exceeds_t15() {
    let physical = FakeSerialPhysicalLayer::new();
    let app = RtuApplicationLayer::new(
        physical.clone(),
        RtuApplicationLayerOptions {
            interval_between_frames: Some(FrameInterval::Ms(50)),
            inter_char_timeout: Some(FrameInterval::Ms(30)),
            ..Default::default()
        },
    );
    app.set_role(ApplicationRole::Slave).unwrap();

    let mut framing_rx = app.subscribe_framing();
    let mut error_rx = app.subscribe_framing_error();

    let frame = rtu_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    physical.inject(frame[..4].to_vec(), Arc::from("fake-conn"));

    // Wait longer than t1.5 (30ms) but less than t3.5 (50ms).
    sleep(Duration::from_millis(60)).await;

    // Inject rest of frame — should be too late; t1.5 already expired.
    physical.inject(frame[4..].to_vec(), Arc::from("fake-conn"));

    // Wait for t3.5 to flush.
    let err = timeout(Duration::from_millis(200), error_rx.recv())
        .await
        .expect("error within 200ms")
        .expect("channel open");
    assert!(
        matches!(err, ModbusError::T1_5Exceeded),
        "expected T1_5Exceeded, got {:?}",
        err
    );

    assert!(
        timeout(Duration::from_millis(50), framing_rx.recv())
            .await
            .is_err(),
        "no framing expected"
    );

    app.destroy().await;
}

#[tokio::test]
async fn t15_tolerates_gap_shorter_than_timeout() {
    let physical = FakeSerialPhysicalLayer::new();
    let app = RtuApplicationLayer::new(
        physical.clone(),
        RtuApplicationLayerOptions {
            interval_between_frames: Some(FrameInterval::Ms(50)),
            inter_char_timeout: Some(FrameInterval::Ms(30)),
            ..Default::default()
        },
    );
    app.set_role(ApplicationRole::Slave).unwrap();

    let mut framing_rx = app.subscribe_framing();
    let mut error_rx = app.subscribe_framing_error();

    let frame = rtu_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    physical.inject(frame[..4].to_vec(), Arc::from("fake-conn"));

    // Wait less than t1.5.
    sleep(Duration::from_millis(10)).await;

    physical.inject(frame[4..].to_vec(), Arc::from("fake-conn"));

    let f = timeout(Duration::from_millis(200), framing_rx.recv())
        .await
        .expect("framing within 200ms")
        .expect("channel open");
    assert_eq!(f.adu.fc, 0x03);

    assert!(
        timeout(Duration::from_millis(50), error_rx.recv())
            .await
            .is_err(),
        "no error expected"
    );

    app.destroy().await;
}

#[tokio::test]
async fn t15_disabled_no_error_on_mid_frame_gap() {
    let physical = FakeSerialPhysicalLayer::new();
    let app = RtuApplicationLayer::new(
        physical.clone(),
        RtuApplicationLayerOptions {
            interval_between_frames: Some(FrameInterval::Ms(50)),
            ..Default::default()
        },
    );
    app.set_role(ApplicationRole::Slave).unwrap();

    let mut framing_rx = app.subscribe_framing();
    let mut error_rx = app.subscribe_framing_error();

    let frame = rtu_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    physical.inject(frame[..4].to_vec(), Arc::from("fake-conn"));

    // Gap shorter than t3.5 (50ms) — frame should still be delivered.
    sleep(Duration::from_millis(30)).await;

    physical.inject(frame[4..].to_vec(), Arc::from("fake-conn"));

    let f = timeout(Duration::from_millis(200), framing_rx.recv())
        .await
        .expect("framing within 200ms")
        .expect("channel open");
    assert_eq!(f.adu.fc, 0x03);

    assert!(
        timeout(Duration::from_millis(50), error_rx.recv())
            .await
            .is_err(),
        "no error expected when t1.5 disabled"
    );

    app.destroy().await;
}
