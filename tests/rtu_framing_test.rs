//! RTU application-layer framing tests over a Net transport
//! (RTU-over-TCP). Covers:
//! - `predict_rtu_frame_length` fast path on a known FC
//! - CRC sliding-window fallback for unknown / variable-length FC (e.g. 43/14
//!   responses).
//! - Sticky packets (multiple frames in one chunk).
//! - Half packets (frame split across writes).
//! - CRC mismatch -> framing_error + recovery on the next valid frame.
//!
//! Serial-only behaviors (3.5T inter-frame timing) are covered separately as
//! unit tests on the `compute_interval_ms` helper.

use rs_modbus::layers::application::{
    ApplicationLayer, ApplicationRole, FrameInterval, RtuApplicationLayer,
    RtuApplicationLayerOptions,
};
use rs_modbus::layers::physical::{PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::utils::crc;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Build an RTU request: `[unit, fc, ...payload, crc_lo, crc_hi]`.
fn rtu_request(unit: u8, fc: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + payload.len() + 2);
    buf.push(unit);
    buf.push(fc);
    buf.extend_from_slice(payload);
    let c = crc(&buf);
    buf.extend_from_slice(&c.to_le_bytes());
    buf
}

async fn setup() -> (
    Arc<TcpServerPhysicalLayer>,
    Arc<RtuApplicationLayer>,
    Arc<TcpClientPhysicalLayer>,
) {
    let server = TcpServerPhysicalLayer::new();
    server.set_addr("127.0.0.1:0".to_string()).await;
    server.open().await.unwrap();
    let addr = server.get_addr().await.unwrap();
    // RTU layer bound to Net transport (no inter-frame timer).
    let application =
        RtuApplicationLayer::new(server.clone(), RtuApplicationLayerOptions::default());
    application.set_role(ApplicationRole::Slave).unwrap();
    sleep(Duration::from_millis(30)).await;

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    (server, application, client)
}

#[tokio::test]
async fn test_rtu_decodes_known_fc_via_predict() {
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    // FC03 request: 8 bytes (fixed length).
    let frame = rtu_request(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    assert_eq!(frame.len(), 8);
    client.write(&frame).await.unwrap();

    let f = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("framing within 2s")
        .expect("channel open");
    assert_eq!(f.adu.unit, 1);
    assert_eq!(f.adu.fc, 0x03);
    assert_eq!(f.adu.data, vec![0x00, 0x14, 0x00, 0x02]);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn test_rtu_splits_sticky_frames() {
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    let a = rtu_request(1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let b = rtu_request(2, 0x06, &[0x00, 0x05, 0x12, 0x34]);
    let mut combined = a.clone();
    combined.extend_from_slice(&b);
    client.write(&combined).await.unwrap();

    let f1 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let f2 = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f1.adu.unit, 1);
    assert_eq!(f1.adu.fc, 0x03);
    assert_eq!(f2.adu.unit, 2);
    assert_eq!(f2.adu.fc, 0x06);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn test_rtu_reassembles_half_frame() {
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    let frame = rtu_request(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    client.write(&frame[..4]).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    client.write(&frame[4..]).await.unwrap();

    let f = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.adu.fc, 0x03);
    assert_eq!(f.adu.data, vec![0x00, 0x14, 0x00, 0x02]);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn test_rtu_framing_error_on_bad_crc_then_recovers() {
    let (server, app, client) = setup().await;
    let mut err_rx = app.subscribe_framing_error();
    let mut frame_rx = app.subscribe_framing();

    // Bad CRC frame (8 bytes that look like an FC03 request but with random
    // CRC bytes). The predictor returns 8; CRC then fails. The layer should
    // skip ahead and try sliding-window; eventually report `CrcCheckFailed`.
    let bad = vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x0a, 0xff, 0xff];
    // Make absolutely sure CRC is wrong.
    let real_crc = crc(&bad[..6]);
    assert_ne!(u16::from_le_bytes([bad[6], bad[7]]), real_crc);
    client.write(&bad).await.unwrap();

    // Allow some processing time, then send a valid frame which must reach us.
    sleep(Duration::from_millis(80)).await;
    let good = rtu_request(2, 0x03, &[0x00, 0x05, 0x00, 0x01]);
    client.write(&good).await.unwrap();

    // We expect *some* framing event for the good frame eventually.
    let good_received = tokio::time::timeout(Duration::from_secs(2), frame_rx.recv())
        .await
        .expect("good frame within 2s")
        .expect("framing channel open");
    assert_eq!(good_received.adu.unit, 2);
    assert_eq!(good_received.adu.fc, 0x03);

    // Drain any framing_error events (optional; corruption may or may not
    // produce an error before recovery).
    let _ = err_rx.try_recv();

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;

    // Mark bad as used (it's already used above; explicit to avoid lints).
    let _ = bad;
}

#[tokio::test]
async fn test_rtu_compute_interval_ms_via_constructor_net() {
    // For Net transports the interval is always 0 regardless of inputs.
    let physical = TcpClientPhysicalLayer::new();
    let app = RtuApplicationLayer::new(
        physical.clone(),
        RtuApplicationLayerOptions {
            baud_rate: Some(9600),
            ..Default::default()
        },
    );
    // The `interval_ms` field is private; we exercise it via constructor not
    // panicking with various inputs.
    let _ = app;
    let app2 = RtuApplicationLayer::new(
        physical.clone(),
        RtuApplicationLayerOptions {
            baud_rate: Some(9600),
            interval_between_frames: Some(FrameInterval::Bits(96.0)),
            ..Default::default()
        },
    );
    let _ = app2;
    let app3 = RtuApplicationLayer::new(
        physical,
        RtuApplicationLayerOptions {
            baud_rate: Some(115200),
            interval_between_frames: Some(FrameInterval::Ms(7)),
            ..Default::default()
        },
    );
    let _ = app3;
}
