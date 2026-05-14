//! ASCII over TCP fragmentation/reassembly tests.
//!
//! Mirrors `njs-modbus/test/ascii-tcp-fragmentation.test.ts` scenarios:
//! half-packet (split at hex digits and at CRLF), sticky packet,
//! multi-client interleaved halves, idle-reset after stray bytes,
//! and 512-byte payload overflow cap + recovery.

use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::{ApplicationLayer, AsciiApplicationLayer};
use rs_modbus::layers::physical::{PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::utils::lrc;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn ascii_frame(unit: u8, fc: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![unit, fc];
    bytes.extend_from_slice(payload);
    bytes.push(lrc(&bytes));
    let mut out = Vec::with_capacity(1 + bytes.len() * 2 + 2);
    out.push(b':');
    for b in &bytes {
        out.push(hex_nibble(b >> 4));
        out.push(hex_nibble(b & 0x0f));
    }
    out.extend_from_slice(b"\r\n");
    out
}

fn hex_nibble(n: u8) -> u8 {
    match n {
        0..=9 => b'0' + n,
        10..=15 => b'A' + (n - 10),
        _ => unreachable!(),
    }
}

async fn setup() -> (
    Arc<TcpServerPhysicalLayer>,
    Arc<AsciiApplicationLayer>,
    Arc<TcpClientPhysicalLayer>,
) {
    let server = TcpServerPhysicalLayer::new();
    server.set_addr("127.0.0.1:0".to_string()).await;
    server.open(None).await.unwrap();
    let addr = server.get_addr().await.unwrap();
    let application = AsciiApplicationLayer::new(server.clone());
    sleep(Duration::from_millis(30)).await;
    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open(None).await.unwrap();
    sleep(Duration::from_millis(30)).await;
    (server, application, client)
}

#[tokio::test]
async fn test_reassembles_half_packet_split_at_hex_and_crlf() {
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    let frame = ascii_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    // Split at arbitrary hex position (5 bytes in)
    client.write(&frame[..5]).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    // Second chunk up to but excluding final LF
    client.write(&frame[5..frame.len() - 1]).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    // Final LF byte
    client.write(&frame[frame.len() - 1..]).await.unwrap();

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
async fn test_splits_sticky_packet() {
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    let a = ascii_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let b = ascii_frame(2, 0x06, &[0x00, 0x05, 0x12, 0x34]);
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
async fn test_isolates_multiple_clients_with_interleaved_halves() {
    let (server, app, _client_a) = setup().await;
    let mut rx = app.subscribe_framing();

    // second client connected to same server
    let client_b = TcpClientPhysicalLayer::new();
    client_b.set_addr(server.get_addr().await.unwrap()).await;
    client_b.open(None).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let r_a = ascii_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let r_b = ascii_frame(1, 0x03, &[0x00, 0x15, 0x00, 0x01]);

    _client_a.write(&r_a[..6]).await.unwrap();
    sleep(Duration::from_millis(20)).await;
    client_b.write(&r_b[..6]).await.unwrap();
    sleep(Duration::from_millis(20)).await;
    _client_a.write(&r_a[6..]).await.unwrap();
    sleep(Duration::from_millis(20)).await;
    client_b.write(&r_b[6..]).await.unwrap();

    let mut seen_a = false;
    let mut seen_b = false;
    for _ in 0..2 {
        let f = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        // Distinguish by the data payload (address 0x14 vs 0x15)
        if f.adu.data.starts_with(&[0x00, 0x14]) {
            seen_a = true;
        } else if f.adu.data.starts_with(&[0x00, 0x15]) {
            seen_b = true;
        } else {
            panic!("unexpected payload start {:?}", &f.adu.data[..2]);
        }
    }
    assert!(seen_a && seen_b, "both frames should be received");

    _client_a.destroy().await;
    client_b.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn test_recovers_after_stray_bytes_before_colon() {
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    // Junk bytes without leading colon, then a valid frame
    let junk = b"hello world\r\n";
    let good = ascii_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let mut buf = junk.to_vec();
    buf.extend_from_slice(&good);
    client.write(&buf).await.unwrap();

    let f = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("framing within 2s")
        .expect("channel open");
    assert_eq!(f.adu.unit, 1);
    assert_eq!(f.adu.fc, 0x03);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn test_caps_payload_at_512_bytes_and_recovers() {
    let (server, app, client) = setup().await;
    let mut err_rx = app.subscribe_framing_error();
    let mut framing_rx = app.subscribe_framing();

    // ':' then > MAX_ASCII_PAYLOAD (512) hex chars without CR.
    // The FSM should emit an overflow error and return to idle.
    let mut overflow = Vec::new();
    overflow.push(b':');
    overflow.extend(std::iter::repeat(b'A').take(600));
    client.write(&overflow).await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(2), err_rx.recv())
        .await
        .expect("framing-error within 2s")
        .expect("error channel open");
    assert!(
        matches!(err, ModbusError::InvalidData),
        "expected InvalidData from overflow, got {:?}",
        err
    );

    // FSM must accept a follow-up valid frame.
    let good = ascii_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    client.write(&good).await.unwrap();

    let f = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .expect("recovery framing within 2s")
        .expect("channel open");
    assert_eq!(f.adu.unit, 1);
    assert_eq!(f.adu.fc, 0x03);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn test_handles_byte_by_byte_delivery() {
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    let frame = ascii_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    for chunk in frame.chunks(1) {
        client.write(chunk).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let f = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.adu.unit, 1);
    assert_eq!(f.adu.fc, 0x03);
    assert_eq!(f.adu.data, vec![0x00, 0x14, 0x00, 0x02]);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}
