//! ASCII application-layer FSM tests. Drive the layer with raw bytes over a
//! TCP loopback and assert the right frames / errors come out.

use rs_modbus::layers::application::{ApplicationLayer, AsciiApplicationLayer};
use rs_modbus::layers::physical::{
    PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer,
};
use rs_modbus::utils::lrc;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Encode an ASCII MODBUS frame: `:HH HH ... HH \r\n` where each byte is two
/// uppercase hex chars. LRC byte is appended before encoding.
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
    server.open().await.unwrap();
    let addr = server.get_addr().await.unwrap();
    let application = AsciiApplicationLayer::new(server.clone());
    sleep(Duration::from_millis(30)).await;
    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;
    (server, application, client)
}

#[tokio::test]
async fn test_ascii_decodes_single_frame() {
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    let frame = ascii_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
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
async fn test_ascii_splits_sticky_frames() {
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
async fn test_ascii_reassembles_half_frame() {
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    let frame = ascii_frame(1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    let half = frame.len() / 2;
    client.write(&frame[..half]).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    client.write(&frame[half..]).await.unwrap();

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

#[tokio::test]
async fn test_ascii_lrc_failure_emits_framing_error() {
    let (server, app, client) = setup().await;
    let mut err_rx = app.subscribe_framing_error();

    // Same as a valid `:01030000000A...` frame but with the LRC byte
    // intentionally corrupted to `FF`.
    let bogus: &[u8] = b":01030000000AFF\r\n";
    client.write(bogus).await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(2), err_rx.recv())
        .await
        .expect("framing_error within 2s")
        .expect("error channel open");
    assert!(matches!(err, rs_modbus::ModbusError::LrcCheckFailed));

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn test_ascii_reception_reset_on_inner_colon() {
    // FSM behavior: while in `reception`, seeing another `:` should reset
    // the frame buffer (discard the partial garbage and start fresh).
    let (server, app, client) = setup().await;
    let mut rx = app.subscribe_framing();

    // Garbage bytes after a starting `:`, then another `:` followed by a
    // well-formed frame.
    let mut bytes = vec![b':', b'X', b'Y', b'Z'];
    let good = ascii_frame(1, 0x03, &[0x00, 0x00, 0x00, 0x01]);
    bytes.extend_from_slice(&good);
    client.write(&bytes).await.unwrap();

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
