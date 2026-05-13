//! Mirrors `njs-modbus/test/ascii-hex-validation.test.ts`. Covers ASCII
//! reception-time hex validation:
//!
//! - Default (strict) accepts only uppercase `0-9` and `A-F`.
//! - `lenient_hex: true` additionally accepts `a-f` for legacy peers.
//! - Both modes reject other characters (`G`, `Z`, `!`, …) and emit a
//!   `framing-error` instead of silently letting bad bytes leak to LRC.

use rs_modbus::error::ModbusError;
use rs_modbus::layers::application::{
    ApplicationLayer, AsciiApplicationLayer, AsciiApplicationLayerOptions,
};
use rs_modbus::layers::physical::{PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::utils::lrc;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const UNIT: u8 = 1;

fn hex_nibble_upper(n: u8) -> u8 {
    match n {
        0..=9 => b'0' + n,
        10..=15 => b'A' + (n - 10),
        _ => unreachable!(),
    }
}

fn build_ascii_frame(unit: u8, fc: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![unit, fc];
    bytes.extend_from_slice(payload);
    bytes.push(lrc(&bytes));
    let mut out = Vec::with_capacity(1 + bytes.len() * 2 + 2);
    out.push(b':');
    for b in &bytes {
        out.push(hex_nibble_upper(b >> 4));
        out.push(hex_nibble_upper(b & 0x0f));
    }
    out.extend_from_slice(b"\r\n");
    out
}

/// Lowercase the hex digits between `:` and `\r`; leave framing characters alone.
fn to_lowercase_hex(frame: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(frame.len());
    let mut inside = false;
    for &b in frame {
        match b {
            b':' => {
                out.push(b);
                inside = true;
            }
            b'\r' | b'\n' => {
                out.push(b);
                inside = false;
            }
            b'A'..=b'F' if inside => out.push(b - b'A' + b'a'),
            _ => out.push(b),
        }
    }
    out
}

async fn setup_strict() -> (
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

async fn setup_lenient() -> (
    Arc<TcpServerPhysicalLayer>,
    Arc<AsciiApplicationLayer>,
    Arc<TcpClientPhysicalLayer>,
) {
    let server = TcpServerPhysicalLayer::new();
    server.set_addr("127.0.0.1:0".to_string()).await;
    server.open().await.unwrap();
    let addr = server.get_addr().await.unwrap();
    let application = AsciiApplicationLayer::with_options(
        server.clone(),
        AsciiApplicationLayerOptions { lenient_hex: true },
    );
    sleep(Duration::from_millis(30)).await;
    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;
    (server, application, client)
}

// ===== Strict mode (default) =====

#[tokio::test]
async fn strict_rejects_lowercase_hex_frame() {
    let (server, app, client) = setup_strict().await;
    let mut err_rx = app.subscribe_framing_error();
    let mut framing_rx = app.subscribe_framing();

    let upper = build_ascii_frame(UNIT, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let lower = to_lowercase_hex(&upper);
    assert_ne!(upper, lower, "lowering should actually change bytes");

    client.write(&lower).await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(2), err_rx.recv())
        .await
        .expect("framing-error within 2s")
        .expect("error channel open");
    assert!(
        matches!(err, ModbusError::InvalidHex),
        "expected InvalidHex hex-validation error, got {err:?}"
    );

    // No valid framing should have been emitted.
    let timeout = tokio::time::timeout(Duration::from_millis(120), framing_rx.recv()).await;
    assert!(
        timeout.is_err(),
        "expected no framing event for lowercase frame"
    );

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn strict_rejects_non_hex_garbage() {
    let (server, app, client) = setup_strict().await;
    let mut err_rx = app.subscribe_framing_error();

    client.write(b":01GZ00AA\r\n").await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(2), err_rx.recv())
        .await
        .expect("framing-error within 2s")
        .expect("error channel open");
    assert!(
        matches!(err, ModbusError::InvalidHex),
        "expected InvalidHex hex-validation error, got {err:?}"
    );

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn strict_accepts_uppercase_valid_frame() {
    let (server, app, client) = setup_strict().await;
    let mut err_rx = app.subscribe_framing_error();
    let mut framing_rx = app.subscribe_framing();

    let upper = build_ascii_frame(UNIT, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    client.write(&upper).await.unwrap();

    let f = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .expect("framing within 2s")
        .expect("channel open");
    assert_eq!(f.adu.unit, UNIT);
    assert_eq!(f.adu.fc, 0x03);

    // No framing-error should have fired.
    let err = tokio::time::timeout(Duration::from_millis(120), err_rx.recv()).await;
    assert!(err.is_err(), "no framing-error expected, got {:?}", err);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn strict_recovers_after_bad_frame() {
    let (server, app, client) = setup_strict().await;
    let mut framing_rx = app.subscribe_framing();

    // First: garbage that must be dropped.
    client.write(b":01GZ00AA\r\n").await.unwrap();
    sleep(Duration::from_millis(60)).await;

    // Then: a perfectly valid uppercase frame.
    let upper = build_ascii_frame(UNIT, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    client.write(&upper).await.unwrap();

    let f = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .expect("recovery framing within 2s")
        .expect("channel open");
    assert_eq!(f.adu.unit, UNIT);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

// ===== Lenient mode (opt-in) =====

#[tokio::test]
async fn lenient_accepts_lowercase_valid_frame() {
    let (server, app, client) = setup_lenient().await;
    let mut err_rx = app.subscribe_framing_error();
    let mut framing_rx = app.subscribe_framing();

    let upper = build_ascii_frame(UNIT, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let lower = to_lowercase_hex(&upper);
    client.write(&lower).await.unwrap();

    let f = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .expect("lenient framing within 2s")
        .expect("channel open");
    assert_eq!(f.adu.unit, UNIT);
    assert_eq!(f.adu.fc, 0x03);

    let err = tokio::time::timeout(Duration::from_millis(120), err_rx.recv()).await;
    assert!(err.is_err(), "no framing-error expected, got {:?}", err);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn lenient_still_rejects_non_hex_garbage() {
    let (server, app, client) = setup_lenient().await;
    let mut err_rx = app.subscribe_framing_error();

    client.write(b":01GZ00aa\r\n").await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(2), err_rx.recv())
        .await
        .expect("framing-error within 2s")
        .expect("error channel open");
    assert!(
        matches!(err, ModbusError::InvalidHex),
        "expected InvalidHex hex-validation error in lenient mode too, got {err:?}"
    );

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}
