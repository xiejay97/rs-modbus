//! TCP application-layer fragmentation/reassembly tests.
//!
//! Mirrors `njs-modbus/test/tcp-fragmentation.test.ts` scenarios: half packet,
//! sticky packet, byte-by-byte, multi-client isolation, invalid protocol id,
//! and absurd length-field handling.

use rs_modbus::layers::application::{ApplicationLayer, TcpApplicationLayer};
use rs_modbus::layers::physical::{PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn mbap_request(transaction: u16, unit: u8, fc: u8, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + data.len());
    buf.extend_from_slice(&transaction.to_be_bytes());
    buf.extend_from_slice(&[0, 0]);
    let len = (data.len() as u16) + 2;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.push(unit);
    buf.push(fc);
    buf.extend_from_slice(data);
    buf
}

async fn setup_server() -> (
    Arc<TcpServerPhysicalLayer>,
    Arc<TcpApplicationLayer>,
    String,
) {
    let physical = TcpServerPhysicalLayer::new();
    physical.set_addr("127.0.0.1:0".to_string()).await;
    physical.open().await.unwrap();
    let addr = physical.get_addr().await.unwrap();
    let application = TcpApplicationLayer::new(physical.clone());
    sleep(Duration::from_millis(30)).await;
    (physical, application, addr)
}

#[tokio::test]
async fn test_reassembles_half_packet() {
    let (server_physical, application, addr) = setup_server().await;
    let mut framing_rx = application.subscribe_framing();

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    let request = mbap_request(1, 1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    assert_eq!(request.len(), 12);
    client.write(&request[..6]).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    client.write(&request[6..]).await.unwrap();

    let frame = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .expect("frame within 2s")
        .expect("framing channel open");
    assert_eq!(frame.adu.transaction, Some(1));
    assert_eq!(frame.adu.unit, 1);
    assert_eq!(frame.adu.fc, 0x03);
    assert_eq!(frame.adu.data, vec![0x00, 0x14, 0x00, 0x02]);

    client.destroy().await;
    server_physical.destroy().await;
    application.destroy().await;
}

#[tokio::test]
async fn test_splits_sticky_packet() {
    let (server_physical, application, addr) = setup_server().await;
    let mut framing_rx = application.subscribe_framing();

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    let r1 = mbap_request(10, 1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let r2 = mbap_request(11, 1, 0x03, &[0x00, 0x15, 0x00, 0x01]);
    let mut combined = r1.clone();
    combined.extend_from_slice(&r2);
    client.write(&combined).await.unwrap();

    let first = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .expect("first frame within 2s")
        .expect("framing channel open");
    let second = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .expect("second frame within 2s")
        .expect("framing channel open");

    assert_eq!(first.adu.transaction, Some(10));
    assert_eq!(second.adu.transaction, Some(11));

    client.destroy().await;
    server_physical.destroy().await;
    application.destroy().await;
}

#[tokio::test]
async fn test_handles_mixed_full_plus_partial() {
    let (server_physical, application, addr) = setup_server().await;
    let mut framing_rx = application.subscribe_framing();

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    let r1 = mbap_request(20, 1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let r2 = mbap_request(21, 1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let mut chunk1 = r1.clone();
    chunk1.extend_from_slice(&r2[..4]);
    client.write(&chunk1).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    client.write(&r2[4..]).await.unwrap();

    let first = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.adu.transaction, Some(20));
    assert_eq!(second.adu.transaction, Some(21));

    client.destroy().await;
    server_physical.destroy().await;
    application.destroy().await;
}

#[tokio::test]
async fn test_handles_byte_by_byte_delivery() {
    let (server_physical, application, addr) = setup_server().await;
    let mut framing_rx = application.subscribe_framing();

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    let request = mbap_request(30, 1, 0x03, &[0x00, 0x14, 0x00, 0x02]);
    for chunk in request.chunks(1) {
        client.write(chunk).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let frame = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.adu.transaction, Some(30));
    assert_eq!(frame.adu.data, vec![0x00, 0x14, 0x00, 0x02]);

    client.destroy().await;
    server_physical.destroy().await;
    application.destroy().await;
}

#[tokio::test]
async fn test_isolates_multiple_clients_with_interleaved_halves() {
    let (server_physical, application, addr) = setup_server().await;
    let mut framing_rx = application.subscribe_framing();

    let client_a = TcpClientPhysicalLayer::new();
    client_a.set_addr(addr.clone()).await;
    client_a.open().await.unwrap();
    let client_b = TcpClientPhysicalLayer::new();
    client_b.set_addr(addr).await;
    client_b.open().await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let r_a = mbap_request(100, 1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    let r_b = mbap_request(200, 1, 0x03, &[0x00, 0x15, 0x00, 0x01]);

    client_a.write(&r_a[..6]).await.unwrap();
    sleep(Duration::from_millis(20)).await;
    client_b.write(&r_b[..6]).await.unwrap();
    sleep(Duration::from_millis(20)).await;
    client_a.write(&r_a[6..]).await.unwrap();
    sleep(Duration::from_millis(20)).await;
    client_b.write(&r_b[6..]).await.unwrap();

    let mut seen_a = false;
    let mut seen_b = false;
    for _ in 0..2 {
        let frame = tokio::time::timeout(Duration::from_secs(2), framing_rx.recv())
            .await
            .unwrap()
            .unwrap();
        match frame.adu.transaction {
            Some(100) => seen_a = true,
            Some(200) => seen_b = true,
            other => panic!("unexpected transaction {:?}", other),
        }
    }
    assert!(seen_a && seen_b, "both transactions should be received");

    client_a.destroy().await;
    client_b.destroy().await;
    server_physical.destroy().await;
    application.destroy().await;
}

#[tokio::test]
async fn test_rejects_invalid_protocol_id() {
    let (server_physical, application, addr) = setup_server().await;
    let mut err_rx = application.subscribe_framing_error();
    let mut framing_rx = application.subscribe_framing();

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    let mut bogus = mbap_request(300, 1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    bogus[2] = 0x12;
    bogus[3] = 0x34;
    client.write(&bogus).await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(2), err_rx.recv())
        .await
        .expect("framing_error within 2s")
        .expect("error channel open");
    assert!(matches!(err, rs_modbus::ModbusError::InvalidData));

    // After the bogus frame, a subsequent valid frame must NOT be silently
    // included as transaction 300.
    let good = mbap_request(301, 1, 0x03, &[0x00, 0x14, 0x00, 0x01]);
    client.write(&good).await.unwrap();
    // Either a valid framing comes (with transaction 301) or no framing
    // arrives (connection dropped). What MUST NOT happen is `Some(300)`.
    if let Ok(Ok(frame)) = tokio::time::timeout(Duration::from_millis(500), framing_rx.recv()).await
    {
        assert_eq!(frame.adu.transaction, Some(301));
    }

    client.destroy().await;
    server_physical.destroy().await;
    application.destroy().await;
}

#[tokio::test]
async fn test_caps_buffer_growth_on_absurd_length() {
    let (server_physical, application, addr) = setup_server().await;
    let mut err_rx = application.subscribe_framing_error();

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    // length field = 0xFFFF → frame would be 0x10005, way past MAX_TCP_FRAME.
    let mut header = vec![0u8; 8];
    header[0..2].copy_from_slice(&400u16.to_be_bytes());
    header[2..4].copy_from_slice(&[0, 0]);
    header[4..6].copy_from_slice(&0xffffu16.to_be_bytes());
    header[6] = 1;
    header[7] = 0x03;
    client.write(&header).await.unwrap();

    let err = tokio::time::timeout(Duration::from_secs(2), err_rx.recv())
        .await
        .expect("framing_error within 2s")
        .expect("error channel open");
    assert!(matches!(err, rs_modbus::ModbusError::InvalidData));

    client.destroy().await;
    server_physical.destroy().await;
    application.destroy().await;
}
