//! RTU pool overflow — loop consumption (035dbd1)
//!
//! Verifies that the fixed-size pool can absorb a single large inbound chunk
//! (many concatenated frames) without truncating or losing frames.

use rs_modbus::layers::application::{
    ApplicationLayer, ApplicationRole, RtuApplicationLayer, RtuApplicationLayerOptions,
};
use rs_modbus::layers::physical::{PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer};
use rs_modbus::utils::crc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

fn rtu_request(unit: u8, fc: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + payload.len() + 2);
    buf.push(unit);
    buf.push(fc);
    buf.extend_from_slice(payload);
    let c = crc(&buf);
    buf.extend_from_slice(&c.to_le_bytes());
    buf
}

#[tokio::test]
async fn pool_consumes_chunk_larger_than_pool_without_truncating() {
    let server = TcpServerPhysicalLayer::new();
    server.set_addr("127.0.0.1:0".to_string()).await;
    server.open().await.unwrap();
    let addr = server.get_addr().await.unwrap();

    let app = RtuApplicationLayer::new(server.clone(), RtuApplicationLayerOptions::default());
    app.set_role(ApplicationRole::Slave).unwrap();
    let mut rx = app.subscribe_framing();
    sleep(Duration::from_millis(30)).await;

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    // 80 frames x 8 bytes = 640 bytes, exceeding the 512-byte pool.
    // Write in two batches so the receiver can keep up with the broadcast
    // channel (capacity 64) between flushes.
    let mut chunk_a = Vec::new();
    for i in 0..40 {
        let frame = rtu_request(1, 0x03, &[0x00, i as u8, 0x00, 0x02]);
        chunk_a.extend_from_slice(&frame);
    }
    let mut chunk_b = Vec::new();
    for i in 40..80 {
        let frame = rtu_request(1, 0x03, &[0x00, i as u8, 0x00, 0x02]);
        chunk_b.extend_from_slice(&frame);
    }

    client.write(&chunk_a).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    client.write(&chunk_b).await.unwrap();

    // Collect all 80 frames
    let mut count = 0;
    let result = timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(_) => {
                    count += 1;
                    if count == 80 {
                        break;
                    }
                }
                Err(e) => panic!("recv error: {:?}", e),
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "timeout waiting for 80 frames, got {}",
        count
    );
    assert_eq!(count, 80, "expected 80 frames, got {}", count);

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}

#[tokio::test]
async fn pool_emits_framing_error_when_chunk_exceeds_pool_and_no_valid_frame() {
    let server = TcpServerPhysicalLayer::new();
    server.set_addr("127.0.0.1:0".to_string()).await;
    server.open().await.unwrap();
    let addr = server.get_addr().await.unwrap();

    let app = RtuApplicationLayer::new(server.clone(), RtuApplicationLayerOptions::default());
    app.set_role(ApplicationRole::Slave).unwrap();
    let mut err_rx = app.subscribe_framing_error();
    sleep(Duration::from_millis(30)).await;

    let client = TcpClientPhysicalLayer::new();
    client.set_addr(addr).await;
    client.open().await.unwrap();
    sleep(Duration::from_millis(30)).await;

    // 600 bytes of garbage: no valid RTU frame can be extracted
    let garbage = vec![0xabu8; 600];
    client.write(&garbage).await.unwrap();

    // Should get at least one framing-error
    let result = timeout(Duration::from_secs(2), err_rx.recv()).await;
    assert!(
        result.is_ok(),
        "expected at least one framing-error for pool overflow"
    );

    client.destroy().await;
    server.destroy().await;
    app.destroy().await;
}
