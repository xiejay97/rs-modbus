//! Mirrors `njs-modbus/test/udp-multi-client.test.ts`. Covers UDP server-mode
//! per-rinfo isolation (each unique remote → distinct PhysicalConnection),
//! idle eviction, and client-mode source filtering. Without per-rinfo
//! framing, RTU/ASCII per-connection buffers would interleave across
//! senders and CRC/LRC checks would fail under any multi-client load.

use rs_modbus::layers::physical::UdpPhysicalLayerOptions;
use rs_modbus::layers::physical::{ConnectionId, PhysicalLayer, UdpPhysicalLayer};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Collect data events into a shared vec, returning the JoinHandle so the
/// task can be aborted to stop accumulating. Each event records the
/// connection id and the first byte.
#[allow(clippy::type_complexity)]
fn spawn_data_collector(
    phy: &UdpPhysicalLayer,
) -> (
    Arc<Mutex<Vec<(ConnectionId, u8)>>>,
    tokio::task::JoinHandle<()>,
) {
    let events: Arc<Mutex<Vec<(ConnectionId, u8)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut rx = phy.subscribe_data();
    let events_for_task = Arc::clone(&events);
    let task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Some(b) = event.data.first() {
                        events_for_task.lock().await.push((event.connection, *b));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    (events, task)
}

fn spawn_connection_close_collector(
    phy: &UdpPhysicalLayer,
) -> (Arc<Mutex<Vec<ConnectionId>>>, tokio::task::JoinHandle<()>) {
    let closes: Arc<Mutex<Vec<ConnectionId>>> = Arc::new(Mutex::new(Vec::new()));
    let mut rx = phy.subscribe_connection_close();
    let closes_for_task = Arc::clone(&closes);
    let task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(id) => closes_for_task.lock().await.push(id),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    (closes, task)
}

async fn wait_until_events(
    events: &Arc<Mutex<Vec<(ConnectionId, u8)>>>,
    target: usize,
    timeout: Duration,
) {
    let start = std::time::Instant::now();
    while events.lock().await.len() < target {
        if start.elapsed() >= timeout {
            return;
        }
        sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_until_closes(
    closes: &Arc<Mutex<Vec<ConnectionId>>>,
    target: usize,
    timeout: Duration,
) {
    let start = std::time::Instant::now();
    while closes.lock().await.len() < target {
        if start.elapsed() >= timeout {
            return;
        }
        sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn per_rinfo_distinct_connection_ids() {
    let phy = UdpPhysicalLayer::new_server();
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    let (events, _task) = spawn_data_collector(&phy);
    phy.open(None).await.unwrap();
    let addr = phy.local_addr().await.unwrap();

    let s1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let s2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let s3 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    s1.send_to(&[0x11], &addr).await.unwrap();
    s2.send_to(&[0x22], &addr).await.unwrap();
    s3.send_to(&[0x33], &addr).await.unwrap();

    wait_until_events(&events, 3, Duration::from_secs(2)).await;
    let snap = events.lock().await.clone();
    assert_eq!(snap.len(), 3, "expected 3 data events");
    let ids: HashSet<ConnectionId> = snap.iter().map(|(id, _)| Arc::clone(id)).collect();
    assert_eq!(ids.len(), 3, "three distinct connection ids");

    phy.destroy().await;
}

#[tokio::test]
async fn same_rinfo_keeps_stable_connection_id() {
    let phy = UdpPhysicalLayer::new_server();
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    let (events, _task) = spawn_data_collector(&phy);
    phy.open(None).await.unwrap();
    let addr = phy.local_addr().await.unwrap();

    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for b in [0xa1u8, 0xa2, 0xa3] {
        s.send_to(&[b], &addr).await.unwrap();
    }

    wait_until_events(&events, 3, Duration::from_secs(2)).await;
    let snap = events.lock().await.clone();
    assert_eq!(snap.len(), 3);
    let ids: HashSet<ConnectionId> = snap.iter().map(|(id, _)| Arc::clone(id)).collect();
    assert_eq!(
        ids.len(),
        1,
        "same rinfo → one stable connection id, got {} distinct",
        ids.len()
    );

    phy.destroy().await;
}

#[tokio::test]
async fn idle_rinfo_evicted_after_timeout() {
    let phy = UdpPhysicalLayer::new_server_with_options(UdpPhysicalLayerOptions {
        idle_timeout_ms: 60,
    });
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    let (events, _task) = spawn_data_collector(&phy);
    let (closes, _close_task) = spawn_connection_close_collector(&phy);
    phy.open(None).await.unwrap();
    let addr = phy.local_addr().await.unwrap();

    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    s.send_to(&[0xc0], &addr).await.unwrap();
    wait_until_events(&events, 1, Duration::from_secs(2)).await;
    let observed_id = events.lock().await[0].0.clone();

    // Before idle expiry: no eviction yet.
    sleep(Duration::from_millis(20)).await;
    assert_eq!(closes.lock().await.len(), 0, "should not be evicted yet");

    // After idle expiry: evicted exactly once with the same id.
    wait_until_closes(&closes, 1, Duration::from_secs(2)).await;
    let snap = closes.lock().await.clone();
    assert_eq!(snap.len(), 1, "evicted exactly once");
    assert_eq!(
        &*snap[0], &*observed_id,
        "evicted id matches the observed id"
    );

    phy.destroy().await;
}

#[tokio::test]
async fn idle_timeout_zero_disables_eviction() {
    let phy =
        UdpPhysicalLayer::new_server_with_options(UdpPhysicalLayerOptions { idle_timeout_ms: 0 });
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    let (events, _task) = spawn_data_collector(&phy);
    let (closes, _close_task) = spawn_connection_close_collector(&phy);
    phy.open(None).await.unwrap();
    let addr = phy.local_addr().await.unwrap();

    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    s.send_to(&[0xd0], &addr).await.unwrap();
    wait_until_events(&events, 1, Duration::from_secs(2)).await;

    sleep(Duration::from_millis(120)).await;
    assert_eq!(
        closes.lock().await.len(),
        0,
        "idle_timeout_ms=0 must disable eviction"
    );

    phy.destroy().await;
}

#[tokio::test]
async fn close_emits_connection_close_per_active_rinfo() {
    let phy =
        UdpPhysicalLayer::new_server_with_options(UdpPhysicalLayerOptions { idle_timeout_ms: 0 });
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    let (events, _task) = spawn_data_collector(&phy);
    let (closes, _close_task) = spawn_connection_close_collector(&phy);
    phy.open(None).await.unwrap();
    let addr = phy.local_addr().await.unwrap();

    let sa = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let sb = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sa.send_to(&[0x01], &addr).await.unwrap();
    sb.send_to(&[0x02], &addr).await.unwrap();
    wait_until_events(&events, 2, Duration::from_secs(2)).await;

    phy.close().await.unwrap();
    wait_until_closes(&closes, 2, Duration::from_secs(2)).await;

    let snap = closes.lock().await.clone();
    assert_eq!(
        snap.len(),
        2,
        "one connection_close per active rinfo, got {}",
        snap.len()
    );
    let ids: HashSet<ConnectionId> = snap.into_iter().collect();
    assert_eq!(ids.len(), 2, "distinct ids");

    phy.destroy().await;
}

// ===== Client mode: filter datagrams from unexpected senders =====

#[tokio::test]
async fn client_mode_drops_datagrams_from_unexpected_senders() {
    // A real "peer" on a well-known address; our client points at it.
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer.local_addr().unwrap();

    let phy = UdpPhysicalLayer::new_client(peer_addr.to_string());
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    let (events, _task) = spawn_data_collector(&phy);
    phy.open(None).await.unwrap();

    // Push one packet so the peer learns our ephemeral source addr.
    phy.write(&[0xff]).await.unwrap();
    let mut buf = [0u8; 16];
    let (_, client_addr) = peer.recv_from(&mut buf).await.unwrap();

    // Legit: peer replies → expected
    peer.send_to(&[0x55], client_addr).await.unwrap();

    // Spoof: a third party sends to our client port → should be dropped
    let intruder = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    intruder.send_to(&[0xee], client_addr).await.unwrap();

    sleep(Duration::from_millis(80)).await;
    let snap = events.lock().await.clone();
    assert_eq!(
        snap.len(),
        1,
        "only the expected peer datagram should be observed, got {}",
        snap.len()
    );
    assert_eq!(snap[0].1, 0x55, "byte from intruder must not leak through");

    phy.destroy().await;
}
