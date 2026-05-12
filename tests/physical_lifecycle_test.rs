//! Mirrors `njs-modbus/test/physical-lifecycle.test.ts`.
//!
//! Audit items #2 (reopen broken), #14 (user-initiated `close` event
//! suppressed) and #15 (no double-open guard). One `mod`-style group per
//! `describe` block in the TypeScript suite.

use rs_modbus::error::ModbusError;
use rs_modbus::layers::physical::{
    PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer, UdpPhysicalLayer,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::oneshot;

/// Spawns an echo server on an ephemeral port and returns its addr plus a
/// shutdown handle. Dropping the handle terminates the accept loop after the
/// next pending operation completes.
async fn spawn_echo_server() -> (String, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((mut s, _)) => {
                            tokio::spawn(async move {
                                let mut buf = vec![0u8; 1024];
                                loop {
                                    match s.read(&mut buf).await {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => {
                                            if s.write_all(&buf[..n]).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });
    (addr, stop_tx)
}

// ===== TcpClientPhysicalLayer — lifecycle =====

#[tokio::test]
async fn tcp_client_reopens_after_close_and_exchanges_data_again() {
    let (addr, _stop) = spawn_echo_server().await;
    let phy = TcpClientPhysicalLayer::new();
    phy.set_addr(addr.clone()).await;
    let mut rx = phy.subscribe_data();

    // Session 1
    phy.open().await.unwrap();
    assert!(phy.is_open());
    phy.write(&[0x01, 0x02, 0x03]).await.unwrap();
    let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("data within 2s")
        .unwrap();
    assert_eq!(evt.data, vec![0x01, 0x02, 0x03]);
    phy.close().await.unwrap();
    assert!(!phy.is_open());

    // Session 2: same physical-layer instance, fresh socket under the hood.
    phy.open().await.unwrap();
    assert!(phy.is_open());
    phy.write(&[0x0a, 0x0b]).await.unwrap();
    let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("data within 2s")
        .unwrap();
    assert_eq!(evt.data, vec![0x0a, 0x0b]);
    phy.destroy().await;
}

#[tokio::test]
async fn tcp_client_emits_close_event_on_user_initiated_close() {
    let (addr, _stop) = spawn_echo_server().await;
    let phy = TcpClientPhysicalLayer::new();
    phy.set_addr(addr).await;
    let mut close_rx = phy.subscribe_close();
    phy.open().await.unwrap();
    phy.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), close_rx.recv())
        .await
        .expect("close event within 2s")
        .unwrap();
    phy.destroy().await;
}

#[tokio::test]
async fn tcp_client_rejects_double_open() {
    let (addr, _stop) = spawn_echo_server().await;
    let phy = TcpClientPhysicalLayer::new();
    phy.set_addr(addr).await;
    phy.open().await.unwrap();
    let result = phy.open().await;
    assert!(
        matches!(result, Err(ModbusError::PortAlreadyOpen)),
        "expected PortAlreadyOpen, got {:?}",
        result
    );
    phy.destroy().await;
}

#[tokio::test]
async fn tcp_client_rejects_open_and_write_after_destroy() {
    let (addr, _stop) = spawn_echo_server().await;
    let phy = TcpClientPhysicalLayer::new();
    phy.set_addr(addr).await;
    phy.open().await.unwrap();
    phy.destroy().await;
    assert!(matches!(phy.open().await, Err(ModbusError::PortDestroyed)));
    assert!(matches!(phy.write(&[0]).await, Err(ModbusError::PortNotOpen)));
}

// ===== TcpServerPhysicalLayer — lifecycle =====

#[tokio::test]
async fn tcp_server_reopens_after_close_and_accepts_new_client() {
    let phy = TcpServerPhysicalLayer::new();
    phy.set_addr("127.0.0.1:0".to_string()).await;
    let mut data_rx = phy.subscribe_data();

    // Session 1
    phy.open().await.unwrap();
    let addr1 = phy.get_addr().await.unwrap();
    let mut c1 = TcpStream::connect(&addr1).await.unwrap();
    c1.write_all(&[0x42]).await.unwrap();
    let evt = tokio::time::timeout(Duration::from_secs(2), data_rx.recv())
        .await
        .expect("data within 2s")
        .unwrap();
    assert_eq!(evt.data, vec![0x42]);
    drop(c1);
    phy.close().await.unwrap();
    assert!(!phy.is_open());

    // Session 2 — re-bind to a fresh ephemeral port (the previous
    // listener should have been released so a new bind can land).
    phy.set_addr("127.0.0.1:0".to_string()).await;
    phy.open().await.unwrap();
    let addr2 = phy.get_addr().await.unwrap();
    let mut c2 = TcpStream::connect(&addr2).await.unwrap();
    c2.write_all(&[0x99]).await.unwrap();
    let evt = tokio::time::timeout(Duration::from_secs(2), data_rx.recv())
        .await
        .expect("data within 2s")
        .unwrap();
    assert_eq!(evt.data, vec![0x99]);
    drop(c2);
    phy.destroy().await;
}

#[tokio::test]
async fn tcp_server_emits_close_event_on_user_initiated_close() {
    let phy = TcpServerPhysicalLayer::new();
    phy.set_addr("127.0.0.1:0".to_string()).await;
    let mut close_rx = phy.subscribe_close();
    phy.open().await.unwrap();
    phy.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), close_rx.recv())
        .await
        .expect("close event within 2s")
        .unwrap();
    phy.destroy().await;
}

#[tokio::test]
async fn tcp_server_emits_connection_close_for_active_clients_on_close() {
    let phy = TcpServerPhysicalLayer::new();
    phy.set_addr("127.0.0.1:0".to_string()).await;
    phy.open().await.unwrap();
    let addr = phy.get_addr().await.unwrap();

    let _c1 = TcpStream::connect(&addr).await.unwrap();
    let _c2 = TcpStream::connect(&addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut conn_close_rx = phy.subscribe_connection_close();
    phy.close().await.unwrap();

    let mut count = 0;
    while let Ok(Ok(_)) =
        tokio::time::timeout(Duration::from_millis(500), conn_close_rx.recv()).await
    {
        count += 1;
        if count >= 2 {
            break;
        }
    }
    assert!(
        count >= 1,
        "expected at least one connection-close on server close, got {}",
        count
    );
    phy.destroy().await;
}

#[tokio::test]
async fn tcp_server_rejects_double_open() {
    let phy = TcpServerPhysicalLayer::new();
    phy.set_addr("127.0.0.1:0".to_string()).await;
    phy.open().await.unwrap();
    let result = phy.open().await;
    assert!(
        matches!(result, Err(ModbusError::PortAlreadyOpen)),
        "expected PortAlreadyOpen, got {:?}",
        result
    );
    phy.destroy().await;
}

#[tokio::test]
async fn tcp_server_rejects_open_after_destroy() {
    let phy = TcpServerPhysicalLayer::new();
    phy.set_addr("127.0.0.1:0".to_string()).await;
    phy.open().await.unwrap();
    phy.destroy().await;
    assert!(matches!(phy.open().await, Err(ModbusError::PortDestroyed)));
}

// ===== UdpPhysicalLayer — lifecycle (server mode) =====

#[tokio::test]
async fn udp_reopens_after_close_and_receives_new_datagram() {
    let phy = UdpPhysicalLayer::new_server();
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    let mut rx = phy.subscribe_data();

    // Session 1
    phy.open().await.unwrap();
    let addr1 = phy.local_addr().await.unwrap();
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender.send_to(&[0xaa], &addr1).await.unwrap();
    let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("datagram within 2s")
        .unwrap();
    assert_eq!(evt.data, vec![0xaa]);
    phy.close().await.unwrap();
    assert!(!phy.is_open());

    // Session 2 — fresh ephemeral port, fresh dgram socket.
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    phy.open().await.unwrap();
    let addr2 = phy.local_addr().await.unwrap();
    sender.send_to(&[0xbb], &addr2).await.unwrap();
    let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("datagram within 2s")
        .unwrap();
    assert_eq!(evt.data, vec![0xbb]);
    phy.destroy().await;
}

#[tokio::test]
async fn udp_emits_close_event_on_user_initiated_close() {
    let phy = UdpPhysicalLayer::new_server();
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    let mut close_rx = phy.subscribe_close();
    phy.open().await.unwrap();
    phy.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), close_rx.recv())
        .await
        .expect("close event within 2s")
        .unwrap();
    phy.destroy().await;
}

#[tokio::test]
async fn udp_rejects_double_open() {
    let phy = UdpPhysicalLayer::new_server();
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    phy.open().await.unwrap();
    let result = phy.open().await;
    assert!(
        matches!(result, Err(ModbusError::PortAlreadyOpen)),
        "expected PortAlreadyOpen, got {:?}",
        result
    );
    phy.destroy().await;
}

#[tokio::test]
async fn udp_rejects_open_after_destroy() {
    let phy = UdpPhysicalLayer::new_server();
    phy.set_local_addr("127.0.0.1:0".to_string()).await;
    phy.open().await.unwrap();
    phy.destroy().await;
    assert!(matches!(phy.open().await, Err(ModbusError::PortDestroyed)));
}

// ===== SerialPhysicalLayer — lifecycle (no MockBinding in Rust) =====
//
// `serialport`'s Rust crate doesn't expose an in-memory mock binding, so the
// data-exchange and close-event flows for serial are exercised by the master
// integration tests using real ports. Here we only cover the guards that don't
// need a live port — destroyed/closed state transitions.

#[cfg(feature = "serial")]
mod serial_lifecycle {
    use super::*;
    use rs_modbus::layers::physical::SerialPhysicalLayer;

    #[tokio::test]
    async fn serial_rejects_open_after_destroy() {
        let phy = SerialPhysicalLayer::new("/dev/rs-modbus-nonexistent".to_string(), 9600);
        phy.destroy().await;
        assert!(matches!(phy.open().await, Err(ModbusError::PortDestroyed)));
    }

    #[tokio::test]
    async fn serial_close_before_open_is_noop() {
        let phy = SerialPhysicalLayer::new("/dev/rs-modbus-nonexistent".to_string(), 9600);
        // close() must succeed on a never-opened layer without panicking.
        phy.close().await.unwrap();
    }
}
