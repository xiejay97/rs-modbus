use crate::error::ModbusError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Identifies a single connection (socket / serial port / udp peer) within a
/// physical layer. Cheaply cloneable via `Arc`.
pub type ConnectionId = Arc<str>;

/// Differentiates serial transports (where RTU needs 3.5T inter-frame timing)
/// from network transports (where TCP/UDP delivery boundaries are already
/// message-aligned in practice).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhysicalLayerType {
    Serial,
    Net,
}

/// Per-message reply closure. The physical layer hands this to upper layers so
/// they can write a response back to the originating connection without
/// exposing connection-id details.
pub type ResponseFn = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), ModbusError>> + Send>> + Send + Sync,
>;

/// Payload of the `subscribe_data` broadcast channel. Carries the bytes that
/// arrived, the reply closure for the originating connection, and the
/// connection identifier so upper layers can demultiplex.
#[derive(Clone)]
pub struct DataEvent {
    pub data: Vec<u8>,
    pub response: ResponseFn,
    pub connection: ConnectionId,
}

#[async_trait::async_trait]
pub trait PhysicalLayer: Send + Sync {
    /// Distinguishes serial vs network transports. Used by the RTU
    /// application layer to decide whether to apply 3.5T inter-frame timing.
    fn layer_type(&self) -> PhysicalLayerType;

    async fn open(&self) -> Result<(), ModbusError>;
    async fn write(&self, data: &[u8]) -> Result<(), ModbusError>;
    async fn close(&self) -> Result<(), ModbusError>;
    async fn destroy(&self);

    fn is_open(&self) -> bool;
    fn is_destroyed(&self) -> bool;

    /// Subscribe to incoming bytes from any connection. Each event carries the
    /// connection id so upper layers can demultiplex.
    fn subscribe_data(&self) -> broadcast::Receiver<DataEvent>;

    /// Subscribe to outgoing bytes written via [`write`]. Useful for logging.
    fn subscribe_write(&self) -> broadcast::Receiver<Vec<u8>>;

    fn subscribe_error(&self) -> broadcast::Receiver<ModbusError>;

    /// Subscribe to individual connection-level close events (a single socket
    /// disconnecting in a TCP server, the serial port closing, etc.). Separate
    /// from `subscribe_close` which fires when the whole physical layer shuts
    /// down.
    fn subscribe_connection_close(&self) -> broadcast::Receiver<ConnectionId>;

    fn subscribe_close(&self) -> broadcast::Receiver<()>;
}

mod tcp_client;
mod tcp_server;
mod udp;

pub use tcp_client::TcpClientPhysicalLayer;
pub use tcp_server::TcpServerPhysicalLayer;
pub use udp::UdpPhysicalLayer;

#[cfg(feature = "serial")]
mod serial;
#[cfg(feature = "serial")]
pub use serial::SerialPhysicalLayer;

#[cfg(test)]
mod tests {
    use super::*;

    // ===== Base types =====

    #[test]
    fn test_physical_layer_type_equality() {
        assert_eq!(PhysicalLayerType::Serial, PhysicalLayerType::Serial);
        assert_ne!(PhysicalLayerType::Serial, PhysicalLayerType::Net);
    }

    #[test]
    fn test_data_event_clone_preserves_fields() {
        let response: ResponseFn = Arc::new(|_| Box::pin(async { Ok(()) }));
        let conn: ConnectionId = Arc::from("test-conn-1");
        let event = DataEvent {
            data: vec![1, 2, 3],
            response: Arc::clone(&response),
            connection: Arc::clone(&conn),
        };
        let cloned = event.clone();
        assert_eq!(cloned.data, vec![1, 2, 3]);
        assert_eq!(&*cloned.connection, "test-conn-1");
    }

    #[test]
    fn test_connection_id_is_cheap_to_clone() {
        // ConnectionId should be Arc<str> so clone is O(1)
        let id: ConnectionId = Arc::from("hello");
        let cloned = Arc::clone(&id);
        assert_eq!(&*id, "hello");
        assert_eq!(&*cloned, "hello");
        // Both should point to same allocation
        assert!(Arc::ptr_eq(&id, &cloned));
    }

    #[tokio::test]
    async fn test_tcp_client_server_communication() {
        let server = TcpServerPhysicalLayer::new();
        server.set_addr("127.0.0.1:0".to_string()).await;
        server.open().await.unwrap();
        assert_eq!(server.layer_type(), PhysicalLayerType::Net);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let client = TcpClientPhysicalLayer::new();
        client.set_addr(server.get_addr().await.unwrap()).await;
        client.open().await.unwrap();
        assert_eq!(client.layer_type(), PhysicalLayerType::Net);

        let mut server_rx = server.subscribe_data();
        let mut client_rx = client.subscribe_data();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let test_data = vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x0a];
        client.write(&test_data).await.unwrap();

        let event = tokio::time::timeout(tokio::time::Duration::from_secs(2), server_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.data, test_data);
        assert!(!event.connection.is_empty(), "server should issue a connection id");

        let response_data = vec![0x01, 0x03, 0x02, 0x00, 0x0a];
        (event.response)(response_data.clone()).await.unwrap();

        let client_event =
            tokio::time::timeout(tokio::time::Duration::from_secs(2), client_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(client_event.data, response_data);

        client.destroy().await;
        server.destroy().await;
    }

    #[tokio::test]
    async fn test_udp_communication() {
        let server = UdpPhysicalLayer::new_server();
        *server.local_addr.lock().await = Some("127.0.0.1:0".to_string());
        server.open().await.unwrap();
        assert_eq!(server.layer_type(), PhysicalLayerType::Net);

        let server_addr = {
            let socket = server.socket.lock().await;
            socket.as_ref().unwrap().local_addr().unwrap()
        };

        let client = UdpPhysicalLayer::new_client(server_addr.to_string());
        *client.local_addr.lock().await = Some("127.0.0.1:0".to_string());
        client.open().await.unwrap();

        let mut server_rx = server.subscribe_data();
        let mut client_rx = client.subscribe_data();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let test_data = vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x0a];
        client.write(&test_data).await.unwrap();

        let event = tokio::time::timeout(tokio::time::Duration::from_secs(2), server_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.data, test_data);

        let response_data = vec![0x01, 0x03, 0x02, 0x00, 0x0a];
        (event.response)(response_data.clone()).await.unwrap();

        let client_event =
            tokio::time::timeout(tokio::time::Duration::from_secs(2), client_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(client_event.data, response_data);

        client.destroy().await;
        server.destroy().await;
    }

    #[tokio::test]
    async fn test_tcp_server_emits_connection_close() {
        let server = TcpServerPhysicalLayer::new();
        server.set_addr("127.0.0.1:0".to_string()).await;
        server.open().await.unwrap();

        let mut close_rx = server.subscribe_connection_close();

        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;

        let client = TcpClientPhysicalLayer::new();
        client.set_addr(server.get_addr().await.unwrap()).await;
        client.open().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        client.destroy().await;

        let closed_id = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            close_rx.recv(),
        )
        .await
        .expect("should receive connection_close within 2s")
        .expect("subscribe_connection_close should yield an id");
        assert!(!closed_id.is_empty());

        server.destroy().await;
    }

    #[tokio::test]
    async fn test_write_before_open_fails() {
        let client = TcpClientPhysicalLayer::new();
        let result = client.write(&[0x01]).await;
        assert!(matches!(result, Err(ModbusError::PortNotOpen)));
    }

    #[tokio::test]
    async fn test_server_write_not_supported() {
        let server = TcpServerPhysicalLayer::new();
        let result = server.write(&[0x01]).await;
        assert!(matches!(result, Err(ModbusError::NotSupported)));
    }
}
