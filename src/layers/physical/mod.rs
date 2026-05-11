use crate::error::ModbusError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;

pub type ResponseFn = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), ModbusError>> + Send>> + Send + Sync,
>;

#[async_trait::async_trait]
pub trait PhysicalLayer: Send + Sync {
    async fn open(&self) -> Result<(), ModbusError>;
    async fn write(&self, data: &[u8]) -> Result<(), ModbusError>;
    async fn close(&self) -> Result<(), ModbusError>;
    async fn destroy(&self);
    fn is_open(&self) -> bool;
    fn is_destroyed(&self) -> bool;

    fn subscribe_data(&self) -> broadcast::Receiver<(Vec<u8>, ResponseFn)>;
    fn subscribe_error(&self) -> broadcast::Receiver<ModbusError>;
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

    #[tokio::test]
    async fn test_tcp_client_server_communication() {
        let server = TcpServerPhysicalLayer::new();
        *server.addr.lock().await = Some("127.0.0.1:1502".to_string());
        server.open().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let client = TcpClientPhysicalLayer::new();
        *client.addr.lock().await = Some("127.0.0.1:1502".to_string());
        client.open().await.unwrap();

        let mut server_rx = server.subscribe_data();
        let mut client_rx = client.subscribe_data();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let test_data = vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x0a];
        client.write(&test_data).await.unwrap();

        let (received, response) = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            server_rx.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(received, test_data);

        let response_data = vec![0x01, 0x03, 0x02, 0x00, 0x0a];
        response(response_data.clone()).await.unwrap();

        let (client_received, _) = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            client_rx.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(client_received, response_data);

        client.destroy().await;
        server.destroy().await;
    }

    #[tokio::test]
    async fn test_udp_communication() {
        let server = UdpPhysicalLayer::new_server();
        *server.local_addr.lock().await = Some("127.0.0.1:0".to_string());
        server.open().await.unwrap();

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

        let (received, response) = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            server_rx.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(received, test_data);

        let response_data = vec![0x01, 0x03, 0x02, 0x00, 0x0a];
        response(response_data.clone()).await.unwrap();

        let (client_received, _) = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            client_rx.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(client_received, response_data);

        client.destroy().await;
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
