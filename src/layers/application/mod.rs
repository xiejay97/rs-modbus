use crate::error::ModbusError;
use crate::layers::physical::{ConnectionId, ResponseFn};
use crate::types::{ApplicationDataUnit, CustomFunctionCode, FramedDataUnit};
use tokio::sync::broadcast;

/// Application-layer role. Set by [`ModbusMaster`] / [`ModbusSlave`] when they
/// take ownership of an application layer.
///
/// RTU framing differentiates request vs response by role (request and
/// response of the same FC may have different lengths).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplicationRole {
    Master,
    Slave,
}

/// Shared implementation of [`ApplicationLayer::set_role`].
///
/// Returns `Ok(())` on first set or re-setting the same role.
/// Returns `InvalidState` if a *different* role is already assigned.
pub(crate) fn set_role_impl(
    current: &mut Option<ApplicationRole>,
    role: ApplicationRole,
) -> Result<(), ModbusError> {
    match *current {
        Some(existing) if existing == role => Ok(()),
        Some(existing) => Err(ModbusError::InvalidState(format!(
            "application layer role already set to {existing:?}, cannot change to {role:?}"
        ))),
        None => {
            *current = Some(role);
            Ok(())
        }
    }
}

/// Wire protocol implemented by an [`ApplicationLayer`]. Exposed through
/// [`ApplicationLayer::protocol`] so callers (master, slave, tests) can
/// gate protocol-specific behavior — notably, `ModbusMaster` uses it to
/// reject `concurrent: true` configurations on non-TCP layers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplicationProtocol {
    Tcp,
    Rtu,
    Ascii,
}

/// A successfully framed PDU emitted by an [`ApplicationLayer`] via
/// `subscribe_framing`. Carries the parsed ADU, the raw bytes that produced it,
/// the per-message reply closure, and the connection it came from.
#[derive(Clone)]
pub struct Framing {
    pub adu: ApplicationDataUnit,
    pub raw: Vec<u8>,
    pub response: ResponseFn,
    pub connection: ConnectionId,
}

#[async_trait::async_trait]
pub trait ApplicationLayer: Send + Sync {
    /// Bind the application layer to a master/slave role. Must succeed on the
    /// first call and fail (`ModbusError::InvalidState`) if a different role
    /// is then assigned. Re-assigning the same role is a no-op (idempotent).
    fn set_role(&self, role: ApplicationRole) -> Result<(), ModbusError>;

    /// Current role, or `None` if not yet assigned.
    fn role(&self) -> Option<ApplicationRole>;

    /// Wire protocol implemented by this layer. Used by `ModbusMaster` to
    /// validate `concurrent` configuration at construction time.
    fn protocol(&self) -> ApplicationProtocol;

    /// Encode an ADU into wire bytes per the protocol's framing format
    /// (MBAP for TCP, CRC for RTU, hex+LRC for ASCII).
    fn encode(&self, adu: &ApplicationDataUnit) -> Vec<u8>;

    /// Decode a single complete frame back into an ADU. Returned only for
    /// backward compatibility with the previous stateless API; new consumers
    /// should subscribe to `subscribe_framing` instead.
    fn decode(&self, data: &[u8]) -> Result<FramedDataUnit, ModbusError>;

    /// Drop any per-connection state (decoding buffers, timers). Called by
    /// Master before each request, by Slave between sessions.
    fn flush(&self);

    /// Subscribe to successfully framed PDUs assembled from the underlying
    /// physical layer.
    fn subscribe_framing(&self) -> broadcast::Receiver<Framing>;

    /// Subscribe to framing-level errors (CRC/LRC failure, invalid MBAP
    /// header, etc.). One error per offending physical-layer chunk.
    fn subscribe_framing_error(&self) -> broadcast::Receiver<ModbusError>;

    /// Register a custom function code predictor. Default is a no-op; only
    /// [`RtuApplicationLayer`] overrides this with real behavior.
    fn add_custom_function_code(&self, _cfc: CustomFunctionCode) {}

    /// Remove a previously registered custom function code. Default is a no-op.
    fn remove_custom_function_code(&self, _fc: u8) {}

    /// Release task handles and drop physical-layer subscriptions.
    async fn destroy(&self);
}

mod ascii;
mod rtu;
mod tcp;

pub use ascii::{AsciiApplicationLayer, AsciiApplicationLayerOptions};
pub use rtu::{FrameInterval, RtuApplicationLayer, RtuApplicationLayerOptions};
pub use tcp::TcpApplicationLayer;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::physical::{PhysicalLayer, TcpClientPhysicalLayer, TcpServerPhysicalLayer};
    use std::sync::Arc;

    // ===== Base types =====

    #[test]
    fn test_application_role_equality() {
        assert_eq!(ApplicationRole::Master, ApplicationRole::Master);
        assert_ne!(ApplicationRole::Master, ApplicationRole::Slave);
    }

    #[test]
    fn test_framing_clone_preserves_fields() {
        use crate::layers::physical::{ConnectionId, ResponseFn};

        let response: ResponseFn = Arc::new(|_| Box::pin(async { Ok(()) }));
        let conn: ConnectionId = Arc::from("test");
        let framing = Framing {
            adu: ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x0a]),
            raw: vec![0xff; 4],
            response,
            connection: conn,
        };
        let cloned = framing.clone();
        assert_eq!(cloned.adu.unit, 1);
        assert_eq!(cloned.adu.fc, 0x03);
        assert_eq!(cloned.raw, vec![0xff; 4]);
        assert_eq!(&*cloned.connection, "test");
    }

    // ===== role / set_role =====

    fn make_tcp_app() -> Arc<TcpApplicationLayer> {
        // Bind to an idle server physical (never opened) so we have a real
        // PhysicalLayer to subscribe; the spawned task simply awaits an event
        // that will never come, which is fine for tests focused on
        // encode/decode/role behavior.
        let physical = TcpServerPhysicalLayer::new();
        TcpApplicationLayer::new(physical)
    }

    fn make_rtu_app() -> Arc<RtuApplicationLayer> {
        // RTU bound to a TCP-like (Net) physical: no inter-frame timer is
        // needed and decode is stateless in this commit.
        let physical = TcpClientPhysicalLayer::new();
        RtuApplicationLayer::new(physical, RtuApplicationLayerOptions::default())
    }

    fn make_ascii_app() -> Arc<AsciiApplicationLayer> {
        let physical = TcpServerPhysicalLayer::new();
        AsciiApplicationLayer::new(physical)
    }

    #[tokio::test]
    async fn test_set_role_first_call_succeeds() {
        let app = make_tcp_app();
        assert_eq!(app.role(), None);
        app.set_role(ApplicationRole::Master).unwrap();
        assert_eq!(app.role(), Some(ApplicationRole::Master));
        app.destroy().await;
    }

    #[tokio::test]
    async fn test_set_role_same_again_is_idempotent() {
        let app = make_tcp_app();
        app.set_role(ApplicationRole::Slave).unwrap();
        app.set_role(ApplicationRole::Slave).unwrap();
        assert_eq!(app.role(), Some(ApplicationRole::Slave));
        app.destroy().await;
    }

    #[tokio::test]
    async fn test_set_role_conflict_returns_invalid_state() {
        let app = make_tcp_app();
        app.set_role(ApplicationRole::Master).unwrap();
        let err = app.set_role(ApplicationRole::Slave).unwrap_err();
        assert!(matches!(err, ModbusError::InvalidState(_)));
        app.destroy().await;
    }

    // ===== encode / decode =====

    #[tokio::test]
    async fn test_tcp_encode() {
        let layer = make_tcp_app();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let frame = layer.encode(&adu);
        assert_eq!(frame.len(), 12);
        assert_eq!(&frame[2..4], [0x00, 0x00]); // protocol = 0
        assert_eq!(u16::from_be_bytes([frame[4], frame[5]]), 6); // length = 2 + 4
        assert_eq!(frame[6], 1); // unit
        assert_eq!(frame[7], 0x03); // fc
        assert_eq!(&frame[8..], [0x00, 0x00, 0x00, 0x0a]);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_tcp_encode_with_transaction() {
        let layer = make_tcp_app();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![]).with_transaction(42);
        let frame = layer.encode(&adu);
        assert_eq!(u16::from_be_bytes([frame[0], frame[1]]), 42);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_tcp_decode() {
        let layer = make_tcp_app();
        let frame = vec![0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x01, 0x03, 0x00, 0x0a];
        let decoded = layer.decode(&frame).unwrap();
        assert_eq!(decoded.adu.transaction, Some(1));
        assert_eq!(decoded.adu.unit, 1);
        assert_eq!(decoded.adu.fc, 0x03);
        assert_eq!(decoded.adu.data, vec![0x00, 0x0a]);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_tcp_decode_invalid_protocol() {
        let layer = make_tcp_app();
        let frame = vec![0x00, 0x01, 0x00, 0x01, 0x00, 0x04, 0x01, 0x03, 0x00, 0x0a];
        assert!(matches!(
            layer.decode(&frame),
            Err(ModbusError::InvalidData)
        ));
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_tcp_roundtrip() {
        let layer = make_tcp_app();
        let adu =
            ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]).with_transaction(5);
        let encoded = layer.encode(&adu);
        let decoded = layer.decode(&encoded).unwrap();
        assert_eq!(decoded.adu.transaction, Some(5));
        assert_eq!(decoded.adu.unit, 1);
        assert_eq!(decoded.adu.fc, 0x03);
        assert_eq!(decoded.adu.data, vec![0x00, 0x00, 0x00, 0x0a]);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_rtu_encode() {
        let layer = make_rtu_app();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let frame = layer.encode(&adu);
        assert_eq!(frame.len(), 8);
        assert_eq!(frame[0], 1);
        assert_eq!(frame[1], 0x03);
        assert_eq!(&frame[2..6], [0x00, 0x00, 0x00, 0x0a]);
        let crc_val = u16::from_le_bytes([frame[6], frame[7]]);
        assert_eq!(crate::utils::crc(&frame[..6]), crc_val);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_rtu_decode() {
        let layer = make_rtu_app();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let frame = layer.encode(&adu);
        let decoded = layer.decode(&frame).unwrap();
        assert_eq!(decoded.adu.unit, 1);
        assert_eq!(decoded.adu.fc, 0x03);
        assert_eq!(decoded.adu.data, vec![0x00, 0x00, 0x00, 0x0a]);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_rtu_decode_crc_fail() {
        let layer = make_rtu_app();
        let frame = vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x0a, 0xFF, 0xFF];
        assert!(matches!(
            layer.decode(&frame),
            Err(ModbusError::CrcCheckFailed)
        ));
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_rtu_roundtrip() {
        let layer = make_rtu_app();
        let adu = ApplicationDataUnit::new(
            17,
            0x10,
            vec![0x00, 0x01, 0x00, 0x02, 0x04, 0xAB, 0xCD, 0xEF, 0x01],
        );
        let encoded = layer.encode(&adu);
        let decoded = layer.decode(&encoded).unwrap();
        assert_eq!(decoded.adu.unit, 17);
        assert_eq!(decoded.adu.fc, 0x10);
        assert_eq!(decoded.adu.data, adu.data);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_ascii_encode() {
        let layer = make_ascii_app();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let frame = layer.encode(&adu);
        let frame_str = String::from_utf8(frame.clone()).unwrap();
        assert!(frame_str.starts_with(':'));
        assert!(frame_str.ends_with("\r\n"));
        assert_eq!(frame_str.len(), 1 + 2 + 2 + 8 + 2 + 2);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_ascii_decode() {
        let layer = make_ascii_app();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let encoded = layer.encode(&adu);
        let decoded = layer.decode(&encoded).unwrap();
        assert_eq!(decoded.adu.unit, 1);
        assert_eq!(decoded.adu.fc, 0x03);
        assert_eq!(decoded.adu.data, vec![0x00, 0x00, 0x00, 0x0a]);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_ascii_roundtrip() {
        let layer = make_ascii_app();
        let adu = ApplicationDataUnit::new(
            17,
            0x10,
            vec![0x00, 0x01, 0x00, 0x02, 0x04, 0xAB, 0xCD, 0xEF, 0x01],
        );
        let encoded = layer.encode(&adu);
        let decoded = layer.decode(&encoded).unwrap();
        assert_eq!(decoded.adu.unit, 17);
        assert_eq!(decoded.adu.fc, 0x10);
        assert_eq!(decoded.adu.data, adu.data);
        layer.destroy().await;
    }

    #[tokio::test]
    async fn test_ascii_decode_lrc_fail() {
        let layer = make_ascii_app();
        let frame = b":01030000000AFF\r\n";
        assert!(matches!(
            layer.decode(frame),
            Err(ModbusError::LrcCheckFailed)
        ));
        layer.destroy().await;
    }

    // ===== framing event end-to-end (TCP) =====

    #[tokio::test]
    async fn test_framing_emits_on_valid_tcp_frame() {
        let server = TcpServerPhysicalLayer::new();
        server.set_addr("127.0.0.1:0".to_string()).await;
        server.open().await.unwrap();
        let application = TcpApplicationLayer::new(server.clone());

        // Bring up a peer client to push bytes at the server.
        let client = TcpClientPhysicalLayer::new();
        client.set_addr(server.get_addr().await.unwrap()).await;
        client.open().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let mut framing_rx = application.subscribe_framing();

        let frame = vec![0x00, 0x07, 0x00, 0x00, 0x00, 0x04, 0x01, 0x03, 0x00, 0x0a];
        client.write(&frame).await.unwrap();

        let f = tokio::time::timeout(tokio::time::Duration::from_secs(2), framing_rx.recv())
            .await
            .expect("framing event within 2s")
            .expect("framing channel still open");
        assert_eq!(f.adu.transaction, Some(7));
        assert_eq!(f.adu.unit, 1);
        assert_eq!(f.adu.fc, 0x03);
        assert_eq!(f.adu.data, vec![0x00, 0x0a]);

        client.destroy().await;
        server.destroy().await;
        application.destroy().await;
    }

    #[tokio::test]
    async fn test_framing_error_on_invalid_tcp_protocol() {
        let server = TcpServerPhysicalLayer::new();
        server.set_addr("127.0.0.1:0".to_string()).await;
        server.open().await.unwrap();
        let application = TcpApplicationLayer::new(server.clone());

        let client = TcpClientPhysicalLayer::new();
        client.set_addr(server.get_addr().await.unwrap()).await;
        client.open().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let mut err_rx = application.subscribe_framing_error();

        // Bogus protocol_id (bytes 2..4 != 0).
        let bogus = vec![0x00, 0x07, 0x12, 0x34, 0x00, 0x04, 0x01, 0x03, 0x00, 0x0a];
        client.write(&bogus).await.unwrap();

        let err = tokio::time::timeout(tokio::time::Duration::from_secs(2), err_rx.recv())
            .await
            .expect("framing_error event within 2s")
            .expect("framing_error channel still open");
        assert!(matches!(err, ModbusError::InvalidData));

        client.destroy().await;
        server.destroy().await;
        application.destroy().await;
    }
}
