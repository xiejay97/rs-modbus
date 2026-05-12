use crate::error::ModbusError;
use crate::layers::physical::{ConnectionId, ResponseFn};
use crate::types::{ApplicationDataUnit, FramedDataUnit};

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

pub trait ApplicationLayer: Send + Sync {
    fn encode(&self, adu: &ApplicationDataUnit) -> Vec<u8>;
    fn decode(&self, data: &[u8]) -> Result<FramedDataUnit, ModbusError>;
}

mod ascii;
mod rtu;
mod tcp;

pub use ascii::AsciiApplicationLayer;
pub use rtu::RtuApplicationLayer;
pub use tcp::TcpApplicationLayer;

#[cfg(test)]
mod tests {
    use super::*;

    // ===== Base types =====

    #[test]
    fn test_application_role_equality() {
        assert_eq!(ApplicationRole::Master, ApplicationRole::Master);
        assert_ne!(ApplicationRole::Master, ApplicationRole::Slave);
    }

    #[test]
    fn test_framing_clone_preserves_fields() {
        use crate::layers::physical::{ConnectionId, ResponseFn};
        use std::sync::Arc;

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

    #[test]
    fn test_tcp_encode() {
        let layer = TcpApplicationLayer::new();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let frame = layer.encode(&adu);
        assert_eq!(frame.len(), 12);
        assert_eq!(&frame[2..4], [0x00, 0x00]); // protocol = 0
        assert_eq!(u16::from_be_bytes([frame[4], frame[5]]), 6); // length = 2 + 4
        assert_eq!(frame[6], 1); // unit
        assert_eq!(frame[7], 0x03); // fc
        assert_eq!(&frame[8..], [0x00, 0x00, 0x00, 0x0a]);
    }

    #[test]
    fn test_tcp_encode_with_transaction() {
        let layer = TcpApplicationLayer::new();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![]).with_transaction(42);
        let frame = layer.encode(&adu);
        assert_eq!(u16::from_be_bytes([frame[0], frame[1]]), 42);
    }

    #[test]
    fn test_tcp_decode() {
        let layer = TcpApplicationLayer::new();
        let frame = vec![0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x01, 0x03, 0x00, 0x0a];
        let decoded = layer.decode(&frame).unwrap();
        assert_eq!(decoded.adu.transaction, Some(1));
        assert_eq!(decoded.adu.unit, 1);
        assert_eq!(decoded.adu.fc, 0x03);
        assert_eq!(decoded.adu.data, vec![0x00, 0x0a]);
    }

    #[test]
    fn test_tcp_decode_invalid_protocol() {
        let layer = TcpApplicationLayer::new();
        let frame = vec![0x00, 0x01, 0x00, 0x01, 0x00, 0x04, 0x01, 0x03, 0x00, 0x0a];
        assert!(matches!(
            layer.decode(&frame),
            Err(ModbusError::InvalidData)
        ));
    }

    #[test]
    fn test_tcp_roundtrip() {
        let layer = TcpApplicationLayer::new();
        let adu =
            ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]).with_transaction(5);
        let encoded = layer.encode(&adu);
        let decoded = layer.decode(&encoded).unwrap();
        assert_eq!(decoded.adu.transaction, Some(5));
        assert_eq!(decoded.adu.unit, 1);
        assert_eq!(decoded.adu.fc, 0x03);
        assert_eq!(decoded.adu.data, vec![0x00, 0x00, 0x00, 0x0a]);
    }

    #[test]
    fn test_rtu_encode() {
        let layer = RtuApplicationLayer::new();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let frame = layer.encode(&adu);
        assert_eq!(frame.len(), 8);
        assert_eq!(frame[0], 1);
        assert_eq!(frame[1], 0x03);
        assert_eq!(&frame[2..6], [0x00, 0x00, 0x00, 0x0a]);
        let crc_val = u16::from_le_bytes([frame[6], frame[7]]);
        assert_eq!(crate::utils::crc(&frame[..6]), crc_val);
    }

    #[test]
    fn test_rtu_decode() {
        let layer = RtuApplicationLayer::new();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let frame = layer.encode(&adu);
        let decoded = layer.decode(&frame).unwrap();
        assert_eq!(decoded.adu.unit, 1);
        assert_eq!(decoded.adu.fc, 0x03);
        assert_eq!(decoded.adu.data, vec![0x00, 0x00, 0x00, 0x0a]);
    }

    #[test]
    fn test_rtu_decode_crc_fail() {
        let layer = RtuApplicationLayer::new();
        let frame = vec![0x01, 0x03, 0x00, 0x00, 0x00, 0x0a, 0xFF, 0xFF];
        assert!(matches!(
            layer.decode(&frame),
            Err(ModbusError::CrcCheckFailed)
        ));
    }

    #[test]
    fn test_rtu_roundtrip() {
        let layer = RtuApplicationLayer::new();
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
    }

    #[test]
    fn test_ascii_encode() {
        let layer = AsciiApplicationLayer::new();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let frame = layer.encode(&adu);
        let frame_str = String::from_utf8(frame.clone()).unwrap();
        assert!(frame_str.starts_with(':'));
        assert!(frame_str.ends_with("\r\n"));
        assert_eq!(frame_str.len(), 1 + 2 + 2 + 8 + 2 + 2);
    }

    #[test]
    fn test_ascii_decode() {
        let layer = AsciiApplicationLayer::new();
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        let encoded = layer.encode(&adu);
        let decoded = layer.decode(&encoded).unwrap();
        assert_eq!(decoded.adu.unit, 1);
        assert_eq!(decoded.adu.fc, 0x03);
        assert_eq!(decoded.adu.data, vec![0x00, 0x00, 0x00, 0x0a]);
    }

    #[test]
    fn test_ascii_roundtrip() {
        let layer = AsciiApplicationLayer::new();
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
    }

    #[test]
    fn test_ascii_decode_lrc_fail() {
        let layer = AsciiApplicationLayer::new();
        let frame = b":01030000000AFF\r\n";
        assert!(matches!(
            layer.decode(frame),
            Err(ModbusError::LrcCheckFailed)
        ));
    }
}
