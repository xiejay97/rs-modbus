use crate::error::ModbusError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ApplicationDataUnit {
    pub transaction: Option<u16>,
    pub unit: u8,
    pub fc: u8,
    pub data: Vec<u8>,
}

impl ApplicationDataUnit {
    pub fn new(unit: u8, fc: u8, data: Vec<u8>) -> Self {
        Self {
            transaction: None,
            unit,
            fc,
            data,
        }
    }

    pub fn with_transaction(mut self, transaction: u16) -> Self {
        self.transaction = Some(transaction);
        self
    }
}

#[derive(Debug, Clone)]
pub struct FramedDataUnit {
    pub adu: ApplicationDataUnit,
    pub raw: Vec<u8>,
}

/// FC17 Server ID. Modbus V1.1b3 §6.17 leaves the Server ID length as
/// device-specific (N bytes), so multi-byte IDs are supported via
/// [`ServerId::Multi`]. Mirrors njs-modbus `ServerId { serverId: number | number[] }`.
#[derive(Debug, Clone)]
pub struct ServerId {
    /// Server ID bytes — typically 1 byte, but the spec allows N bytes.
    pub server_id: Vec<u8>,
    pub run_indicator_status: bool,
    pub additional_data: Vec<u8>,
}

impl ServerId {
    /// Convenience constructor for a single-byte Server ID.
    pub fn single(id: u8, run_indicator: bool, additional_data: Vec<u8>) -> Self {
        Self {
            server_id: vec![id],
            run_indicator_status: run_indicator,
            additional_data,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeviceIdentification {
    pub read_device_id_code: u8,
    pub conformity_level: u8,
    pub more_follows: bool,
    pub next_object_id: u8,
    pub objects: Vec<DeviceObject>,
}

#[derive(Debug, Clone)]
pub struct DeviceObject {
    pub id: u8,
    pub value: String,
}

#[derive(Debug, Clone, Default)]
pub struct AddressRange {
    pub discrete_inputs: Vec<(u16, u16)>,
    pub coils: Vec<(u16, u16)>,
    pub input_registers: Vec<(u16, u16)>,
    pub holding_registers: Vec<(u16, u16)>,
}

/// Predictor result from a [`CustomFunctionCode`].
///
/// - `Length(n)` — total RTU frame length (PDU + CRC) is `n` bytes.
/// - `NeedMore` — predictor can't decide yet; wait for more bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustomFcPredict {
    Length(usize),
    NeedMore,
}

/// Async handler return for [`CustomFunctionCode::handle`].
pub type CustomFcHandleResult =
    Pin<Box<dyn Future<Output = Result<Vec<u8>, ModbusError>> + Send + 'static>>;

/// Slave-side handler: receives PDU payload (bytes after `fc`, before CRC) and
/// the unit ID being addressed; must return the PDU payload of the response.
pub type CustomFcHandler =
    Arc<dyn Fn(Vec<u8>, u8) -> CustomFcHandleResult + Send + Sync + 'static>;

/// Defines a non-standard / user-defined Modbus function code. Mirrors
/// njs-modbus `CustomFunctionCode`.
///
/// Registration paths:
/// - [`crate::layers::application::RtuApplicationLayer::add_custom_function_code`] — framing only.
/// - [`crate::slave::ModbusSlave::add_custom_function_code`] — framing + slave-side dispatch.
/// - [`crate::master::ModbusMaster::add_custom_function_code`] + `send_custom_fc` — framing + request issuance.
///
/// The two `predict_*` callbacks declare how to derive the total RTU frame
/// length (PDU + 2-byte CRC) from leading bytes; they are required so the
/// framing FSM can advance without the deleted sliding-window CRC fallback.
#[derive(Clone)]
pub struct CustomFunctionCode {
    /// Function code value (must fit in `u8`).
    pub fc: u8,
    /// Predict total RTU frame length for an incoming request (slave-side framing).
    pub predict_request_length: Arc<dyn Fn(&[u8]) -> CustomFcPredict + Send + Sync + 'static>,
    /// Predict total RTU frame length for an incoming response (master-side framing).
    pub predict_response_length: Arc<dyn Fn(&[u8]) -> CustomFcPredict + Send + Sync + 'static>,
    /// Slave-side handler. Returning `Err` is turned into a Modbus exception
    /// response by the slave. If `handle` is `None`, the slave returns an
    /// `ILLEGAL_FUNCTION` exception for this FC.
    pub handle: Option<CustomFcHandler>,
}

impl std::fmt::Debug for CustomFunctionCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomFunctionCode")
            .field("fc", &self.fc)
            .field("handle", &self.handle.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adu_new() {
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x00, 0x00, 0x0a]);
        assert_eq!(adu.unit, 1);
        assert_eq!(adu.fc, 0x03);
        assert_eq!(adu.data, vec![0x00, 0x00, 0x00, 0x0a]);
        assert_eq!(adu.transaction, None);
    }

    #[test]
    fn test_adu_with_transaction() {
        let adu = ApplicationDataUnit::new(1, 0x03, vec![]).with_transaction(42);
        assert_eq!(adu.transaction, Some(42));
    }

    #[test]
    fn test_framed_data_unit() {
        let adu = ApplicationDataUnit::new(1, 0x03, vec![0x00, 0x01]);
        let raw = vec![0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x01, 0x03, 0x00, 0x01];
        let frame = FramedDataUnit {
            adu,
            raw: raw.clone(),
        };
        assert_eq!(frame.raw, raw);
    }

    #[test]
    fn test_server_id_single() {
        let sid = ServerId::single(1, true, vec![1, 2, 3]);
        assert_eq!(sid.server_id, vec![1]);
        assert!(sid.run_indicator_status);
        assert_eq!(sid.additional_data, vec![1, 2, 3]);
    }

    #[test]
    fn test_server_id_multi() {
        let sid = ServerId {
            server_id: vec![0x01, 0x02, 0x03],
            run_indicator_status: false,
            additional_data: vec![0xab, 0xcd],
        };
        assert_eq!(sid.server_id.len(), 3);
        assert!(!sid.run_indicator_status);
    }

    #[test]
    fn test_device_object() {
        let obj = DeviceObject {
            id: 0x01,
            value: "ProductCode".to_string(),
        };
        assert_eq!(obj.id, 0x01);
        assert_eq!(obj.value, "ProductCode");
    }

    #[test]
    fn test_device_identification() {
        let di = DeviceIdentification {
            read_device_id_code: 0x01,
            conformity_level: 0x81,
            more_follows: false,
            next_object_id: 0x00,
            objects: vec![
                DeviceObject {
                    id: 0x00,
                    value: "VendorName".to_string(),
                },
                DeviceObject {
                    id: 0x01,
                    value: "ProductCode".to_string(),
                },
            ],
        };
        assert_eq!(di.read_device_id_code, 0x01);
        assert_eq!(di.conformity_level, 0x81);
        assert!(!di.more_follows);
        assert_eq!(di.objects.len(), 2);
    }

    #[test]
    fn test_address_range_default() {
        let range = AddressRange::default();
        assert!(range.coils.is_empty());
        assert!(range.discrete_inputs.is_empty());
        assert!(range.input_registers.is_empty());
        assert!(range.holding_registers.is_empty());
    }

    #[test]
    fn test_custom_function_code_construction() {
        let cfc = CustomFunctionCode {
            fc: 0x65,
            predict_request_length: Arc::new(|_| CustomFcPredict::Length(8)),
            predict_response_length: Arc::new(|_| CustomFcPredict::Length(8)),
            handle: None,
        };
        assert_eq!(cfc.fc, 0x65);
        assert!(cfc.handle.is_none());
    }
}

