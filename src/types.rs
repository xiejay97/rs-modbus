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

#[derive(Debug, Clone)]
pub struct ServerId {
    pub server_id: u8,
    pub run_indicator_status: bool,
    pub additional_data: Vec<u8>,
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
        let frame = FramedDataUnit { adu, raw: raw.clone() };
        assert_eq!(frame.raw, raw);
    }

    #[test]
    fn test_server_id() {
        let sid = ServerId {
            server_id: 1,
            run_indicator_status: true,
            additional_data: vec![1, 2, 3],
        };
        assert_eq!(sid.server_id, 1);
        assert!(sid.run_indicator_status);
        assert_eq!(sid.additional_data, vec![1, 2, 3]);
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
                DeviceObject { id: 0x00, value: "VendorName".to_string() },
                DeviceObject { id: 0x01, value: "ProductCode".to_string() },
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
}
