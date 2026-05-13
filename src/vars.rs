//! Modbus protocol constants — function codes, exception offsets, MEI types,
//! and PDU quantity limits. Mirrors njs-modbus `vars.ts`.
//!
//! These exist as named constants instead of hex literals so call sites read as
//! "WRITE_MULTIPLE_COILS_MAX" instead of "0x07b0", matching the Modbus V1.1b3
//! spec wording.

/// Standard Modbus function codes (V1.1b3 §6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FunctionCode {
    ReadCoils = 0x01,
    ReadDiscreteInputs = 0x02,
    ReadHoldingRegisters = 0x03,
    ReadInputRegisters = 0x04,
    WriteSingleCoil = 0x05,
    WriteSingleRegister = 0x06,
    WriteMultipleCoils = 0x0f,
    WriteMultipleRegisters = 0x10,
    ReportServerId = 0x11,
    MaskWriteRegister = 0x16,
    ReadWriteMultipleRegisters = 0x17,
    ReadDeviceIdentification = 0x2b,
}

impl FunctionCode {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Exception response FC = request FC | EXCEPTION_OFFSET (V1.1b3 §7).
pub const EXCEPTION_OFFSET: u8 = 0x80;

/// Coil ON value for FC 5 / FC 15 (V1.1b3 §6.5/§6.11).
pub const COIL_ON: u16 = 0xff00;
/// Coil OFF value for FC 5 / FC 15.
pub const COIL_OFF: u16 = 0x0000;

/// FC 0x2B MEI sub-function selecting Read Device Identification (V1.1b3 §6.21).
pub const MEI_READ_DEVICE_ID: u8 = 0x0e;

/// Read Device ID code values inside an FC 0x2B / MEI 0x0E request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ReadDeviceIdCode {
    BasicStream = 0x01,
    RegularStream = 0x02,
    ExtendedStream = 0x03,
    SpecificAccess = 0x04,
}

/// Conformity level reported in an FC 0x2B / MEI 0x0E response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ConformityLevel {
    Basic = 0x81,
    Regular = 0x82,
    Extended = 0x83,
}

/// Modbus V1.1b3 PDU quantity limits.
pub mod limits {
    pub const READ_COILS_MIN: u16 = 0x0001;
    pub const READ_COILS_MAX: u16 = 0x07d0;
    pub const READ_REGISTERS_MIN: u16 = 0x0001;
    pub const READ_REGISTERS_MAX: u16 = 0x007d;
    pub const WRITE_COILS_MAX: u16 = 0x07b0;
    pub const WRITE_REGISTERS_MAX: u16 = 0x007b;
    pub const RW_REGISTERS_WRITE_MAX: u16 = 0x0079;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn function_code_values_match_spec() {
        assert_eq!(FunctionCode::ReadCoils as u8, 0x01);
        assert_eq!(FunctionCode::ReadDiscreteInputs as u8, 0x02);
        assert_eq!(FunctionCode::ReadHoldingRegisters as u8, 0x03);
        assert_eq!(FunctionCode::ReadInputRegisters as u8, 0x04);
        assert_eq!(FunctionCode::WriteSingleCoil as u8, 0x05);
        assert_eq!(FunctionCode::WriteSingleRegister as u8, 0x06);
        assert_eq!(FunctionCode::WriteMultipleCoils as u8, 0x0f);
        assert_eq!(FunctionCode::WriteMultipleRegisters as u8, 0x10);
        assert_eq!(FunctionCode::ReportServerId as u8, 0x11);
        assert_eq!(FunctionCode::MaskWriteRegister as u8, 0x16);
        assert_eq!(FunctionCode::ReadWriteMultipleRegisters as u8, 0x17);
        assert_eq!(FunctionCode::ReadDeviceIdentification as u8, 0x2b);
    }

    #[test]
    fn exception_offset_is_high_bit() {
        assert_eq!(EXCEPTION_OFFSET, 0x80);
    }

    #[test]
    fn coil_constants_match_spec() {
        assert_eq!(COIL_ON, 0xff00);
        assert_eq!(COIL_OFF, 0x0000);
    }

    #[test]
    fn limits_match_spec() {
        assert_eq!(limits::READ_COILS_MIN, 1);
        assert_eq!(limits::READ_COILS_MAX, 2000);
        assert_eq!(limits::READ_REGISTERS_MIN, 1);
        assert_eq!(limits::READ_REGISTERS_MAX, 125);
        assert_eq!(limits::WRITE_COILS_MAX, 1968);
        assert_eq!(limits::WRITE_REGISTERS_MAX, 123);
        assert_eq!(limits::RW_REGISTERS_WRITE_MAX, 121);
    }

    #[test]
    fn conformity_levels_match_spec() {
        assert_eq!(ConformityLevel::Basic as u8, 0x81);
        assert_eq!(ConformityLevel::Regular as u8, 0x82);
        assert_eq!(ConformityLevel::Extended as u8, 0x83);
    }

    #[test]
    fn mei_read_device_id_is_0x0e() {
        assert_eq!(MEI_READ_DEVICE_ID, 0x0e);
    }
}
