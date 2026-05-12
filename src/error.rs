use thiserror::Error;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    IllegalFunction = 0x01,
    IllegalDataAddress = 0x02,
    IllegalDataValue = 0x03,
    ServerDeviceFailure = 0x04,
    Acknowledge = 0x05,
    ServerDeviceBusy = 0x06,
    MemoryParityError = 0x08,
    GatewayPathUnavailable = 0x0a,
    GatewayTargetDeviceFailedToRespond = 0x0b,
}

#[derive(Error, Debug, Clone)]
pub enum ModbusError {
    #[error("CRC check failed")]
    CrcCheckFailed,
    #[error("LRC check failed")]
    LrcCheckFailed,
    #[error("Insufficient data length")]
    InsufficientData,
    #[error("Invalid response")]
    InvalidResponse,
    #[error("Invalid data")]
    InvalidData,
    #[error("Timeout")]
    Timeout,
    #[error("Port is not open")]
    PortNotOpen,
    #[error("Port is already open")]
    PortAlreadyOpen,
    #[error("Port is destroyed")]
    PortDestroyed,
    #[error("MODBUS_ERROR_CODE_{0}")]
    ModbusErrorCode(u8),
    #[error("Not supported")]
    NotSupported,
    #[error("Illegal function")]
    IllegalFunction,
    #[error("Illegal data address")]
    IllegalDataAddress,
    #[error("Illegal data value")]
    IllegalDataValue,
    #[error("Server device failure")]
    ServerDeviceFailure,
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Invalid state: {0}")]
    InvalidState(String),
    #[error("IO error: {0}")]
    Io(std::sync::Arc<std::io::Error>),
}

impl From<std::io::Error> for ModbusError {
    fn from(e: std::io::Error) -> Self {
        ModbusError::Io(std::sync::Arc::new(e))
    }
}

impl TryFrom<u8> for ErrorCode {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(ErrorCode::IllegalFunction),
            0x02 => Ok(ErrorCode::IllegalDataAddress),
            0x03 => Ok(ErrorCode::IllegalDataValue),
            0x04 => Ok(ErrorCode::ServerDeviceFailure),
            0x05 => Ok(ErrorCode::Acknowledge),
            0x06 => Ok(ErrorCode::ServerDeviceBusy),
            0x08 => Ok(ErrorCode::MemoryParityError),
            0x0a => Ok(ErrorCode::GatewayPathUnavailable),
            0x0b => Ok(ErrorCode::GatewayTargetDeviceFailedToRespond),
            _ => Err(()),
        }
    }
}

pub fn get_error_by_code(code: ErrorCode) -> ModbusError {
    match code {
        ErrorCode::IllegalFunction => ModbusError::IllegalFunction,
        ErrorCode::IllegalDataAddress => ModbusError::IllegalDataAddress,
        ErrorCode::IllegalDataValue => ModbusError::IllegalDataValue,
        ErrorCode::ServerDeviceFailure => ModbusError::ServerDeviceFailure,
        _ => ModbusError::ModbusErrorCode(code as u8),
    }
}

pub fn get_code_by_error(err: &ModbusError) -> ErrorCode {
    match err {
        ModbusError::IllegalFunction => ErrorCode::IllegalFunction,
        ModbusError::IllegalDataAddress => ErrorCode::IllegalDataAddress,
        ModbusError::IllegalDataValue => ErrorCode::IllegalDataValue,
        ModbusError::ServerDeviceFailure => ErrorCode::ServerDeviceFailure,
        ModbusError::ModbusErrorCode(code) => {
            ErrorCode::try_from(*code).unwrap_or(ErrorCode::ServerDeviceFailure)
        }
        _ => ErrorCode::ServerDeviceFailure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_values() {
        assert_eq!(ErrorCode::IllegalFunction as u8, 0x01);
        assert_eq!(ErrorCode::IllegalDataAddress as u8, 0x02);
        assert_eq!(ErrorCode::IllegalDataValue as u8, 0x03);
        assert_eq!(ErrorCode::ServerDeviceFailure as u8, 0x04);
        assert_eq!(ErrorCode::Acknowledge as u8, 0x05);
        assert_eq!(ErrorCode::ServerDeviceBusy as u8, 0x06);
        assert_eq!(ErrorCode::MemoryParityError as u8, 0x08);
        assert_eq!(ErrorCode::GatewayPathUnavailable as u8, 0x0a);
        assert_eq!(ErrorCode::GatewayTargetDeviceFailedToRespond as u8, 0x0b);
    }

    #[test]
    fn test_get_error_by_code() {
        let err = get_error_by_code(ErrorCode::IllegalFunction);
        assert!(matches!(err, ModbusError::IllegalFunction));
    }

    #[test]
    fn test_get_code_by_error_roundtrip() {
        for code in [
            ErrorCode::IllegalFunction,
            ErrorCode::IllegalDataAddress,
            ErrorCode::IllegalDataValue,
            ErrorCode::ServerDeviceFailure,
            ErrorCode::Acknowledge,
            ErrorCode::ServerDeviceBusy,
            ErrorCode::MemoryParityError,
            ErrorCode::GatewayPathUnavailable,
            ErrorCode::GatewayTargetDeviceFailedToRespond,
        ] {
            let err = get_error_by_code(code);
            assert_eq!(get_code_by_error(&err), code);
        }
    }

    #[test]
    fn test_get_code_by_error_non_modbus() {
        let err = ModbusError::Timeout;
        assert_eq!(get_code_by_error(&err), ErrorCode::ServerDeviceFailure);
    }

    #[test]
    fn test_get_code_by_error_unknown() {
        let err = ModbusError::ModbusErrorCode(0x99);
        assert_eq!(get_code_by_error(&err), ErrorCode::ServerDeviceFailure);
    }
}
