use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const CRC_TABLE: [u16; 256] = [
    0x0000, 0xc0c1, 0xc181, 0x0140, 0xc301, 0x03c0, 0x0280, 0xc241, 0xc601, 0x06c0, 0x0780, 0xc741,
    0x0500, 0xc5c1, 0xc481, 0x0440, 0xcc01, 0x0cc0, 0x0d80, 0xcd41, 0x0f00, 0xcfc1, 0xce81, 0x0e40,
    0x0a00, 0xcac1, 0xcb81, 0x0b40, 0xc901, 0x09c0, 0x0880, 0xc841, 0xd801, 0x18c0, 0x1980, 0xd941,
    0x1b00, 0xdbc1, 0xda81, 0x1a40, 0x1e00, 0xdec1, 0xdf81, 0x1f40, 0xdd01, 0x1dc0, 0x1c80, 0xdc41,
    0x1400, 0xd4c1, 0xd581, 0x1540, 0xd701, 0x17c0, 0x1680, 0xd641, 0xd201, 0x12c0, 0x1380, 0xd341,
    0x1100, 0xd1c1, 0xd081, 0x1040, 0xf001, 0x30c0, 0x3180, 0xf141, 0x3300, 0xf3c1, 0xf281, 0x3240,
    0x3600, 0xf6c1, 0xf781, 0x3740, 0xf501, 0x35c0, 0x3480, 0xf441, 0x3c00, 0xfcc1, 0xfd81, 0x3d40,
    0xff01, 0x3fc0, 0x3e80, 0xfe41, 0xfa01, 0x3ac0, 0x3b80, 0xfb41, 0x3900, 0xf9c1, 0xf881, 0x3840,
    0x2800, 0xe8c1, 0xe981, 0x2940, 0xeb01, 0x2bc0, 0x2a80, 0xea41, 0xee01, 0x2ec0, 0x2f80, 0xef41,
    0x2d00, 0xedc1, 0xec81, 0x2c40, 0xe401, 0x24c0, 0x2580, 0xe541, 0x2700, 0xe7c1, 0xe681, 0x2640,
    0x2200, 0xe2c1, 0xe381, 0x2340, 0xe101, 0x21c0, 0x2080, 0xe041, 0xa001, 0x60c0, 0x6180, 0xa141,
    0x6300, 0xa3c1, 0xa281, 0x6240, 0x6600, 0xa6c1, 0xa781, 0x6740, 0xa501, 0x65c0, 0x6480, 0xa441,
    0x6c00, 0xacc1, 0xad81, 0x6d40, 0xaf01, 0x6fc0, 0x6e80, 0xae41, 0xaa01, 0x6ac0, 0x6b80, 0xab41,
    0x6900, 0xa9c1, 0xa881, 0x6840, 0x7800, 0xb8c1, 0xb981, 0x7940, 0xbb01, 0x7bc0, 0x7a80, 0xba41,
    0xbe01, 0x7ec0, 0x7f80, 0xbf41, 0x7d00, 0xbdc1, 0xbc81, 0x7c40, 0xb401, 0x74c0, 0x7580, 0xb541,
    0x7700, 0xb7c1, 0xb681, 0x7640, 0x7200, 0xb2c1, 0xb381, 0x7340, 0xb101, 0x71c0, 0x7080, 0xb041,
    0x5000, 0x90c1, 0x9181, 0x5140, 0x9301, 0x53c0, 0x5280, 0x9241, 0x9601, 0x56c0, 0x5780, 0x9741,
    0x5500, 0x95c1, 0x9481, 0x5440, 0x9c01, 0x5cc0, 0x5d80, 0x9d41, 0x5f00, 0x9fc1, 0x9e81, 0x5e40,
    0x5a00, 0x9ac1, 0x9b81, 0x5b40, 0x9901, 0x59c0, 0x5880, 0x9841, 0x8801, 0x48c0, 0x4980, 0x8941,
    0x4b00, 0x8bc1, 0x8a81, 0x4a40, 0x4e00, 0x8ec1, 0x8f81, 0x4f40, 0x8d01, 0x4dc0, 0x4c80, 0x8c41,
    0x4400, 0x84c1, 0x8581, 0x4540, 0x8701, 0x47c0, 0x4680, 0x8641, 0x8201, 0x42c0, 0x4380, 0x8341,
    0x4100, 0x81c1, 0x8081, 0x4040,
];

pub fn crc(data: &[u8]) -> u16 {
    crc_with_seed(data, 0xffff)
}

/// Update an in-progress CRC by feeding `data`. Pass `0xffff` for a fresh
/// computation, or the previous result to extend a running CRC by additional
/// bytes (mirrors njs-modbus `crc(data, seed)`).
pub fn crc_with_seed(data: &[u8], seed: u16) -> u16 {
    let mut crc = seed;
    for &byte in data {
        crc = CRC_TABLE[((crc ^ byte as u16) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

pub fn lrc(data: &[u8]) -> u8 {
    let sum: u8 = data.iter().copied().fold(0u8, |a, b| a.wrapping_add(b));
    (0u8).wrapping_sub(sum)
}

fn in_range(n: u16, (min, max): (u16, u16)) -> bool {
    n >= min && n <= max
}

pub fn check_range(value: &[u16], range: &[(u16, u16)]) -> bool {
    if range.is_empty() {
        return true;
    }
    for &(min, max) in range {
        if min <= max && value.iter().all(|&v| in_range(v, (min, max))) {
            return true;
        }
    }
    false
}

pub fn get_three_point_five_t(baud_rate: u32, approximation: u32) -> f64 {
    (approximation as f64 * 1000.0) / baud_rate as f64
}

pub fn pack_coils(coils: &[bool], length: u16) -> Vec<u8> {
    let byte_count = ((length + 7) / 8) as usize;
    let mut data = Vec::with_capacity(1 + byte_count);
    data.push(byte_count as u8);
    for chunk in coils.chunks(8) {
        let mut byte = 0u8;
        for (bit_idx, &v) in chunk.iter().enumerate() {
            if v {
                byte |= 1 << bit_idx;
            }
        }
        data.push(byte);
    }
    data
}

pub fn parse_coils(data: &[u8], length: u16) -> Vec<bool> {
    let mut result = Vec::with_capacity(length as usize);
    for byte_idx in 0..((length + 7) / 8) as usize {
        let byte = data[1 + byte_idx];
        for bit_idx in 0..8 {
            let i = byte_idx * 8 + bit_idx;
            if i >= length as usize {
                break;
            }
            result.push((byte >> bit_idx) & 1 == 1);
        }
    }
    result
}

pub fn pack_registers(registers: &[u16], length: u16) -> Vec<u8> {
    let byte_count = (length * 2) as usize;
    let mut data = Vec::with_capacity(1 + byte_count);
    data.push(byte_count as u8);
    for reg in registers {
        data.extend_from_slice(&reg.to_be_bytes());
    }
    data
}

pub fn parse_registers(data: &[u8], length: u16) -> Vec<u16> {
    let mut result = Vec::with_capacity(length as usize);
    for i in 0..length {
        let idx = 1 + (i as usize) * 2;
        result.push(u16::from_be_bytes([data[idx], data[idx + 1]]));
    }
    result
}

/// Predict the total RTU frame length (PDU + 2-byte CRC) given the leading bytes.
///
/// Returns `None` when:
/// - The buffer is too short to identify the function code or read a byteCount.
/// - The function code is unknown or has a variable-length response that cannot
///   be predicted from leading bytes (FC 43/14 responses).
///
/// Callers must fall back to a sliding-window CRC scan when `None` is returned.
pub fn predict_rtu_frame_length(buffer: &[u8], is_response: bool) -> Option<usize> {
    if buffer.len() < 2 {
        return None;
    }
    let fc = buffer[1];

    if is_response && (fc & 0x80) != 0 {
        return Some(5);
    }

    let fixed = if is_response {
        match fc {
            0x05 | 0x06 | 0x0f | 0x10 => Some(8usize),
            0x16 => Some(10),
            _ => None,
        }
    } else {
        match fc {
            0x01..=0x06 => Some(8usize),
            0x11 => Some(4),
            0x16 => Some(10),
            0x2b => Some(7),
            _ => None,
        }
    };
    if let Some(n) = fixed {
        return Some(n);
    }

    let (offset, extra) = if is_response {
        match fc {
            0x01 | 0x02 | 0x03 | 0x04 | 0x11 | 0x17 => (2usize, 5usize),
            _ => return None,
        }
    } else {
        match fc {
            0x0f | 0x10 => (6usize, 9usize),
            0x17 => (10, 13),
            _ => return None,
        }
    };
    if buffer.len() <= offset {
        return None;
    }
    Some(extra + buffer[offset] as usize)
}

static CONNECTION_ID_SEQ: AtomicU64 = AtomicU64::new(0);

/// Generate a process-unique connection id with the given prefix.
///
/// Format: `{prefix}-{nanos_since_epoch}-{seq}` where seq is a monotonically
/// increasing per-process counter. Used by physical layer implementations to
/// identify individual sockets / serial ports.
pub fn gen_connection_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = CONNECTION_ID_SEQ
        .fetch_add(1, Ordering::Relaxed)
        .wrapping_add(1);
    format!("{prefix}-{nanos}-{seq}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc_known_values() {
        // Test with empty data
        assert_eq!(crc(&[]), 0xffff);
        // Test with single byte: TABLE[0xFE] ^ 0x00FF = 0x8081 ^ 0x00FF = 0x807E
        assert_eq!(crc(&[0x01]), 0x807e);
        // Test self-consistency: CRC of [data + crc(data)] should match
        let data = [0x01, 0x03, 0x00, 0x00, 0x00, 0x0a];
        let result = crc(&data);
        let frame_crc = result.to_le_bytes();
        let mut full_frame = data.to_vec();
        full_frame.extend_from_slice(&frame_crc);
        assert_eq!(crc(&full_frame[..full_frame.len() - 2]), result);
    }

    #[test]
    fn test_crc_with_seed_chains_equivalently() {
        // Splitting `crc(a ++ b)` into `crc_with_seed(b, crc(a))` must yield
        // the identical result — this is the invariant the sliding-window
        // running CRC in rtu.rs relies on.
        let data = [0x01u8, 0x03, 0x00, 0x00, 0x00, 0x0a, 0x77, 0xff];
        let full = crc(&data);
        for split in 0..=data.len() {
            let (left, right) = data.split_at(split);
            let chained = crc_with_seed(right, crc(left));
            assert_eq!(chained, full, "split={split}");
        }
        // crc() must equal crc_with_seed(.., 0xffff).
        assert_eq!(crc(&data), crc_with_seed(&data, 0xffff));
    }

    #[test]
    fn test_lrc_known_values() {
        // Empty data
        assert_eq!(lrc(&[]), 0x00);
        // Single byte: ~0x01 + 1 = 0xFF
        assert_eq!(lrc(&[0x01]), 0xff);
        // Two bytes: (0x01 + 0x03) = 0x04, ~0x04 + 1 = 0xFC
        assert_eq!(lrc(&[0x01, 0x03]), 0xfc);
    }

    #[test]
    fn test_check_range_single() {
        assert!(check_range(&[5], &[(0, 10)]));
        assert!(!check_range(&[15], &[(0, 10)]));
        assert!(check_range(&[5, 8], &[(0, 10)]));
        assert!(!check_range(&[5, 15], &[(0, 10)]));
    }

    #[test]
    fn test_check_range_multiple() {
        // Multiple ranges - any match is ok
        assert!(check_range(&[5], &[(0, 3), (4, 10)]));
        assert!(!check_range(&[5], &[(0, 3), (6, 10)]));
    }

    #[test]
    fn test_check_range_empty() {
        // Empty range means no restriction
        assert!(check_range(&[999], &[]));
    }

    #[test]
    fn test_check_range_invalid() {
        // min > max is invalid, skip
        assert!(!check_range(&[5], &[(10, 0)]));
    }

    #[test]
    fn test_three_point_five_t() {
        assert_eq!(get_three_point_five_t(9600, 48), 5.0);
        assert_eq!(get_three_point_five_t(19200, 48), 2.5);
    }

    #[test]
    fn test_parse_coils() {
        // 3 coils: true, false, true
        let data = vec![0x01, 0b00000101];
        let coils = parse_coils(&data, 3);
        assert_eq!(coils, vec![true, false, true]);

        // 8 coils: all true
        let data = vec![0x01, 0xFF];
        let coils = parse_coils(&data, 8);
        assert_eq!(coils, vec![true; 8]);

        // 10 coils
        let data = vec![0x02, 0b00000101, 0b00000011];
        let coils = parse_coils(&data, 10);
        assert_eq!(
            coils,
            vec![true, false, true, false, false, false, false, false, true, true]
        );
    }

    #[test]
    fn test_pack_coils() {
        let coils = vec![
            true, false, true, false, false, false, false, false, true, true,
        ];
        let packed = pack_coils(&coils, 10);
        assert_eq!(packed, vec![0x02, 0b00000101, 0b00000011]);
    }

    #[test]
    fn test_pack_and_parse_coils_roundtrip() {
        let coils = vec![true, false, true, true, false, true, false, true];
        let packed = pack_coils(&coils, 8);
        let parsed = parse_coils(&packed, 8);
        assert_eq!(coils, parsed);
    }

    #[test]
    fn test_parse_registers() {
        let data = vec![0x04, 0x00, 0x01, 0xAB, 0xCD];
        let regs = parse_registers(&data, 2);
        assert_eq!(regs, vec![0x0001, 0xABCD]);
    }

    #[test]
    fn test_pack_registers() {
        let regs = vec![0x0001u16, 0xABCD];
        let packed = pack_registers(&regs, 2);
        assert_eq!(packed, vec![0x04, 0x00, 0x01, 0xAB, 0xCD]);
    }

    #[test]
    fn test_pack_and_parse_registers_roundtrip() {
        let regs = vec![0x1234u16, 0x5678, 0x9ABC];
        let packed = pack_registers(&regs, 3);
        let parsed = parse_registers(&packed, 3);
        assert_eq!(regs, parsed);
    }

    // ===== predict_rtu_frame_length =====

    #[test]
    fn test_predict_buffer_too_short_returns_none() {
        assert_eq!(predict_rtu_frame_length(&[], false), None);
        assert_eq!(predict_rtu_frame_length(&[0x01], false), None);
    }

    #[test]
    fn test_predict_request_fc1_2_3_4_5_6_fixed_8() {
        for fc in [0x01u8, 0x02, 0x03, 0x04, 0x05, 0x06] {
            assert_eq!(
                predict_rtu_frame_length(&[0x01, fc], false),
                Some(8),
                "request fc=0x{:02x} should predict 8",
                fc
            );
        }
    }

    #[test]
    fn test_predict_request_fc17_fixed_4() {
        assert_eq!(predict_rtu_frame_length(&[0x01, 0x11], false), Some(4));
    }

    #[test]
    fn test_predict_request_fc22_fixed_10() {
        assert_eq!(predict_rtu_frame_length(&[0x01, 0x16], false), Some(10));
    }

    #[test]
    fn test_predict_request_fc43_fixed_7() {
        assert_eq!(predict_rtu_frame_length(&[0x01, 0x2b], false), Some(7));
    }

    #[test]
    fn test_predict_request_fc15_byte_count() {
        // fc=0x0f request: offset=6 (byteCount), extra=9
        // bytes: unit fc addr1 addr2 qty1 qty2 byteCount ... + 2 CRC = 9 + byteCount
        let buf = [0x01u8, 0x0f, 0x00, 0x00, 0x00, 0x0a, 0x02];
        assert_eq!(predict_rtu_frame_length(&buf, false), Some(11));
    }

    #[test]
    fn test_predict_request_fc16_byte_count() {
        // fc=0x10 request: offset=6, extra=9
        let buf = [0x01u8, 0x10, 0x00, 0x00, 0x00, 0x02, 0x04];
        assert_eq!(predict_rtu_frame_length(&buf, false), Some(13));
    }

    #[test]
    fn test_predict_request_fc23_byte_count() {
        // fc=0x17 request: offset=10, extra=13
        let buf = [
            0x01u8, 0x17, 0x00, 0x00, 0x00, 0x02, 0x00, 0x02, 0x00, 0x01, 0x02,
        ];
        assert_eq!(predict_rtu_frame_length(&buf, false), Some(15));
    }

    #[test]
    fn test_predict_request_byte_count_buffer_too_short() {
        // fc=0x0f offset=6, buffer length=6 → cannot read byteCount
        let buf = [0x01u8, 0x0f, 0x00, 0x00, 0x00, 0x0a];
        assert_eq!(predict_rtu_frame_length(&buf, false), None);
    }

    #[test]
    fn test_predict_response_exception_returns_5() {
        // Any response with fc & 0x80 = exception, length is 5 (unit + fc + code + crc[2])
        assert_eq!(predict_rtu_frame_length(&[0x01, 0x83], true), Some(5));
        assert_eq!(predict_rtu_frame_length(&[0x01, 0x90], true), Some(5));
        assert_eq!(predict_rtu_frame_length(&[0x01, 0xab], true), Some(5));
    }

    #[test]
    fn test_predict_response_fc5_6_15_16_fixed_8() {
        for fc in [0x05u8, 0x06, 0x0f, 0x10] {
            assert_eq!(
                predict_rtu_frame_length(&[0x01, fc], true),
                Some(8),
                "response fc=0x{:02x} should predict 8",
                fc
            );
        }
    }

    #[test]
    fn test_predict_response_fc22_fixed_10() {
        assert_eq!(predict_rtu_frame_length(&[0x01, 0x16], true), Some(10));
    }

    #[test]
    fn test_predict_response_fc1_2_3_4_byte_count() {
        // response offset=2, extra=5
        // unit fc byteCount data... crc[2] = 5 + byteCount
        for fc in [0x01u8, 0x02, 0x03, 0x04] {
            let buf = [0x01u8, fc, 0x04, 0x00, 0x00, 0x00, 0x00];
            assert_eq!(
                predict_rtu_frame_length(&buf, true),
                Some(9),
                "response fc=0x{:02x} byte_count=4 should predict 9",
                fc
            );
        }
    }

    #[test]
    fn test_predict_response_fc17_byte_count() {
        let buf = [0x01u8, 0x11, 0x03];
        assert_eq!(predict_rtu_frame_length(&buf, true), Some(8));
    }

    #[test]
    fn test_predict_response_fc23_byte_count() {
        let buf = [0x01u8, 0x17, 0x04];
        assert_eq!(predict_rtu_frame_length(&buf, true), Some(9));
    }

    #[test]
    fn test_predict_response_byte_count_buffer_too_short() {
        // response fc=0x03 offset=2, buffer length=2 → no byteCount yet
        let buf = [0x01u8, 0x03];
        assert_eq!(predict_rtu_frame_length(&buf, true), None);
    }

    #[test]
    fn test_predict_unknown_fc_returns_none() {
        // Unknown function codes should return None (caller falls back to sliding scan)
        assert_eq!(predict_rtu_frame_length(&[0x01, 0x99], false), None);
        // FC43 response is variable-length and not predictable from leading bytes
        assert_eq!(predict_rtu_frame_length(&[0x01, 0x2b], true), None);
    }

    #[test]
    fn test_predict_request_exception_path_not_taken() {
        // In a request, fc with high bit set is NOT treated as exception
        // (only responses use that path)
        assert_eq!(predict_rtu_frame_length(&[0x01, 0x83], false), None);
    }

    // ===== gen_connection_id =====

    #[test]
    fn test_gen_connection_id_has_prefix() {
        let id = gen_connection_id("serial");
        assert!(id.starts_with("serial-"), "got {}", id);
    }

    #[test]
    fn test_gen_connection_id_is_unique() {
        let a = gen_connection_id("tcp-server");
        let b = gen_connection_id("tcp-server");
        assert_ne!(a, b);
    }

    #[test]
    fn test_gen_connection_id_different_prefixes() {
        let a = gen_connection_id("a");
        let b = gen_connection_id("b");
        assert!(a.starts_with("a-"));
        assert!(b.starts_with("b-"));
    }
}
