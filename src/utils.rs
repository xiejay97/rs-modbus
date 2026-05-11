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
    let mut crc = 0xffffu16;
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
    for (_byte_idx, chunk) in coils.chunks(8).enumerate() {
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
        assert_eq!(coils, vec![true, false, true, false, false, false, false, false, true, true]);
    }

    #[test]
    fn test_pack_coils() {
        let coils = vec![true, false, true, false, false, false, false, false, true, true];
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
}
