# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [2.0.0] - 2026-05-14

### Added

- `open` / `open_by_id` 方法接受 `Option<OpenOptions>` 参数，用于配置连接超时。
- `MasterResponse` 结构体替代元组，为响应数据提供具名字段。
- `ModbusMaster` / `ModbusSlave` 新增 `is_open()`、`is_closed()`、`has_connections()` 状态查询方法。

### Changed

- **BREAKING**: `ModbusMaster::open` / `open_by_id` 和 `ModbusSlave::open` 签名改为接收 `Option<OpenOptions>`。
- **BREAKING**: `ModbusMaster` 所有读写方法的返回类型从 `(Option<FunctionCode>, Vec<u8>)` 改为 `MasterResponse`。

## [1.0.0] - 2026-05-13

### Added

- Full async/await support via tokio.
- Per-address async locks for FC22/FC23 fallback paths.
- UUID-based connection IDs.
- RTU pool-based frame extraction.
- Strict RTU mode with t3.5/t1.5 timers.
- Custom function code predictor support (`CustomFcPredict`).
- `InvalidHex` error for ASCII framing.

### Changed

- **BREAKING**: `ModbusMaster` and `ModbusSlave` constructors now take `Arc<A>` / `Arc<P>`.
- **BREAKING**: All physical layer and application layer `new()` methods now return `Arc<Self>`.
- **BREAKING**: `ModbusMaster::read_device_identification` return type changed from `HashMap<u8, String>` to `DeviceIdentification`.
- **BREAKING**: `ModbusSlaveModel::read_device_identification` return type changed from `HashMap<u8, String>` to `HashMap<u8, String>` (aligned with master).
- Deduplicated `set_role` implementation; removed redundant `closed` flag.
- Fixed memory leaks in framing task lifecycle.
- Aligned internal `_clean` level semantics with njs-modbus.
- Added `destroy` guards to prevent double-free.

## [0.1.0] - 2025-05-11

### Added

- Initial release.
- Full Modbus standard protocol implementation.
- Support for Modbus RTU, ASCII, TCP/IP, UDP/IP.
- Support for RTU/ASCII Over TCP/IP and UDP/IP.
- Master (client) implementation with all standard function codes:
  - FC01: Read Coils
  - FC02: Read Discrete Inputs
  - FC03: Read Holding Registers
  - FC04: Read Input Registers
  - FC05: Write Single Coil
  - FC06: Write Single Register
  - FC15: Write Multiple Coils
  - FC16: Write Multiple Registers
  - FC17: Report Server ID
  - FC22: Mask Write Register
  - FC23: Read/Write Multiple Registers
  - FC43/14: Read Device Identification
- Slave (server) implementation with multi-unit support.
- Custom function code interception via `ModbusSlaveModel::intercept`.
- Address range validation for slave models.
- Broadcast support (unit = 0).
- Optional serial port support via `serial` feature flag.
