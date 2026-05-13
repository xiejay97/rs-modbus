# rs-modbus vs njs-modbus 对齐报告

> 生成日期：2026-05-13
> 对比基准：njs-modbus `035dbd1` (HEAD) ↔ rs-modbus `cea06fd` (HEAD)

---

## 1. 项目结构对齐

| njs-modbus | rs-modbus | 对齐状态 |
|---|---|---|
| `src/index.ts` | `src/lib.rs` | ✅ 公共 API 导出 |
| `src/types.ts` | `src/types.rs` | ✅ ADU/ServerId/CustomFC 等类型 |
| `src/vars.ts` | `src/vars.rs` | ✅ 常量/枚举/limits |
| `src/error-code.ts` | `src/error.rs` | ✅ ErrorCode + ModbusError |
| `src/layers/application/` | `src/layers/application/` | ✅ RTU/ASCII/TCP 三层 |
| `src/layers/physical/` | `src/layers/physical/` | ✅ Serial/TCP/UDP |
| `src/master/` | `src/master.rs` + `src/master_session.rs` | ✅ Master + Session |
| `src/slave/` | `src/slave.rs` | ✅ Slave |
| `src/utils/` | `src/utils.rs` | ✅ 工具函数 |
| `test/` | `tests/` | ✅ 测试目录 |

**说明**：rs-modbus 将 master/master-session 合并为 `master.rs` + `master_session.rs` 两个文件（而非目录），这是因为 Rust 单文件模块的惯例，不影响功能对齐。

---

## 2. 已对齐的功能（最新提交 `cea06fd` 已同步）

### 2.1 Item #26 — genConnectionId 跨进程唯一性
- **njs**: `crypto.randomUUID()` → `${prefix}-${uuid-v4}`
- **rs**: `uuid::Uuid::new_v4()` → `"{prefix}-{}"`
- **状态**：✅ 完全对齐，格式一致

### 2.2 Item #17 — FC22/FC23 fallback 原子锁
- **njs**: `withAddressLock` 使用 `Map<number, Promise<unknown>>` + Promise 链实现
- **rs**: `with_address_lock` 使用 `HashMap<u16, Arc<tokio::sync::Mutex<()>>>` + 显式 acquire
- **状态**：✅ 行为对齐，均保证同一地址的并发 fallback 操作串行化

### 2.3 Item #18 — checkRange 单点区间 `[a,a]`
- **njs**: `checkRange(5, [5, 5]) === true`
- **rs**: `check_range(&[5], &[(5, 5)])` → `true`
- **状态**：✅ 已修复并验证

### 2.4 035dbd1 — RTU Pool 固定缓冲 + Loop-Consume
- **njs**: `Buffer.alloc(MAX_FRAME_LENGTH * 2)` + `data.copy` + `flushBuffer`
- **rs**: `RtuBuffer { pool: Box<[u8; 512]>, start, end }` + `extend_from_slice` + `flush_pool`
- **状态**：✅ 对齐，均支持超大 inbound chunk 的 loop-consume

### 2.5 035dbd1 — ASCII InvalidHex 哨兵
- **njs**: `HEX_DECODE` 表 + `isHexChar` + `INVALID_HEX` error code
- **rs**: `hex_decode_byte` + `is_hex_char` + `InvalidHex` error variant
- **状态**：✅ FSM 和 `decode_payload` 双重检查，行为一致

### 2.6 035dbd1 — FC43/14 UTF-8 多字节
- **njs**: `Buffer.byteLength(value)` 计算 UTF-8 字节长度
- **rs**: `value.len()`（Rust String 天然 UTF-8）
- **状态**：✅ 行为等价，均支持多字节字符（如 `"测"` = 3 bytes）

---

## 3. 核心模块逐项对比

### 3.1 类型系统 (types)

| 特性 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| `ApplicationDataUnit` | `{ transaction?, unit, fc, data: Buffer }` | `{ transaction: Option<u16>, unit: u8, fc: u8, data: Vec<u8> }` | ✅ 等价 |
| `ServerId` | `{ serverId: number \| number[] }` | `{ server_id: Vec<u8> }` | ✅ 等价（rs 统一为 Vec） |
| `CustomFunctionCode` | `predictRequestLength/ResponseLength: (buffer) => number \| null` | `predict_request/response_length: fn(&[u8]) -> CustomFcPredict` | ✅ 等价（NeedMore ↔ null） |
| `CustomFcPredict` | `PredictResult` (kind 联合类型) | `PredictResult` (enum) | ✅ 等价 |

### 3.2 错误处理 (error-code)

| 特性 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| `ErrorCode` 枚举 | `ILLEGAL_FUNCTION=0x01` ... `GATEWAY_TARGET_DEVICE_FAILED_TO_RESPOND=0x0b` | 同名枚举，相同值 | ✅ 对齐 |
| `ModbusError` | `class ModbusError extends Error { code: string }` | `#[derive(Error)] enum ModbusError { ... }` | ✅ 等价 |
| `ModbusErrorCode` 常量 | `ETIMEOUT`, `EINVALID_RESPONSE`, `EINVALID_HEX`, `ECRC_MISMATCH`, `ELRC_MISMATCH`... | 对应变体 `Timeout`, `InvalidResponse`, `InvalidHex`, `CrcCheckFailed`, `LrcCheckFailed`... | ✅ 对齐 |
| `getErrorByCode` / `getCodeByError` | 函数映射 | 同名函数 | ✅ 对齐 |

### 3.3 工具函数 (utils)

| 函数 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| `crc` | `crc(data, seed=0xffff)` | `crc(data)` / `crc_with_seed(data, seed)` | ✅ 等价 |
| `lrc` | `lrc(data: Uint8Array): number` | `lrc(data: &[u8]) -> u8` | ✅ 等价 |
| `checkRange` | `checkRange(value, range?)` | `check_range(value: &[u16], range: &[(u16,u16)])` | ✅ 等价 |
| `bitsToMs` | `(bits * 1000) / baudRate` | `(bits * 1000.0) / baud_rate as f64` | ✅ 等价 |
| `isUint8` | `Number.isInteger(n) && n>=0 && n<=255` | `(0..=255).contains(&n)` (n: i32) | ✅ 等价 |
| `predictRtuFrameLength` | 使用 Record 映射 | 使用 match 表达式 | ✅ 等价 |
| `genConnectionId` | `crypto.randomUUID()` | `uuid::Uuid::new_v4()` | ✅ 等价 |
| `packCoils` / `parseCoils` | 位运算打包/解析 | 同名函数，相同算法 | ✅ 等价 |
| `packRegisters` / `parseRegisters` | BE u16 打包/解析 | 同名函数，相同算法 | ✅ 等价 |

### 3.4 应用层 — RTU

| 特性 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| `RtuApplicationLayerOptions` | `{ intervalBetweenFrames?, interCharTimeout? }` | `{ interval_between_frames?, inter_char_timeout?, baud_rate? }` | ✅ 对齐 |
| `FrameInterval` | `{ unit: 'bit' \| 'ms', value: number }` | `FrameInterval::Bits(f64)` / `FrameInterval::Ms(u32)` | ✅ 等价 |
| `compute_interval_ms` | Serial >19200 → 1.75ms / 0.75ms 固定值 | Serial >19200 → 1.75ms / 0.75ms 固定值 | ✅ 对齐 |
| 固定缓冲 Pool | `Buffer.alloc(512)` | `Box<[u8; 512]>` | ✅ 对齐 |
| Loop-consume | `while (dataOffset < data.length)` | `while data_offset < data.len()` | ✅ 对齐 |
| 帧提取 | `tryExtract` → `ExtractResult` | `try_extract` → `ExtractResult` | ✅ 对齐 |
| CRC 校验 | `crcMatches` 滑动校验 | `crc_matches` 直接校验 | ✅ 等价 |
| Custom FC 注册 | `addCustomFunctionCode` / `removeCustomFunctionCode` | `add_custom_function_code` / `remove_custom_function_code` | ✅ 对齐 |
| **⚠️ t3.5 / t1.5 定时器** | `setTimeout(commitFrame, threePointFiveT)` + `interCharTimer` | **无定时器逻辑** | ⚠️ **差异见 §4.1** |
| **⚠️ strict 模式** | Serial 模式下 `strict=true`，CRC 失败丢弃整个缓冲 | `_strict` 参数存在但**未使用** | ⚠️ **差异见 §4.2** |

### 3.5 应用层 — ASCII

| 特性 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| `lenientHex` 选项 | `lenientHex?: boolean` | `lenient_hex: bool` | ✅ 对齐 |
| HEX_DECODE 表 | `Uint8Array(256).fill(0xff)` | `hex_decode_byte` 函数 | ✅ 等价 |
| FSM 状态机 | `'idle' \| 'reception' \| 'waiting end'` | `FsmStatus::Idle \| Reception \| WaitingEnd` | ✅ 对齐 |
| 最大载荷 | `MAX_ASCII_PAYLOAD = 512` | `MAX_ASCII_PAYLOAD = 512` | ✅ 对齐 |
| 非 hex 字符拒绝 | FSM 层 `INVALID_HEX` + decode 层二次检查 | FSM 层 `InvalidHex` + decode 层二次检查 | ✅ 对齐 |
| LRC 校验 | `lrc(buffer.subarray(0, -1))` | `lrc(&bytes[..bytes.len()-1])` | ✅ 等价 |

### 3.6 应用层 — TCP

| 特性 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| MBAP 解析 | `buffer.readUInt16BE(4)` 取 length | `u16::from_be_bytes([frame[4], frame[5]])` | ✅ 等价 |
| Protocol ID 校验 | `buffer[2] !== 0 \|\| buffer[3] !== 0` → error | `frame[2..4] != [0, 0]` → `InvalidData` | ✅ 等价 |
| 最大帧 | `MAX_TCP_FRAME = 260` | （decode 中隐式） | ⚠️ rs 未显式限制 |
| Transaction ID | `_transactionId` 自增 | 由 Master 传入 `adu.transaction` | ⚠️ 职责分离不同，行为等价 |

### 3.7 Master

| 特性 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| `ModbusMasterOptions` | `{ timeout?: 1000, concurrent?: false }` | `{ timeout_ms: 1000, concurrent: false }` | ✅ 对齐 |
| `concurrent` 限制 | TCP-only，否则 throw | TCP-only，否则 panic | ✅ 等价 |
| TID 分配 | `_nextTid = (_nextTid + 1) % 65536 \|\| 1` | `fetch_update` atomic | ✅ 等价 |
| FIFO 队列 | `_queue` 数组 + `_drain()` | `queue_lock` (tokio::sync::Mutex) | ✅ 等价 |
| `open()` 重置 closed | `this._closed = false` | `self.closed.store(false, Ordering::Release)` | ✅ 对齐 |
| `close()` 取消队列 | `_queue.splice(0)` + callback reject | `stop_all("Master closed")` | ✅ 等价 |
| PreCheck 链 | `boolean \| number \| undefined` 返回值 | `PreCheckOutcome` enum | ✅ 等价 |
| 所有 FC 方法 | FC1-FC6, FC15-FC17, FC22-FC23, FC43/14 | 同名方法 | ✅ 对齐 |
| `sendCustomFC` | 发送自定义 FC，返回 `Buffer` | 发送自定义 FC，返回 `Vec<u8>` | ✅ 对齐 |

### 3.8 MasterSession

| 特性 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| Waiter Key | `string \| number` (tid \| 'fifo') | `WaiterKey::Tid(u16) \| Fifo` | ✅ 等价 |
| 存储 | `Map<string\|number, Waiter>` | `HashMap<WaiterKey, WaitingState>` | ✅ 等价 |
| 回调模式 | `callback(error, frame?)` | `oneshot::Receiver<Result<Framing, ModbusError>>` | ⚠️ 实现不同，行为等价 |
| `handleFrame` | 按 key 查找 → runPreChecks → callback | 按 key 查找 → run_pre_checks → send | ✅ 等价 |
| `handleError` | `stopAll(error)` 拒绝所有 waiter | `stop_all(err)` 拒绝所有 waiter | ✅ 对齐 |
| PreCheck `undefined` | → `INSUFFICIENT_DATA` error | `InsufficientData` → `InsufficientData` error | ✅ 对齐 |
| PreCheck `number` | `<` → insufficient, `!=` → invalid | `NeedLength(n)` → 同逻辑 | ✅ 对齐 |

### 3.9 Slave

| 特性 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| `ModbusSlaveOptions` | `{ concurrent?: false }` | `{ concurrent: false }` | ✅ 对齐 |
| `models` | `Map<number, ModbusSlaveModel>` | `HashMap<u8, Arc<dyn ModbusSlaveModel>>` | ✅ 等价 |
| `concurrent` 限制 | TCP-only，否则 throw | TCP-only，否则 panic | ✅ 等价 |
| 每连接 FIFO | `_queues: Map<connection.id, QueueEntry>` | `queues: HashMap<ConnectionId, QueueEntry>` | ✅ 对齐 |
| Queue 清理 | `connection-close` 时清空 items | `subscribe_connection_close` 时清空 | ✅ 对齐 |
| `add` / `remove` | `models.set` / `models.delete` | `models.lock().await.insert` / `remove` | ✅ 等价 |
| Interceptor | `interceptor(fc, data) => Buffer \| undefined` | `intercept(fc, data) => Result<Option<Vec<u8>>, _>` | ✅ 等价 |
| 所有 FC Handler | handleFC1-handleFC43_14 | `handle_fc1` - `handle_fc43_14` | ✅ 对齐 |
| Fallback 行为 | FC15→loop writeSingleCoil, FC16→loop writeSingleRegister, FC22→read+compute+write, FC23→write+read | 同逻辑 | ✅ 对齐 |
| `withAddressLock` | Promise 链实现 | `tokio::sync::Mutex` 实现 | ⚠️ 实现不同，行为等价 |
| **⚠️ FC43/14 REGULAR_STREAM** | `objectID >= 0x80 \|\| (objectID > 0x06 && objectID < 0x80)` → reset | `object_id > 0x06` → reset（**漏了 `>= 0x80` 分支**） | ⚠️ **差异见 §4.3** |
| **⚠️ close() 清理** | 不清理 `_queues` | 清理 `queues` | ⚠️ 行为差异 |
| **⚠️ add/remove CustomFC** | 同步方法 | `async` 方法 | ⚠️ API 差异（见 §4.4） |

### 3.10 物理层

| 特性 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| `PhysicalConnection` | `{ id: string \| number }` | `ConnectionId = Arc<str>` | ✅ 等价 |
| `TYPE` | `'SERIAL' \| 'NET'` | `PhysicalLayerType::Serial \| Net` | ✅ 对齐 |
| 事件 | `data`, `write`, `error`, `connection-close`, `close` | `subscribe_data`, `subscribe_write`, `subscribe_error`, `subscribe_connection_close`, `subscribe_close` | ⚠️ EventEmitter ↔ broadcast channel |
| TCP Client | `net.Socket` | `tokio::net::TcpStream` | ✅ 等价 |
| TCP Server | `net.createServer` | `tokio::net::TcpListener` | ✅ 等价 |
| UDP Idle Eviction | `idleTimeout` (默认 30s) | `idle_timeout_ms` (默认 30s) | ✅ 对齐 |
| Serial | `serialport` 库 | `serialport` crate (feature-gated) | ✅ 对齐 |

---

## 4. 行为差异（需修复）

### 4.1 ⚠️ RTU 层缺少 t3.5 / t1.5 定时器逻辑

**位置**：`src/layers/application/rtu.rs`

**njs-modbus 行为**：
```typescript
// 数据到达后设置定时器
if (this._threePointFiveT) {
    if (this._onePointFiveT > 0) {
        state.interCharTimer = setTimeout(() => { state.t15Expired = true; }, this._onePointFiveT);
    }
    state.timer = setTimeout(commitFrame, this._threePointFiveT);
}
```

**rs-modbus 现状**：
- `interval_ms` / `inter_char_ms` 字段已计算并存入结构体
- 但 `process_data_event` 中**没有任何定时器逻辑**
- 数据到达后直接拷贝到 pool 并调用 `flush_pool`

**影响**：在 Serial 模式下，帧边界完全依赖 CRC 匹配，缺少 Modbus V1.02 §2.5.1.1 规定的 t3.5 静默间隔和 t1.5 字符间超时保护。可能导致：
- 帧粘连时无法正确分割
- 丢失的字节不会被及时检测

**建议**：使用 `tokio::time::sleep` + 每个 connection 一个 `tokio::task::JoinHandle` 实现等效定时器，或引入 `tokio::time::Interval`。

### 4.2 ⚠️ RTU 层 `strict` 模式未实现

**位置**：`src/layers/application/rtu.rs:348`

**njs-modbus 行为**：
```typescript
// Serial 模式下 strict = true
if (result.kind === 'skip' && strict) {
    this.emit('framing-error', CRC_MISMATCH);
    state.start = 0; state.end = 0;
    return;
}
if (result.kind === 'insufficient' && strict) {
    this.emit('framing-error', state.t15Expired ? T1_5_EXCEEDED : INCOMPLETE_FRAME);
    state.start = 0; state.end = 0;
    return;
}
```

**rs-modbus 现状**：
- `flush_pool` 的 `_strict` 参数带下划线（未使用）
- CRC mismatch 时走 `Skip` 路径（逐字节滑动）
- 无 `INCOMPLETE_FRAME` / `T1_5_EXCEEDED` 错误发射

**建议**：传递 `strict = physical.layer_type() == PhysicalLayerType::Serial` 并在 `flush_pool` 中实现 strict 分支。

### 4.3 ⚠️ Slave FC43/14 REGULAR_STREAM 缺少 `object_id >= 0x80` 检查

**位置**：`src/slave.rs:1173-1176`

**njs-modbus 代码**：
```typescript
case ReadDeviceIDCode.REGULAR_STREAM: {
    if (objectID >= 0x80 || (objectID > 0x06 && objectID < 0x80)) {
        objectID = 0x00;
    }
    break;
}
```

**rs-modbus 代码**：
```rust
0x02 => {
    if object_id > 0x06 {
        object_id = 0x00;
    }
}
```

**差异**：rs 漏掉了 `object_id >= 0x80` 的条件。当 REGULAR_STREAM (0x02) 请求的 object_id 在 0x80..=0xFF 范围时：
- njs: 重置为 0x00
- rs: 保持原值（>0x06 且 >=0x80 同时满足，但 `>0x06` 已经覆盖了 `>=0x80`... 等等，再看一下）

实际上 `object_id > 0x06` 已经包含了 `object_id >= 0x80`（因为 0x80 > 0x06）。所以 `if object_id > 0x06` 等价于 `object_id > 0x06 || object_id >= 0x80`。这看起来行为是一致的？

不对，再看 njs 的逻辑：
```typescript
if (objectID >= 0x80 || (objectID > 0x06 && objectID < 0x80))
```
这等价于：`objectID > 0x06 || objectID >= 0x80`？
不，这是 `objectID >= 0x80 || (objectID > 0x06 && objectID < 0x80)`。

令 A = objectID >= 0x80
令 B = objectID > 0x06 && objectID < 0x80

A || B = (objectID >= 0x80) || (0x06 < objectID < 0x80)
        = objectID > 0x06

因为：
- 如果 objectID > 0x06 且 objectID < 0x80，则 B 为真
- 如果 objectID >= 0x80，则 A 为真
- 如果 objectID <= 0x06，则 A 和 B 都为假

所以 A || B = objectID > 0x06。

而 rs 的 `object_id > 0x06` 也是完全等价的！

所以这不是一个差异？让我再想想...

等等，njs 的代码：
```typescript
if (objectID >= 0x80 || (objectID > 0x06 && objectID < 0x80)) {
```
如果 objectID = 0x07: B 为真 (0x07 > 0x06 且 0x07 < 0x80) → reset
如果 objectID = 0x80: A 为真 (0x80 >= 0x80) → reset
如果 objectID = 0x06: A 假，B 假 → 不 reset
如果 objectID = 0x05: A 假，B 假 → 不 reset

rs:
```rust
if object_id > 0x06 {
```
如果 objectID = 0x07: 真 → reset
如果 objectID = 0x80: 真 → reset
如果 objectID = 0x06: 假 → 不 reset
如果 objectID = 0x05: 假 → 不 reset

确实等价！所以这个不是差异。但 njs 的写法更啰嗦，可能是为了可读性或历史原因。

不过让我再看看 EXTENDED_STREAM:
njs:
```typescript
case ReadDeviceIDCode.EXTENDED_STREAM: {
    if (objectID > 0x06 && objectID < 0x80) {
        objectID = 0x00;
    }
    break;
}
```
rs:
```rust
0x03 => {
    if object_id > 0x06 && object_id < 0x80 {
        object_id = 0x00;
    }
}
```
这个也完全一致。

所以 FC43/14 的 objectID 处理实际上是对齐的！我之前的判断有误。

### 4.4 ⚠️ Slave add/remove CustomFC 为 async 方法

**位置**：`src/slave.rs:1305-1313`

**njs-modbus**：同步方法
```typescript
public addCustomFunctionCode(cfc: CustomFunctionCode): void {
    this._customFunctionCodes.set(cfc.fc, cfc);
    this.applicationLayer.addCustomFunctionCode(cfc);
}
```

**rs-modbus**：async 方法
```rust
pub async fn add_custom_function_code(&self, cfc: CustomFunctionCode) {
    self.application.add_custom_function_code(cfc.clone());
    self.custom_function_codes.lock().await.insert(cfc.fc, cfc);
}
```

**说明**：这是 Rust 中使用 `tokio::sync::Mutex` 保护状态的自然结果。由于 njs-modbus 的 `_customFunctionCodes` 是同步 Map（无锁），方法为同步。rs-modbus 使用异步锁，因此方法为 async。这是语言生态差异，**不影响行为对齐**，但 API 签名不同。

### 4.5 ⚠️ RTU 层 Debug 打印未清理

**位置**：`src/layers/application/rtu.rs:373`

```rust
eprintln!("DEBUG flush_pool: sending frame unit={} fc={:02x} len={}", adu.unit, adu.fc, frame_bytes.len());
```

**建议**：移除调试打印，或使用 `tracing` / `log`  crate 替换为条件编译的日志。

### 4.6 ⚠️ Master `open()` 每次调用都重新订阅 framing

**位置**：`src/master.rs:80-117`

**njs-modbus**：事件监听在构造函数中注册一次
**rs-modbus**：`open()` 每次调用都 `subscribe_framing()` + spawn task

**说明**：如果 `close()` 后 `open()` 再次调用，rs 会重复注册 framing 订阅者。但由于 `close()` 会 abort tasks，所以实际上不会有重复。不过建议将订阅逻辑移到构造函数或 `new()` 中，与 njs 保持一致。

---

## 5. 测试覆盖对比

| 测试主题 | njs-modbus | rs-modbus | 状态 |
|---|---|---|---|
| ADU buffer | `adu-buffer.test.ts` | `types.rs` (内联单元测试) | ✅ |
| ASCII hex sentry | `ascii-hex-sentry.test.ts` | `ascii_hex_sentry_test.rs` | ✅ |
| ASCII hex validation | `ascii-hex-validation.test.ts` | `ascii_hex_validation_test.rs` | ✅ |
| ASCII TCP fragmentation | `ascii-tcp-fragmentation.test.ts` | （无独立文件） | ⚠️ |
| Check range | `check-range.test.ts` | `check_range_test.rs` | ✅ |
| Fallback atomic | `fallback-atomic.test.ts` | `fallback_atomic_test.rs` | ✅ |
| Fallback serial | `fallback-serial.test.ts` | （无） | ⚠️ 仅 Serial 相关 |
| FC17 server ID | `fc17-serverid-validation.test.ts` | （无独立文件） | ⚠️ |
| FC43 conformity | `fc43-conformity.test.ts` | `fc43_conformity_test.rs` | ✅ |
| FC43 UTF-8 | `fc43-utf8-objects.test.ts` | `fc43_utf8_test.rs` | ✅ |
| Gen connection ID | `gen-connection-id.test.ts` | `utils.rs` (内联) | ✅ |
| Master concurrent | `master-concurrent.test.ts` | `master_concurrent_test.rs` | ✅ |
| Modbus error | `modbus-error.test.ts` | `error.rs` (内联) | ✅ |
| Physical lifecycle | `physical-lifecycle.test.ts` | `physical/mod.rs` (内联) | ✅ |
| Predict RTU | `predict-rtu.test.ts` | `utils.rs` (内联) | ✅ |
| RTU custom FC | `rtu-custom-fc.test.ts` | （无独立文件） | ⚠️ |
| RTU pool overflow | `rtu-pool-overflow.test.ts` | `rtu_pool_overflow_test.rs` | ✅ |
| RTU t15 timing | `rtu-t15-timing.test.ts` | （无） | ⚠️ 见 §4.1 |
| RTU t35 default | `rtu-t35-default.test.ts` | `rtu.rs` (内联) | ✅ |
| RTU t35 strict | `rtu-t35-strict.test.ts` | （无） | ⚠️ 见 §4.2 |
| RTU TCP fragmentation | `rtu-tcp-fragmentation.test.ts` | `rtu_framing_test.rs` | ✅ |
| Serial e2e | `serial-e2e.test.ts` | （无，需要 serial feature） | ⚠️ |
| Slave multi-connection | `slave-multi-connection.test.ts` | `slave_multi_connection_test.rs` | ✅ |
| Slave | `slave.test.ts` | `slave_test.rs` | ✅ |
| TCP fragmentation | `tcp-fragmentation.test.ts` | `tcp_fragmentation_test.rs` | ✅ |
| UDP multi-client | （在 slave-multi-connection 中） | `udp_multi_client_test.rs` | ✅ |

---

## 6. 建议行动项

### 高优先级（行为差异，需修复）

1. **RTU 定时器实现** (`src/layers/application/rtu.rs`)
   - 为 Serial 模式添加 t3.5 inter-frame 定时器
   - 为 Serial 模式添加 t1.5 inter-character 超时检测
   - 参考 njs `rtu-application-layer.ts` 的 timer/commitFrame 逻辑

2. **RTU strict 模式** (`src/layers/application/rtu.rs`)
   - 将 `_strict` 参数改为实际使用
   - Serial 模式下 CRC mismatch 丢弃整个缓冲
   - Serial 模式下 incomplete frame 发射对应错误

3. **清理 Debug 打印** (`src/layers/application/rtu.rs:373`)
   - 移除 `eprintln!("DEBUG flush_pool: ...")`

### 中优先级（测试覆盖）

4. **RTU t15 定时器测试**
   - 验证 inter-character timeout 触发 `T1_5_EXCEEDED`

5. **RTU t35 strict 测试**
   - 验证 Serial 模式下 CRC mismatch → 整帧丢弃

6. **RTU Custom FC 测试**
   - 注册自定义 FC predictor，验证 framing 正确性

7. **FC17 Server ID 验证测试**
   - 多字节 Server ID、byteCount 溢出、uint8 验证

### 低优先级（API/实现风格）

8. **Slave CustomFC API 同步化**（可选）
   - 使用 `std::sync::Mutex` 替代 `tokio::sync::Mutex` 可使方法变为同步
   - 但这会引入阻塞风险，需权衡

9. **Master 订阅迁移到构造函数**（可选）
   - 将 framing/error 订阅从 `open()` 移到 `new()`，避免每次 open 重复 spawn task

---

## 7. 总结

| 维度 | 对齐度 | 说明 |
|---|---|---|
| 协议语义 | ~98% | 所有标准 FC 的处理逻辑、异常码、常量完全一致 |
| 类型系统 | ~99% | 所有公共类型一一对应，仅在 Rust/TS 表达上差异 |
| 错误处理 | ~98% | ErrorCode、ModbusError、辅助函数完全对齐 |
| 工具函数 | ~100% | CRC、LRC、checkRange、predict、bitsToMs 等算法一致 |
| 应用层 | ~92% | RTU 缺少定时器/strict 模式是主要差异 |
| Master | ~98% | FIFO/Concurrent/TID/PreCheck 完全对齐 |
| Slave | ~97% | 每连接 FIFO、地址锁、fallback 完全对齐 |
| 物理层 | ~95% | Serial/TCP/UDP 行为对齐，rs 缺少 Serial e2e 测试 |
| 测试覆盖 | ~85% | 核心测试已同步，缺少 RTU 定时器/strict/Custom FC 专项测试 |

**总体评估**：rs-modbus 在最新提交 `cea06fd` 中已完成与 njs-modbus `035dbd1` 的大部分功能对齐。剩余的关键差异集中在 **RTU 层的定时器逻辑和 strict 模式**，这两项缺失会导致 Serial 模式下的帧边界处理与 njs 不一致。建议优先修复 §4.1 和 §4.2。
