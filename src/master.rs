use crate::error::ModbusError;
use crate::layers::application::{ApplicationLayer, ApplicationProtocol, ApplicationRole, Framing};
use crate::layers::physical::PhysicalLayer;
use crate::master_session::{MasterSession, PreCheck, PreCheckOutcome, WaiterKey};
use crate::types::{ApplicationDataUnit, CustomFunctionCode, DeviceIdentification, DeviceObject, ServerId};
use crate::utils::{parse_coils, parse_registers};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

/// Tunables for [`ModbusMaster::new`]. Mirrors njs-modbus
/// `ModbusMasterOptions`.
#[derive(Clone, Copy, Debug)]
pub struct ModbusMasterOptions {
    /// Per-request timeout in ms when the caller does not pass an explicit
    /// timeout. Defaults to 1000.
    pub timeout_ms: u64,
    /// Enable pipelined concurrent requests on a single connection. Only
    /// valid for Modbus TCP application layers — constructing a master with
    /// `concurrent: true` on RTU or ASCII layers panics. Defaults to
    /// `false` (FIFO queue, requests are serialized).
    pub concurrent: bool,
}

impl Default for ModbusMasterOptions {
    fn default() -> Self {
        Self {
            timeout_ms: 1000,
            concurrent: false,
        }
    }
}

pub struct ModbusMaster<A: ApplicationLayer, P: PhysicalLayer> {
    application: Arc<A>,
    physical: Arc<P>,
    session: Arc<MasterSession>,
    pub timeout_ms: u64,
    pub concurrent: bool,
    next_tid: AtomicU16,
    closed: AtomicBool,
    clean_level: AtomicU8,
    queue_lock: tokio::sync::Mutex<()>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<A: ApplicationLayer + 'static, P: PhysicalLayer + 'static> ModbusMaster<A, P> {
    pub fn new(application: Arc<A>, physical: Arc<P>, options: ModbusMasterOptions) -> Self {
        if options.concurrent && application.protocol() != ApplicationProtocol::Tcp {
            panic!("concurrent mode requires a Modbus TCP application layer");
        }
        application
            .set_role(ApplicationRole::Master)
            .expect("application layer is already bound to a different role");
        let session = Arc::new(MasterSession::new());

        let session_for_framing = Arc::clone(&session);
        let mut framing_rx = application.subscribe_framing();
        let framing_task = tokio::spawn(async move {
            loop {
                match framing_rx.recv().await {
                    Ok(frame) => session_for_framing.handle_frame(frame),
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                }
            }
        });

        let session_for_error = Arc::clone(&session);
        let mut error_rx = application.subscribe_framing_error();
        let error_task = tokio::spawn(async move {
            loop {
                match error_rx.recv().await {
                    Ok(err) => session_for_error.handle_error(err),
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                }
            }
        });

        Self {
            application,
            physical,
            session,
            timeout_ms: options.timeout_ms,
            concurrent: options.concurrent,
            next_tid: AtomicU16::new(1),
            closed: AtomicBool::new(false),
            clean_level: AtomicU8::new(0),
            queue_lock: tokio::sync::Mutex::new(()),
            tasks: Mutex::new(vec![framing_task, error_task]),
        }
    }

    /// Allocate the next transaction ID. Cycles through `1..=65535`,
    /// skipping `0` on wrap. Matches njs-modbus `_nextTid` semantics.
    fn allocate_tid(&self) -> u16 {
        self.next_tid
            .fetch_update(Ordering::Release, Ordering::Acquire, |t| {
                let next = if t == 65535 { 1 } else { t + 1 };
                Some(next)
            })
            .unwrap()
    }

    fn clean(&self, level: u8) {
        let current = self.clean_level.load(Ordering::Acquire);
        if current == 2 {
            return;
        }
        if current == 1 && level == 1 {
            return;
        }
        self.closed.store(true, Ordering::Release);
        let err = if level == 2 {
            ModbusError::InvalidState("Master destroyed".into())
        } else {
            ModbusError::InvalidState("Master closed".into())
        };
        self.session.stop_all(err);
        self.clean_level.store(level, Ordering::Release);
    }

    pub async fn open(&self) -> Result<(), ModbusError> {
        if self.clean_level.load(Ordering::Acquire) == 2 {
            return Err(ModbusError::PortDestroyed);
        }
        self.clean_level.store(0, Ordering::Release);
        self.closed.store(false, Ordering::Release);
        self.next_tid.store(1, Ordering::Release);
        self.physical.open().await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<(), ModbusError> {
        if self.clean_level.load(Ordering::Acquire) == 2 {
            return Ok(());
        }
        self.clean(1);
        self.physical.close().await
    }

    pub async fn destroy(&self) {
        if self.clean_level.load(Ordering::Acquire) == 2 {
            return;
        }
        self.clean(2);
        {
            let mut tasks = self.tasks.lock().unwrap();
            for task in tasks.drain(..) {
                task.abort();
            }
        }
        self.application.destroy().await;
        let _ = self.physical.destroy().await;
    }

    fn check_unit_fc(unit: u8, fc: u8) -> PreCheck {
        Arc::new(move |f: &Framing| {
            if f.adu.unit == unit && f.adu.fc == fc {
                PreCheckOutcome::Pass
            } else {
                PreCheckOutcome::Fail(ModbusError::InvalidResponse)
            }
        })
    }

    fn check_length(expected: usize) -> PreCheck {
        Arc::new(move |_: &Framing| PreCheckOutcome::NeedLength(expected))
    }

    fn check_byte_count(expected: usize) -> PreCheck {
        Arc::new(move |f: &Framing| {
            if !f.adu.data.is_empty() && f.adu.data[0] as usize == expected {
                PreCheckOutcome::Pass
            } else {
                PreCheckOutcome::Fail(ModbusError::InvalidResponse)
            }
        })
    }

    fn check_echo(expected: Vec<u8>) -> PreCheck {
        Arc::new(move |f: &Framing| {
            if f.adu.data == expected {
                PreCheckOutcome::Pass
            } else {
                PreCheckOutcome::Fail(ModbusError::InvalidResponse)
            }
        })
    }

    async fn wait_response(
        &self,
        request: &ApplicationDataUnit,
        checks: Vec<PreCheck>,
        timeout_ms: u64,
    ) -> Result<Option<Framing>, ModbusError> {
        // Reject up-front so a newly issued call after `close()` doesn't
        // hit the socket. Necessary in concurrent mode (no queue lock) and
        // also covers the rare case where a FIFO caller starts mid-close.
        if self.closed.load(Ordering::Acquire) {
            return Err(ModbusError::InvalidState("Master closed".into()));
        }

        // FIFO mode: serialize call-sites via the queue lock so two callers
        // can't trample each other's MasterSession waiter slot. Concurrent
        // mode dispatches without holding the lock — each TCP request gets
        // its own TID-keyed waiter slot.
        let _queue_guard = if self.concurrent {
            None
        } else {
            Some(self.queue_lock.lock().await)
        };

        // A close() may have landed while we were waiting for the queue
        // lock. Re-check before allocating a TID / writing.
        if self.closed.load(Ordering::Acquire) {
            return Err(ModbusError::InvalidState("Master closed".into()));
        }

        // FIFO mode: drop stale buffer state from the previous request
        // before sending. Concurrent mode must NOT flush because other
        // in-flight requests share the application-layer buffer.
        if !self.concurrent {
            self.application.flush();
        }

        let broadcast = request.unit == 0;
        let uses_tid = self.application.protocol() == ApplicationProtocol::Tcp && !broadcast;

        // Build the actual request frame. For TCP non-broadcast requests
        // we allocate a fresh TID and encode it into the MBAP header; the
        // slave echoes that TID back on its response so we can demux
        // pipelined replies.
        let (encoded, key) = if uses_tid {
            let tid = self.allocate_tid();
            let adu = ApplicationDataUnit {
                transaction: Some(tid),
                unit: request.unit,
                fc: request.fc,
                data: request.data.clone(),
            };
            (self.application.encode(&adu), WaiterKey::Tid(tid))
        } else {
            (self.application.encode(request), WaiterKey::Fifo)
        };

        // Pre-check chain. When TID is used, prepend a TID match so any
        // stale response from a previous request (or a different in-flight
        // one) fails fast.
        let final_checks: Vec<PreCheck> = if let WaiterKey::Tid(tid) = key {
            let mut v: Vec<PreCheck> = Vec::with_capacity(checks.len() + 1);
            v.push(Arc::new(move |f: &Framing| {
                if f.adu.transaction == Some(tid) {
                    PreCheckOutcome::Pass
                } else {
                    PreCheckOutcome::Fail(ModbusError::InvalidResponse)
                }
            }));
            v.extend(checks);
            v
        } else {
            checks
        };

        // Arm the waiter BEFORE the write — otherwise a fast slave's reply
        // can arrive between write completion and `start(...)` and be
        // dropped on the floor.
        let rx = self.session.start(key, final_checks);
        if let Err(err) = self.physical.write(&encoded).await {
            self.session.stop(key);
            return Err(err);
        }

        if broadcast {
            // No response expected. Tear down the (unused) waiter we just
            // armed for the FIFO key.
            self.session.stop(key);
            return Ok(None);
        }

        let timeout = Duration::from_millis(timeout_ms);
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(frame))) => Ok(Some(frame)),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_)) => {
                // Receiver dropped (e.g. session.stop_all elsewhere).
                Err(ModbusError::InvalidState(
                    "master session was cleared while waiting".into(),
                ))
            }
            Err(_) => {
                self.session.stop(key);
                Err(ModbusError::Timeout)
            }
        }
    }

    // FC1 - Read Coils
    pub async fn read_coils(
        &self,
        unit: u8,
        address: u16,
        length: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<bool>>, ModbusError> {
        let fc = 0x01;
        let byte_count = ((length + 7) / 8) as usize;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&length.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + byte_count),
                    Self::check_byte_count(byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_coils(&f.adu.data, length))),
            None => Ok(None),
        }
    }

    // FC2 - Read Discrete Inputs
    pub async fn read_discrete_inputs(
        &self,
        unit: u8,
        address: u16,
        length: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<bool>>, ModbusError> {
        let fc = 0x02;
        let byte_count = ((length + 7) / 8) as usize;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&length.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + byte_count),
                    Self::check_byte_count(byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_coils(&f.adu.data, length))),
            None => Ok(None),
        }
    }

    // FC3 - Read Holding Registers
    pub async fn read_holding_registers(
        &self,
        unit: u8,
        address: u16,
        length: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<u16>>, ModbusError> {
        let fc = 0x03;
        let byte_count = (length * 2) as usize;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&length.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + byte_count),
                    Self::check_byte_count(byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_registers(&f.adu.data, length))),
            None => Ok(None),
        }
    }

    // FC4 - Read Input Registers
    pub async fn read_input_registers(
        &self,
        unit: u8,
        address: u16,
        length: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<u16>>, ModbusError> {
        let fc = 0x04;
        let byte_count = (length * 2) as usize;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&length.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + byte_count),
                    Self::check_byte_count(byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_registers(&f.adu.data, length))),
            None => Ok(None),
        }
    }

    // FC5 - Write Single Coil
    pub async fn write_single_coil(
        &self,
        unit: u8,
        address: u16,
        value: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Option<bool>, ModbusError> {
        let fc = 0x05;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        let value_u16: u16 = if value { 0xff00 } else { 0x0000 };
        buf[2..4].copy_from_slice(&value_u16.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf.clone());

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(4),
                    Self::check_echo(buf),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    // FC6 - Write Single Register
    pub async fn write_single_register(
        &self,
        unit: u8,
        address: u16,
        value: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<u16>, ModbusError> {
        let fc = 0x06;

        let mut buf = vec![0u8; 4];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&value.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf.clone());

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(4),
                    Self::check_echo(buf),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    // FC15 - Write Multiple Coils
    pub async fn write_multiple_coils(
        &self,
        unit: u8,
        address: u16,
        values: &[bool],
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<bool>>, ModbusError> {
        let fc = 0x0f;
        let byte_count = ((values.len() + 7) / 8) as u8;

        let mut buf = vec![0u8; 5 + byte_count as usize];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&(values.len() as u16).to_be_bytes());
        buf[4] = byte_count;
        for (byte_idx, chunk) in values.chunks(8).enumerate() {
            let mut byte = 0u8;
            for (bit_idx, &v) in chunk.iter().enumerate() {
                if v {
                    byte |= 1 << bit_idx;
                }
            }
            buf[5 + byte_idx] = byte;
        }

        let tx_buf = buf.clone();
        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(4),
                    Self::check_echo(tx_buf[..4].to_vec()),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some(values.to_vec())),
            None => Ok(None),
        }
    }

    // FC16 - Write Multiple Registers
    pub async fn write_multiple_registers(
        &self,
        unit: u8,
        address: u16,
        values: &[u16],
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<u16>>, ModbusError> {
        let fc = 0x10;
        let byte_count = (values.len() * 2) as u8;

        let mut buf = vec![0u8; 5 + byte_count as usize];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&(values.len() as u16).to_be_bytes());
        buf[4] = byte_count;
        for (i, &v) in values.iter().enumerate() {
            buf[5 + i * 2..7 + i * 2].copy_from_slice(&v.to_be_bytes());
        }

        let tx_buf = buf.clone();
        let request = ApplicationDataUnit::new(unit, fc, buf);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(4),
                    Self::check_echo(tx_buf[..4].to_vec()),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some(values.to_vec())),
            None => Ok(None),
        }
    }

    // FC17 - Report Server ID
    pub async fn report_server_id(
        &self,
        unit: u8,
        server_id_length: usize,
        timeout_ms: Option<u64>,
    ) -> Result<Option<ServerId>, ModbusError> {
        let fc = 0x11;
        let request = ApplicationDataUnit::new(unit, fc, vec![]);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Arc::new(move |f: &Framing| {
                        if !f.adu.data.is_empty() {
                            let len = 1 + f.adu.data[0] as usize;
                            PreCheckOutcome::NeedLength(len)
                        } else {
                            PreCheckOutcome::InsufficientData
                        }
                    }),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => {
                let run_status_index = 1 + server_id_length;
                if f.adu.data.len() < run_status_index + 1 {
                    return Err(ModbusError::InvalidResponse);
                }
                Ok(Some(ServerId {
                    server_id: f.adu.data[1..run_status_index].to_vec(),
                    run_indicator_status: f.adu.data[run_status_index] == 0xff,
                    additional_data: f.adu.data[run_status_index + 1..].to_vec(),
                }))
            }
            None => Ok(None),
        }
    }

    // FC22 - Mask Write Register
    pub async fn mask_write_register(
        &self,
        unit: u8,
        address: u16,
        and_mask: u16,
        or_mask: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Option<(u16, u16)>, ModbusError> {
        let fc = 0x16;

        let mut buf = vec![0u8; 6];
        buf[0..2].copy_from_slice(&address.to_be_bytes());
        buf[2..4].copy_from_slice(&and_mask.to_be_bytes());
        buf[4..6].copy_from_slice(&or_mask.to_be_bytes());

        let request = ApplicationDataUnit::new(unit, fc, buf.clone());

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(6),
                    Self::check_echo(buf),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(_) => Ok(Some((and_mask, or_mask))),
            None => Ok(None),
        }
    }

    // FC23 - Read/Write Multiple Registers
    pub async fn read_and_write_multiple_registers(
        &self,
        unit: u8,
        read_address: u16,
        read_length: u16,
        write_address: u16,
        write_values: &[u16],
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<u16>>, ModbusError> {
        let fc = 0x17;
        let byte_count = (write_values.len() * 2) as u8;

        let mut buf = vec![0u8; 9 + byte_count as usize];
        buf[0..2].copy_from_slice(&read_address.to_be_bytes());
        buf[2..4].copy_from_slice(&read_length.to_be_bytes());
        buf[4..6].copy_from_slice(&write_address.to_be_bytes());
        buf[6..8].copy_from_slice(&(write_values.len() as u16).to_be_bytes());
        buf[8] = byte_count;
        for (i, &v) in write_values.iter().enumerate() {
            buf[9 + i * 2..11 + i * 2].copy_from_slice(&v.to_be_bytes());
        }

        let request = ApplicationDataUnit::new(unit, fc, buf);
        let read_byte_count = (read_length * 2) as usize;

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Self::check_length(1 + read_byte_count),
                    Self::check_byte_count(read_byte_count),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => Ok(Some(parse_registers(&f.adu.data, read_length))),
            None => Ok(None),
        }
    }

    // FC43/14 - Read Device Identification
    pub async fn read_device_identification(
        &self,
        unit: u8,
        read_device_id_code: u8,
        object_id: u8,
        timeout_ms: Option<u64>,
    ) -> Result<Option<DeviceIdentification>, ModbusError> {
        let fc = 0x2b;
        let request =
            ApplicationDataUnit::new(unit, fc, vec![0x0e, read_device_id_code, object_id]);

        let frame = self
            .wait_response(
                &request,
                vec![
                    Self::check_unit_fc(unit, fc),
                    Arc::new(move |f: &Framing| {
                        if f.adu.data.len() >= 6
                            && f.adu.data[0] == 0x0e
                            && f.adu.data[1] == read_device_id_code
                        {
                            let num_objects = f.adu.data[5] as usize;
                            let mut total = 6usize;
                            let mut idx = 6;
                            for _ in 0..num_objects {
                                if idx + 2 > f.adu.data.len() {
                                    return PreCheckOutcome::InsufficientData;
                                }
                                let obj_len = f.adu.data[idx + 1] as usize;
                                total += 2 + obj_len;
                                idx += 2 + obj_len;
                            }
                            PreCheckOutcome::NeedLength(total)
                        } else {
                            PreCheckOutcome::Fail(ModbusError::InvalidResponse)
                        }
                    }),
                ],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;

        match frame {
            Some(f) => {
                let mut objects = Vec::new();
                let num_objects = f.adu.data[5] as usize;
                let mut idx = 6;
                for _ in 0..num_objects {
                    let obj_id = f.adu.data[idx];
                    let obj_len = f.adu.data[idx + 1] as usize;
                    let obj_value =
                        String::from_utf8_lossy(&f.adu.data[idx + 2..idx + 2 + obj_len])
                            .to_string();
                    objects.push(DeviceObject {
                        id: obj_id,
                        value: obj_value,
                    });
                    idx += 2 + obj_len;
                }
                Ok(Some(DeviceIdentification {
                    read_device_id_code: f.adu.data[1],
                    conformity_level: f.adu.data[2],
                    more_follows: f.adu.data[3] == 0xff,
                    next_object_id: f.adu.data[4],
                    objects,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn add_custom_function_code(&self, cfc: CustomFunctionCode) {
        self.application.add_custom_function_code(cfc);
    }

    pub fn remove_custom_function_code(&self, fc: u8) {
        self.application.remove_custom_function_code(fc);
    }

    /// Send a non-standard / custom function code request. The master only
    /// validates that the response has matching `unit` and `fc`; any payload
    /// is returned as raw bytes. The caller must have registered a
    /// [`CustomFunctionCode`] with `predict_response_length` on the
    /// application layer (or on this master) so RTU framing can advance.
    ///
    /// `unit == 0` is broadcast: returns `Ok(None)` after the write.
    pub async fn send_custom_fc(
        &self,
        unit: u8,
        fc: u8,
        data: Vec<u8>,
        timeout_ms: Option<u64>,
    ) -> Result<Option<Vec<u8>>, ModbusError> {
        let request = ApplicationDataUnit::new(unit, fc, data);
        let frame = self
            .wait_response(
                &request,
                vec![Self::check_unit_fc(unit, fc)],
                timeout_ms.unwrap_or(self.timeout_ms),
            )
            .await?;
        match frame {
            Some(f) => Ok(Some(f.adu.data)),
            None => Ok(None),
        }
    }
}
