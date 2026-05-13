use crate::error::{get_code_by_error, get_error_by_code, ErrorCode, ModbusError};
use crate::layers::application::{ApplicationLayer, ApplicationProtocol, ApplicationRole};
use crate::layers::physical::{ConnectionId, PhysicalLayer, ResponseFn};
use crate::types::{
    AddressRange, ApplicationDataUnit, CustomFunctionCode, FramedDataUnit, ServerId,
};
use crate::utils::{check_range, pack_coils, pack_registers};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

type SlaveResponseFn = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), ModbusError>> + Send>> + Send + Sync,
>;

#[async_trait]
pub trait ModbusSlaveModel: Send + Sync {
    fn unit(&self) -> u8;
    fn address_range(&self) -> AddressRange;

    async fn intercept(&self, _fc: u8, _data: &[u8]) -> Result<Option<Vec<u8>>, ModbusError> {
        Ok(None)
    }

    async fn read_coils(&self, _address: u16, _length: u16) -> Result<Vec<bool>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    async fn write_single_coil(&self, _address: u16, _value: bool) -> Result<(), ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    /// Default: loop `write_single_coil`. Mirrors njs-modbus' behavior where
    /// a model that only provides `writeSingleCoil` is automatically usable
    /// for FC15 requests. Override to provide a bulk-write fast path.
    async fn write_multiple_coils(&self, address: u16, values: &[bool]) -> Result<(), ModbusError> {
        for (i, &v) in values.iter().enumerate() {
            self.write_single_coil(address + i as u16, v).await?;
        }
        Ok(())
    }

    async fn read_discrete_inputs(
        &self,
        _address: u16,
        _length: u16,
    ) -> Result<Vec<bool>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }

    async fn read_holding_registers(
        &self,
        _address: u16,
        _length: u16,
    ) -> Result<Vec<u16>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    async fn write_single_register(&self, _address: u16, _value: u16) -> Result<(), ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    /// Default: loop `write_single_register`. See `write_multiple_coils`.
    async fn write_multiple_registers(
        &self,
        address: u16,
        values: &[u16],
    ) -> Result<(), ModbusError> {
        for (i, &v) in values.iter().enumerate() {
            self.write_single_register(address + i as u16, v).await?;
        }
        Ok(())
    }
    /// Default: read-modify-write using `read_holding_registers` +
    /// `write_single_register`. Mirrors njs-modbus' fallback.
    async fn mask_write_register(
        &self,
        address: u16,
        and_mask: u16,
        or_mask: u16,
    ) -> Result<(), ModbusError> {
        let regs = self.read_holding_registers(address, 1).await?;
        let current = *regs.first().ok_or(ModbusError::ServerDeviceFailure)?;
        let new = (current & and_mask) | (or_mask & !and_mask);
        self.write_single_register(address, new).await
    }

    async fn read_input_registers(
        &self,
        _address: u16,
        _length: u16,
    ) -> Result<Vec<u16>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }

    async fn report_server_id(&self) -> Result<ServerId, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
    async fn read_device_identification(&self) -> Result<HashMap<u8, String>, ModbusError> {
        Err(ModbusError::IllegalFunction)
    }
}

/// Tunables for [`ModbusSlave::with_options`]. Mirrors njs-modbus
/// `ModbusSlaveOptions`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ModbusSlaveOptions {
    /// Pipelined concurrent processing of requests within a single
    /// connection. Only valid for Modbus TCP application layers (TID
    /// disambiguates responses); constructing a slave with `concurrent:
    /// true` on RTU or ASCII panics. Defaults to `false` (per-connection
    /// FIFO — same connection serialized, different connections in
    /// parallel).
    pub concurrent: bool,
}

struct QueueEntry {
    items: VecDeque<(FramedDataUnit, ResponseFn)>,
    processing: bool,
}

pub struct ModbusSlave<A: ApplicationLayer, P: PhysicalLayer> {
    application: Arc<A>,
    physical: Arc<P>,
    pub models: Arc<tokio::sync::Mutex<HashMap<u8, Arc<dyn ModbusSlaveModel>>>>,
    pub concurrent: bool,
    queues: Arc<tokio::sync::Mutex<HashMap<ConnectionId, QueueEntry>>>,
    tasks: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    is_open: Arc<std::sync::atomic::AtomicBool>,
    custom_function_codes: std::sync::Mutex<HashMap<u8, CustomFunctionCode>>,
    clean_level: std::sync::atomic::AtomicU8,
    /// Per-address async locks for FC22/FC23 fallback paths. Maps a register
    /// address to a tokio mutex; requests that touch overlapping addresses
    /// are serialized. Mirrors njs-modbus `withAddressLock`.
    address_locks: Arc<tokio::sync::Mutex<HashMap<u16, Arc<tokio::sync::Mutex<()>>>>>,
}

impl<A: ApplicationLayer + 'static, P: PhysicalLayer + 'static> ModbusSlave<A, P> {
    pub fn new(application: Arc<A>, physical: Arc<P>) -> Self {
        Self::with_options(application, physical, ModbusSlaveOptions::default())
    }

    pub fn with_options(
        application: Arc<A>,
        physical: Arc<P>,
        options: ModbusSlaveOptions,
    ) -> Self {
        if options.concurrent && application.protocol() != ApplicationProtocol::Tcp {
            panic!("concurrent mode requires a Modbus TCP application layer");
        }
        application
            .set_role(ApplicationRole::Slave)
            .expect("application layer is already bound to a different role");
        Self {
            application,
            physical,
            models: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            concurrent: options.concurrent,
            queues: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            tasks: tokio::sync::Mutex::new(Vec::new()),
            is_open: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            custom_function_codes: std::sync::Mutex::new(HashMap::new()),
            address_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            clean_level: std::sync::atomic::AtomicU8::new(0),
        }
    }

    pub async fn add(&self, model: Box<dyn ModbusSlaveModel>) {
        let unit = model.unit();
        // Convert to Arc so model references can be cheaply cloned out of
        // the map and the map lock released before handler invocation.
        // That's critical for slave-side concurrency: holding the models
        // mutex across an FC handler's `.await` would serialize every
        // request slave-wide regardless of which connection it came from.
        let arc: Arc<dyn ModbusSlaveModel> = Arc::from(model);
        self.models.lock().await.insert(unit, arc);
    }

    pub async fn remove(&self, unit: u8) {
        self.models.lock().await.remove(&unit);
    }

    pub async fn open(&self) -> Result<(), ModbusError> {
        self.is_open
            .store(true, std::sync::atomic::Ordering::Release);
        self.clean_level
            .store(0, std::sync::atomic::Ordering::Release);
        // Fresh session — drop any state from a prior open/close cycle.
        self.queues.lock().await.clear();

        let application = Arc::clone(&self.application);
        let models = Arc::clone(&self.models);
        let queues = Arc::clone(&self.queues);
        let address_locks = Arc::clone(&self.address_locks);
        let custom_fcs: Arc<HashMap<u8, CustomFunctionCode>> =
            Arc::new(self.custom_function_codes.lock().unwrap().clone());
        let concurrent = self.concurrent;
        let is_open = Arc::clone(&self.is_open);
        let mut framing_rx = self.application.subscribe_framing();
        let framing_task = tokio::spawn(async move {
            loop {
                match framing_rx.recv().await {
                    Ok(framing) => {
                        if !is_open.load(std::sync::atomic::Ordering::Acquire) {
                            continue;
                        }
                        let frame = FramedDataUnit {
                            adu: framing.adu,
                            raw: framing.raw,
                        };
                        if concurrent {
                            // TCP-only: each frame gets its own task. The
                            // TID embedded in the response disambiguates.
                            let app = Arc::clone(&application);
                            let mdls = Arc::clone(&models);
                            let cfs = Arc::clone(&custom_fcs);
                            let locks = Arc::clone(&address_locks);
                            tokio::spawn(async move {
                                Self::process_frame(
                                    &app,
                                    &mdls,
                                    &cfs,
                                    &locks,
                                    frame,
                                    framing.response,
                                )
                                .await;
                            });
                        } else {
                            // Per-connection FIFO: push onto this
                            // connection's queue, kick off a drain task
                            // if not already running.
                            Self::enqueue_and_drain(
                                Arc::clone(&queues),
                                Arc::clone(&application),
                                Arc::clone(&models),
                                Arc::clone(&custom_fcs),
                                Arc::clone(&address_locks),
                                framing.connection,
                                frame,
                                framing.response,
                            )
                            .await;
                        }
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                }
            }
        });

        // Drop a connection's queue when its peer disconnects — the
        // response closure points at a now-dead socket, so there is no
        // point continuing to process queued frames. Keeps the in-flight
        // one running; the drain task cleans up the entry when it lands
        // on an empty queue.
        let queues_for_close = Arc::clone(&self.queues);
        let mut conn_close_rx = self.physical.subscribe_connection_close();
        let conn_close_task = tokio::spawn(async move {
            loop {
                match conn_close_rx.recv().await {
                    Ok(conn_id) => {
                        let mut g = queues_for_close.lock().await;
                        if let Some(entry) = g.get_mut(&conn_id) {
                            entry.items.clear();
                            if !entry.processing {
                                g.remove(&conn_id);
                            }
                        }
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                }
            }
        });

        // Clear everything on a full physical-layer close.
        let queues_for_full_close = Arc::clone(&self.queues);
        let mut close_rx = self.physical.subscribe_close();
        let close_task = tokio::spawn(async move {
            loop {
                match close_rx.recv().await {
                    Ok(()) => {
                        queues_for_full_close.lock().await.clear();
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                }
            }
        });

        self.tasks
            .lock()
            .await
            .extend([framing_task, conn_close_task, close_task]);

        self.physical.open().await?;
        Ok(())
    }

    async fn clean(&self, level: u8) {
        let current = self.clean_level.load(std::sync::atomic::Ordering::Acquire);
        if current == 2 {
            return;
        }
        if current == 1 && level == 1 {
            return;
        }
        self.is_open
            .store(false, std::sync::atomic::Ordering::Release);
        self.queues.lock().await.clear();
        self.address_locks.lock().await.clear();
        if level == 2 {
            self.custom_function_codes.lock().unwrap().clear();
            self.models.lock().await.clear();
        }
        self.clean_level
            .store(level, std::sync::atomic::Ordering::Release);
    }

    pub async fn close(&self) -> Result<(), ModbusError> {
        if self.clean_level.load(std::sync::atomic::Ordering::Acquire) == 2 {
            return Ok(());
        }
        self.clean(1).await;
        return self.physical.close().await;
    }

    pub async fn destroy(&self) {
        if self.clean_level.load(std::sync::atomic::Ordering::Acquire) == 2 {
            return;
        }
        self.clean(2).await;
        {
            let mut tasks = self.tasks.lock().await;
            for task in tasks.drain(..) {
                task.abort();
            }
        }
        let _ = self.physical.destroy().await;
    }

    /// Acquire an async lock for each address in `addresses`, execute `f`, then
    /// release all locks. Addresses are deduplicated and sorted before locking
    /// to avoid deadlocks. Mirrors njs-modbus `withAddressLock`.
    async fn with_address_lock<F, Fut, T>(
        address_locks: &tokio::sync::Mutex<HashMap<u16, Arc<tokio::sync::Mutex<()>>>>,
        addresses: &[u16],
        f: F,
    ) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let mut sorted: Vec<u16> = addresses.iter().copied().collect();
        sorted.sort_unstable();
        sorted.dedup();

        // Collect the Arc<Mutex<()>> for each address (may create new entries).
        let lock_arcs: Vec<Arc<tokio::sync::Mutex<()>>> = {
            let mut locks = address_locks.lock().await;
            sorted
                .iter()
                .map(|&addr| {
                    locks
                        .entry(addr)
                        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                        .clone()
                })
                .collect()
        };

        // Acquire all locks in sorted order. Locking in a consistent sorted
        // order ensures no two concurrent `with_address_lock` calls can
        // deadlock with each other, even if their address sets overlap.
        let mut guards: Vec<tokio::sync::MutexGuard<'_, ()>> = Vec::with_capacity(lock_arcs.len());
        for arc in &lock_arcs {
            guards.push(arc.lock().await);
        }

        let result = f().await;
        drop(guards);
        drop(lock_arcs);

        // Clean up entries whose Arc is no longer referenced outside the map.
        // This prevents unbounded growth when writes touch many distinct
        // addresses over the lifetime of the slave.
        {
            let mut locks = address_locks.lock().await;
            for &addr in &sorted {
                if let Some(arc) = locks.get(&addr) {
                    if Arc::strong_count(arc) == 1 {
                        locks.remove(&addr);
                    }
                }
            }
        }

        result
    }

    /// Push a frame onto the per-connection queue. If no drain task is
    /// currently running for this connection, spawn one; otherwise the
    /// already-running drain picks the new item up on its next iteration.
    async fn enqueue_and_drain(
        queues: Arc<tokio::sync::Mutex<HashMap<ConnectionId, QueueEntry>>>,
        application: Arc<A>,
        models: Arc<tokio::sync::Mutex<HashMap<u8, Arc<dyn ModbusSlaveModel>>>>,
        custom_fcs: Arc<HashMap<u8, CustomFunctionCode>>,
        address_locks: Arc<tokio::sync::Mutex<HashMap<u16, Arc<tokio::sync::Mutex<()>>>>>,
        connection: ConnectionId,
        frame: FramedDataUnit,
        response: ResponseFn,
    ) {
        let should_spawn = {
            let mut g = queues.lock().await;
            let entry = g.entry(Arc::clone(&connection)).or_insert(QueueEntry {
                items: VecDeque::new(),
                processing: false,
            });
            entry.items.push_back((frame, response));
            if entry.processing {
                false
            } else {
                entry.processing = true;
                true
            }
        };
        if should_spawn {
            tokio::spawn(async move {
                Self::drain_loop(
                    queues,
                    application,
                    models,
                    custom_fcs,
                    address_locks,
                    connection,
                )
                .await;
            });
        }
    }

    /// Drain the per-connection queue until empty. The entry is removed
    /// from the map when the queue settles empty so the map doesn't grow
    /// unbounded across ephemeral connections (UDP rinfos, brief TCP
    /// clients). If the entry is gone mid-drain (cleared by a
    /// `connection_close` handler), we bail early.
    async fn drain_loop(
        queues: Arc<tokio::sync::Mutex<HashMap<ConnectionId, QueueEntry>>>,
        application: Arc<A>,
        models: Arc<tokio::sync::Mutex<HashMap<u8, Arc<dyn ModbusSlaveModel>>>>,
        custom_fcs: Arc<HashMap<u8, CustomFunctionCode>>,
        address_locks: Arc<tokio::sync::Mutex<HashMap<u16, Arc<tokio::sync::Mutex<()>>>>>,
        connection: ConnectionId,
    ) {
        loop {
            let next = {
                let mut g = queues.lock().await;
                match g.get_mut(&connection) {
                    Some(entry) => entry.items.pop_front(),
                    None => return,
                }
            };
            match next {
                Some((frame, response)) => {
                    Self::process_frame(
                        &application,
                        &models,
                        &custom_fcs,
                        &address_locks,
                        frame,
                        response,
                    )
                    .await;
                }
                None => {
                    let mut g = queues.lock().await;
                    if let Some(entry) = g.get_mut(&connection) {
                        if entry.items.is_empty() {
                            g.remove(&connection);
                            return;
                        }
                        // else a new item snuck in between pop_front and
                        // this lock; loop and re-drain.
                    } else {
                        return;
                    }
                }
            }
        }
    }

    async fn process_frame(
        application: &Arc<A>,
        models: &Arc<tokio::sync::Mutex<HashMap<u8, Arc<dyn ModbusSlaveModel>>>>,
        custom_fcs: &HashMap<u8, CustomFunctionCode>,
        address_locks: &tokio::sync::Mutex<HashMap<u16, Arc<tokio::sync::Mutex<()>>>>,
        frame: FramedDataUnit,
        response_fn: ResponseFn,
    ) {
        let unit = frame.adu.unit;
        // Snapshot the model Arc(s) under a brief lock so the map mutex
        // isn't held across handler `.await` points — otherwise a slow
        // handler on one model would block every other connection's
        // frames slave-wide. Mirrors the per-connection FIFO goal of
        // Item #4.
        let models_snapshot: Vec<Arc<dyn ModbusSlaveModel>> = {
            let g = models.lock().await;
            if unit == 0 {
                g.values().map(Arc::clone).collect()
            } else {
                match g.get(&unit) {
                    Some(m) => vec![Arc::clone(m)],
                    None => return,
                }
            }
        };

        for model_arc in models_snapshot {
            let model: &dyn ModbusSlaveModel = &*model_arc;

            let response: SlaveResponseFn = if unit == 0 {
                Arc::new(|_| {
                    Box::pin(async { Ok(()) })
                        as Pin<Box<dyn Future<Output = Result<(), ModbusError>> + Send>>
                })
            } else {
                let response_fn = Arc::clone(&response_fn);
                Arc::new(move |data| {
                    let response_fn = Arc::clone(&response_fn);
                    Box::pin(async move { response_fn(data).await })
                        as Pin<Box<dyn Future<Output = Result<(), ModbusError>> + Send>>
                })
            };

            match model.intercept(frame.adu.fc, &frame.adu.data).await {
                Ok(Some(data)) => {
                    let response_adu = ApplicationDataUnit {
                        transaction: frame.adu.transaction,
                        unit: frame.adu.unit,
                        fc: frame.adu.fc,
                        data,
                    };
                    let encoded = application.encode(&response_adu);
                    let _ = response(encoded).await;
                    continue;
                }
                Ok(None) => {}
                Err(err) => {
                    let _ = Self::response_error(application, &frame.adu, response, &err).await;
                    continue;
                }
            }

            let result = match frame.adu.fc {
                0x01 => {
                    Self::handle_fc1(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x02 => {
                    Self::handle_fc2(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x03 => {
                    Self::handle_fc3(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x04 => {
                    Self::handle_fc4(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x05 => {
                    Self::handle_fc5(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x06 => {
                    Self::handle_fc6(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x0f => {
                    Self::handle_fc15(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x10 => {
                    Self::handle_fc16(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x11 => {
                    Self::handle_fc17(application, model, &frame.adu, Arc::clone(&response)).await
                }
                0x16 => {
                    Self::handle_fc22(
                        application,
                        model,
                        address_locks,
                        &frame.adu,
                        Arc::clone(&response),
                    )
                    .await
                }
                0x17 => {
                    Self::handle_fc23(
                        application,
                        model,
                        address_locks,
                        &frame.adu,
                        Arc::clone(&response),
                    )
                    .await
                }
                0x2b => {
                    Self::handle_fc43_14(application, model, &frame.adu, Arc::clone(&response))
                        .await
                }
                _ => {
                    if let Some(cfc) = custom_fcs.get(&frame.adu.fc) {
                        if let Some(ref handler) = cfc.handle {
                            let handler_clone: std::sync::Arc<
                                dyn Fn(Vec<u8>, u8) -> crate::types::CustomFcHandleResult
                                    + Send
                                    + Sync,
                            > = Arc::clone(handler);
                            let pdu = frame.adu.data.clone();
                            match handler_clone(pdu, frame.adu.unit).await {
                                Ok(response_data) => {
                                    let response_adu = ApplicationDataUnit {
                                        transaction: frame.adu.transaction,
                                        unit: frame.adu.unit,
                                        fc: frame.adu.fc,
                                        data: response_data,
                                    };
                                    let _ = response(application.encode(&response_adu)).await;
                                    continue;
                                }
                                Err(e) => {
                                    let _ = Self::response_error(
                                        application,
                                        &frame.adu,
                                        Arc::clone(&response),
                                        &e,
                                    )
                                    .await;
                                    continue;
                                }
                            }
                        }
                    }
                    Self::response_error(
                        application,
                        &frame.adu,
                        Arc::clone(&response),
                        &get_error_by_code(ErrorCode::IllegalFunction),
                    )
                    .await
                }
            };

            if let Err(e) = result {
                let _ = Self::response_error(application, &frame.adu, response, &e).await;
            }
        }
    }

    async fn response_error(
        application: &Arc<A>,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
        error: &ModbusError,
    ) -> Result<(), ModbusError> {
        let error_code = get_code_by_error(error) as u8;
        let response_adu = ApplicationDataUnit {
            transaction: adu.transaction,
            unit: adu.unit,
            fc: adu.fc | 0x80,
            data: vec![error_code],
        };
        let encoded = application.encode(&response_adu);
        response(encoded).await
    }

    // FC1 - Read Coils
    async fn handle_fc1(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !(1..=0x07d0).contains(&length) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(&[address, address + length], &model.address_range().coils) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.read_coils(address, length).await {
            Ok(coils) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_coils(&coils, length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC2 - Read Discrete Inputs
    async fn handle_fc2(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !(1..=0x07d0).contains(&length) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[address, address + length],
            &model.address_range().discrete_inputs,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.read_discrete_inputs(address, length).await {
            Ok(inputs) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_coils(&inputs, length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC3 - Read Holding Registers
    async fn handle_fc3(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !(1..=0x007d).contains(&length) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[address, address + length],
            &model.address_range().holding_registers,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.read_holding_registers(address, length).await {
            Ok(registers) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_registers(&registers, length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC4 - Read Input Registers
    async fn handle_fc4(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !(1..=0x007d).contains(&length) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[address, address + length],
            &model.address_range().input_registers,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.read_input_registers(address, length).await {
            Ok(registers) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_registers(&registers, length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC5 - Write Single Coil
    async fn handle_fc5(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let value = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if value != 0x0000 && value != 0xff00 {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(&[address], &model.address_range().coils) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.write_single_coil(address, value == 0xff00).await {
            Ok(()) => response(application.encode(adu)).await,
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC6 - Write Single Register
    async fn handle_fc6(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 4 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let value = u16::from_be_bytes([adu.data[2], adu.data[3]]);

        if !check_range(&[address], &model.address_range().holding_registers) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        match model.write_single_register(address, value).await {
            Ok(()) => response(application.encode(adu)).await,
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC15 - Write Multiple Coils
    async fn handle_fc15(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() < 6 || adu.data.len() != 5 + adu.data[4] as usize {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);
        let byte_count = adu.data[4];

        if !(1..=0x07b0).contains(&length) || byte_count as u16 != (length + 7) / 8 {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(&[address, address + length], &model.address_range().coils) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        let values: Vec<bool> = (0..length)
            .map(|i| (adu.data[5 + i as usize / 8] >> (i % 8)) & 1 == 1)
            .collect();

        let result = model.write_multiple_coils(address, &values).await;

        match result {
            Ok(()) => {
                let mut data = vec![0u8; 4];
                data[0..2].copy_from_slice(&address.to_be_bytes());
                data[2..4].copy_from_slice(&length.to_be_bytes());
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data,
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC16 - Write Multiple Registers
    async fn handle_fc16(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() < 6 || adu.data.len() != 5 + adu.data[4] as usize {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let length = u16::from_be_bytes([adu.data[2], adu.data[3]]);
        let byte_count = adu.data[4];

        if !(1..=0x007b).contains(&length) || byte_count as u16 != length * 2 {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[address, address + length],
            &model.address_range().holding_registers,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        let values: Vec<u16> = (0..length)
            .map(|i| {
                u16::from_be_bytes([adu.data[5 + i as usize * 2], adu.data[6 + i as usize * 2]])
            })
            .collect();

        let result = model.write_multiple_registers(address, &values).await;

        match result {
            Ok(()) => {
                let mut data = vec![0u8; 4];
                data[0..2].copy_from_slice(&address.to_be_bytes());
                data[2..4].copy_from_slice(&length.to_be_bytes());
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data,
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC17 - Report Server ID
    async fn handle_fc17(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if !adu.data.is_empty() {
            return Ok(());
        }

        match model.report_server_id().await {
            Ok(server_id) => {
                // Modbus V1.1b3 §6.17 leaves Server ID length device-specific
                // (N bytes). Validate the assembled payload fits in a single
                // byteCount byte before we serialize.
                let server_id_bytes = if server_id.server_id.is_empty() {
                    vec![model.unit()]
                } else {
                    server_id.server_id.clone()
                };
                let byte_count = server_id_bytes.len() + 1 + server_id.additional_data.len();
                if byte_count > 255 {
                    return Self::response_error(
                        application,
                        adu,
                        response,
                        &get_error_by_code(ErrorCode::ServerDeviceFailure),
                    )
                    .await;
                }
                let mut data = Vec::with_capacity(1 + byte_count);
                data.push(byte_count as u8);
                data.extend_from_slice(&server_id_bytes);
                data.push(if server_id.run_indicator_status {
                    0xff
                } else {
                    0x00
                });
                data.extend_from_slice(&server_id.additional_data);
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data,
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC22 - Mask Write Register
    async fn handle_fc22(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        address_locks: &tokio::sync::Mutex<HashMap<u16, Arc<tokio::sync::Mutex<()>>>>,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 6 {
            return Ok(());
        }
        let address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let and_mask = u16::from_be_bytes([adu.data[2], adu.data[3]]);
        let or_mask = u16::from_be_bytes([adu.data[4], adu.data[5]]);

        if !check_range(&[address], &model.address_range().holding_registers) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        let result = Self::with_address_lock(address_locks, &[address], || async {
            model.mask_write_register(address, and_mask, or_mask).await
        })
        .await;

        match result {
            Ok(()) => response(application.encode(adu)).await,
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC23 - Read/Write Multiple Registers
    async fn handle_fc23(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        address_locks: &tokio::sync::Mutex<HashMap<u16, Arc<tokio::sync::Mutex<()>>>>,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() < 10 || adu.data.len() != 9 + adu.data[8] as usize {
            return Ok(());
        }
        let read_address = u16::from_be_bytes([adu.data[0], adu.data[1]]);
        let read_length = u16::from_be_bytes([adu.data[2], adu.data[3]]);
        let write_address = u16::from_be_bytes([adu.data[4], adu.data[5]]);
        let write_length = u16::from_be_bytes([adu.data[6], adu.data[7]]);
        let byte_count = adu.data[8];

        if !(1..=0x007d).contains(&read_length)
            || !(1..=0x0079).contains(&write_length)
            || byte_count as u16 != write_length * 2
        {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataValue),
            )
            .await;
        }

        if !check_range(
            &[
                read_address,
                read_address + read_length,
                write_address,
                write_address + write_length,
            ],
            &model.address_range().holding_registers,
        ) {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalDataAddress),
            )
            .await;
        }

        let write_values: Vec<u16> = (0..write_length)
            .map(|i| {
                u16::from_be_bytes([adu.data[9 + i as usize * 2], adu.data[10 + i as usize * 2]])
            })
            .collect();

        let write_addresses: Vec<u16> = (0..write_length).map(|i| write_address + i).collect();

        let write_result = Self::with_address_lock(address_locks, &write_addresses, || async {
            model
                .write_multiple_registers(write_address, &write_values)
                .await
        })
        .await;

        if let Err(e) = write_result {
            return Self::response_error(application, adu, response, &e).await;
        }

        match model
            .read_holding_registers(read_address, read_length)
            .await
        {
            Ok(registers) => {
                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data: pack_registers(&registers, read_length),
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    // FC43/14 - Read Device Identification
    async fn handle_fc43_14(
        application: &Arc<A>,
        model: &dyn ModbusSlaveModel,
        adu: &ApplicationDataUnit,
        response: SlaveResponseFn,
    ) -> Result<(), ModbusError> {
        if adu.data.len() != 3 || adu.data[0] != 0x0e {
            return Self::response_error(
                application,
                adu,
                response,
                &get_error_by_code(ErrorCode::IllegalFunction),
            )
            .await;
        }

        let read_device_id_code = adu.data[1];
        let mut object_id = adu.data[2];

        match read_device_id_code {
            0x01 => {
                if object_id > 0x02 || (object_id > 0x06 && object_id < 0x80) {
                    object_id = 0x00;
                }
            }
            0x02 => {
                if object_id > 0x06 {
                    object_id = 0x00;
                }
            }
            0x03 => {
                if object_id > 0x06 && object_id < 0x80 {
                    object_id = 0x00;
                }
            }
            0x04 => {
                if object_id > 0x06 && object_id < 0x80 {
                    return Self::response_error(
                        application,
                        adu,
                        response,
                        &get_error_by_code(ErrorCode::IllegalDataAddress),
                    )
                    .await;
                }
            }
            _ => {
                return Self::response_error(
                    application,
                    adu,
                    response,
                    &get_error_by_code(ErrorCode::IllegalDataValue),
                )
                .await;
            }
        }

        match model.read_device_identification().await {
            Ok(identification) => {
                let mut objects: Vec<(u8, String)> = vec![
                    (0x00, "null".to_string()),
                    (0x01, "null".to_string()),
                    (0x02, "null".to_string()),
                ];
                for (k, v) in identification {
                    if let Some(pos) = objects.iter().position(|(id, _)| *id == k) {
                        objects[pos] = (k, v);
                    } else {
                        objects.push((k, v));
                    }
                }
                objects.sort_by_key(|(id, _)| *id);

                let has_object_id = objects.iter().any(|(id, _)| *id == object_id);
                if !has_object_id {
                    if read_device_id_code == 0x04 {
                        return Self::response_error(
                            application,
                            adu,
                            response,
                            &get_error_by_code(ErrorCode::IllegalDataAddress),
                        )
                        .await;
                    }
                    object_id = 0x00;
                }

                let max_id = objects.last().map(|(id, _)| *id).unwrap_or(0);
                // Per Modbus V1.1b3 §6.21, Extended range is 0x80..=0xFF
                // (inclusive at 0x80). Off-by-one here meant an Extended
                // object at exactly 0x80 was under-reported as 0x82.
                let conformity_level = if max_id >= 0x80 {
                    0x83
                } else if max_id > 0x02 {
                    0x82
                } else {
                    0x81
                };

                let mut ids = Vec::new();
                let mut total_length = 10usize;
                let mut last_id = 0u8;

                for &(id, ref value) in &objects {
                    if id < object_id {
                        continue;
                    }
                    if value.len() > 245 {
                        return Self::response_error(
                            application,
                            adu,
                            response,
                            &get_error_by_code(ErrorCode::ServerDeviceFailure),
                        )
                        .await;
                    }
                    if total_length + 2 + value.len() > 253 {
                        if last_id == 0 {
                            last_id = id;
                        }
                        continue;
                    }
                    total_length += 2 + value.len();
                    ids.push(id);
                    if read_device_id_code == 0x04 {
                        break;
                    }
                }

                let mut data = vec![
                    0x0e,
                    read_device_id_code,
                    conformity_level,
                    if last_id == 0 { 0x00 } else { 0xff },
                    last_id,
                    ids.len() as u8,
                ];
                for id in ids {
                    if let Some((_, value)) = objects.iter().find(|(oid, _)| *oid == id) {
                        data.push(id);
                        data.push(value.len() as u8);
                        data.extend_from_slice(value.as_bytes());
                    }
                }

                let response_adu = ApplicationDataUnit {
                    transaction: adu.transaction,
                    unit: adu.unit,
                    fc: adu.fc,
                    data,
                };
                response(application.encode(&response_adu)).await
            }
            Err(e) => Self::response_error(application, adu, response, &e).await,
        }
    }

    pub fn add_custom_function_code(&self, cfc: CustomFunctionCode) {
        self.application.add_custom_function_code(cfc.clone());
        self.custom_function_codes
            .lock()
            .unwrap()
            .insert(cfc.fc, cfc);
    }

    pub fn remove_custom_function_code(&self, fc: u8) {
        self.application.remove_custom_function_code(fc);
        self.custom_function_codes.lock().unwrap().remove(&fc);
    }
}
