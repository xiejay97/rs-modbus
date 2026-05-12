use crate::error::ModbusError;
use crate::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use crate::utils::gen_connection_id;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

/// Tunables for [`UdpPhysicalLayer::new_server_with_options`] /
/// [`UdpPhysicalLayer::new_client_with_options`]. Mirrors njs-modbus
/// `UdpPhysicalLayerOptions`.
#[derive(Clone, Copy, Debug)]
pub struct UdpPhysicalLayerOptions {
    /// Server mode only. Each unique inbound rinfo (`addr:port`) gets its
    /// own [`ConnectionId`]; if no datagram arrives within this many ms,
    /// the connection is evicted and `connection_close` fires so upper-layer
    /// framing state is released. Set to `0` to disable eviction. Default
    /// `30000` (30 seconds).
    pub idle_timeout_ms: u64,
}

impl Default for UdpPhysicalLayerOptions {
    fn default() -> Self {
        Self {
            idle_timeout_ms: 30000,
        }
    }
}

/// Per-rinfo bookkeeping for server mode. The idle timer is replaced on every
/// inbound datagram so that the most recent send slides the eviction deadline
/// forward. Aborting an in-flight timer is preferable to checking `Instant`
/// because eviction must emit `connection_close` exactly-once.
struct RemoteEntry {
    conn: ConnectionId,
    idle_timer: Option<JoinHandle<()>>,
}

type RemoteMap = Arc<Mutex<HashMap<SocketAddr, RemoteEntry>>>;

pub struct UdpPhysicalLayer {
    pub(crate) socket: Arc<Mutex<Option<Arc<UdpSocket>>>>,
    is_open: Arc<Mutex<bool>>,
    is_opening: Arc<Mutex<bool>>,
    is_destroyed: Arc<Mutex<bool>>,
    pub(crate) local_addr: Arc<Mutex<Option<String>>>,
    remote_addr: Arc<Mutex<Option<String>>>,
    is_server: bool,
    idle_timeout_ms: u64,
    /// Server mode: distinct per-rinfo connection state. Client mode keeps a
    /// single entry (lazily created on first valid inbound datagram), so the
    /// same map type works for both at the cost of one extra `HashMap` op
    /// per packet.
    connections: RemoteMap,
    recv_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    data_tx: broadcast::Sender<DataEvent>,
    write_tx: broadcast::Sender<Vec<u8>>,
    error_tx: broadcast::Sender<ModbusError>,
    connection_close_tx: broadcast::Sender<ConnectionId>,
    close_tx: broadcast::Sender<()>,
}

impl UdpPhysicalLayer {
    fn build(
        is_server: bool,
        remote_addr: Option<String>,
        options: UdpPhysicalLayerOptions,
    ) -> Arc<Self> {
        let (data_tx, _) = broadcast::channel(16);
        let (write_tx, _) = broadcast::channel(16);
        let (error_tx, _) = broadcast::channel(16);
        let (connection_close_tx, _) = broadcast::channel(16);
        let (close_tx, _) = broadcast::channel(16);
        Arc::new(Self {
            socket: Arc::new(Mutex::new(None)),
            is_open: Arc::new(Mutex::new(false)),
            is_opening: Arc::new(Mutex::new(false)),
            is_destroyed: Arc::new(Mutex::new(false)),
            local_addr: Arc::new(Mutex::new(None)),
            remote_addr: Arc::new(Mutex::new(remote_addr)),
            is_server,
            idle_timeout_ms: options.idle_timeout_ms,
            connections: Arc::new(Mutex::new(HashMap::new())),
            recv_task: Arc::new(Mutex::new(None)),
            data_tx,
            write_tx,
            error_tx,
            connection_close_tx,
            close_tx,
        })
    }

    pub fn new_server() -> Arc<Self> {
        Self::build(true, None, UdpPhysicalLayerOptions::default())
    }

    pub fn new_server_with_options(options: UdpPhysicalLayerOptions) -> Arc<Self> {
        Self::build(true, None, options)
    }

    pub fn new_client(remote_addr: String) -> Arc<Self> {
        Self::build(false, Some(remote_addr), UdpPhysicalLayerOptions::default())
    }

    pub fn new_client_with_options(
        remote_addr: String,
        options: UdpPhysicalLayerOptions,
    ) -> Arc<Self> {
        Self::build(false, Some(remote_addr), options)
    }

    pub async fn set_local_addr(&self, addr: String) {
        *self.local_addr.lock().await = Some(addr);
    }

    /// Resolves to the currently bound local address (post-`open()`), or
    /// `None` if the socket isn't open.
    pub async fn local_addr(&self) -> Option<String> {
        let guard = self.socket.lock().await;
        guard
            .as_ref()
            .and_then(|s| s.local_addr().ok().map(|a| a.to_string()))
    }
}

/// Resolves the configured remote address to a `SocketAddr` used for source
/// filtering in client mode. Returns `None` for unresolvable or unset
/// addresses (filtering then degrades to "accept anything" rather than
/// silently dropping all replies).
fn parse_remote(remote: &str) -> Option<SocketAddr> {
    remote.parse::<SocketAddr>().ok()
}

/// Decides whether an inbound rinfo should be accepted in client mode. The
/// port must match the configured remote port; the address must match if
/// the configured remote address is bound to a specific (non-wildcard) host.
fn client_accepts(remote: &SocketAddr, rinfo: &SocketAddr) -> bool {
    if remote.port() != rinfo.port() {
        return false;
    }
    let ip = remote.ip();
    if ip.is_unspecified() {
        return true;
    }
    ip == rinfo.ip()
}

#[async_trait::async_trait]
impl PhysicalLayer for UdpPhysicalLayer {
    fn layer_type(&self) -> PhysicalLayerType {
        PhysicalLayerType::Net
    }

    async fn open(&self) -> Result<(), ModbusError> {
        if *self.is_destroyed.lock().await {
            return Err(ModbusError::PortDestroyed);
        }
        {
            let opened = self.is_open.lock().await;
            let opening = self.is_opening.lock().await;
            if *opened || *opening {
                return Err(ModbusError::PortAlreadyOpen);
            }
        }
        *self.is_opening.lock().await = true;

        let bind_addr = if self.is_server {
            self.local_addr
                .lock()
                .await
                .clone()
                .unwrap_or_else(|| "[::]:502".to_string())
        } else {
            self.local_addr
                .lock()
                .await
                .clone()
                .unwrap_or_else(|| "0.0.0.0:0".to_string())
        };
        let socket = match UdpSocket::bind(&bind_addr).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                *self.is_opening.lock().await = false;
                return Err(ModbusError::ConnectionError(e.to_string()));
            }
        };
        *self.socket.lock().await = Some(Arc::clone(&socket));

        // Fresh session — drop any state from prior open/close.
        self.connections.lock().await.clear();

        let remote_filter = if self.is_server {
            None
        } else {
            self.remote_addr
                .lock()
                .await
                .as_deref()
                .and_then(parse_remote)
        };

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let connection_close_tx = self.connection_close_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open_for_task = Arc::clone(&self.is_open);
        let socket_for_task = Arc::clone(&socket);
        let connections_for_task = Arc::clone(&self.connections);
        let is_server = self.is_server;
        let idle_timeout_ms = self.idle_timeout_ms;

        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                match socket_for_task.recv_from(&mut buf).await {
                    Ok((n, addr)) => {
                        // Client mode: drop datagrams whose source rinfo does
                        // not match the configured remote (port required;
                        // address required only when not unspecified).
                        if let Some(remote) = remote_filter {
                            if !client_accepts(&remote, &addr) {
                                continue;
                            }
                        }

                        let data = buf[..n].to_vec();
                        let conn_id = ensure_entry(
                            &connections_for_task,
                            addr,
                            is_server,
                            idle_timeout_ms,
                            &connection_close_tx,
                        )
                        .await;

                        let socket = Arc::clone(&socket_for_task);
                        let response: ResponseFn = Arc::new(move |data: Vec<u8>| {
                            let socket = Arc::clone(&socket);
                            Box::pin(async move {
                                socket
                                    .send_to(&data, addr)
                                    .await
                                    .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
                                Ok(())
                            })
                        });
                        let _ = data_tx.send(DataEvent {
                            data,
                            response,
                            connection: conn_id,
                        });
                    }
                    Err(e) => {
                        let _ = error_tx.send(ModbusError::ConnectionError(e.to_string()));
                        break;
                    }
                }
            }
            // Natural exit (socket errored). Drain all rinfo state, emit
            // connection_close per entry, then close — exactly-once via the
            // is_open transition gate so close() and this task don't both
            // fire the listeners.
            let was_open = {
                let mut g = is_open_for_task.lock().await;
                let prev = *g;
                *g = false;
                prev
            };
            if was_open {
                let drained: Vec<RemoteEntry> = {
                    let mut g = connections_for_task.lock().await;
                    g.drain().map(|(_, v)| v).collect()
                };
                for entry in drained {
                    if let Some(handle) = entry.idle_timer {
                        handle.abort();
                    }
                    let _ = connection_close_tx.send(entry.conn);
                }
                let _ = close_tx.send(());
            }
        });

        *self.recv_task.lock().await = Some(handle);
        *self.is_open.lock().await = true;
        *self.is_opening.lock().await = false;
        Ok(())
    }

    async fn write(&self, data: &[u8]) -> Result<(), ModbusError> {
        if !*self.is_open.lock().await {
            return Err(ModbusError::PortNotOpen);
        }
        let socket = self.socket.lock().await.as_ref().unwrap().clone();
        match *self.remote_addr.lock().await {
            Some(ref remote) => {
                socket
                    .send_to(data, remote)
                    .await
                    .map_err(|e| ModbusError::ConnectionError(e.to_string()))?;
                let _ = self.write_tx.send(data.to_vec());
                Ok(())
            }
            None if self.is_server => Err(ModbusError::ConnectionError(
                "No remote address for server".to_string(),
            )),
            None => Err(ModbusError::ConnectionError(
                "No remote address configured for client".to_string(),
            )),
        }
    }

    async fn close(&self) -> Result<(), ModbusError> {
        let was_open = {
            let mut g = self.is_open.lock().await;
            let prev = *g;
            *g = false;
            prev
        };
        if !was_open {
            return Ok(());
        }
        // Abort the recv task so the socket Arc drops promptly and the
        // bound port is released for the next open().
        if let Some(handle) = self.recv_task.lock().await.take() {
            handle.abort();
        }
        *self.socket.lock().await = None;
        // Drain rinfo state, abort idle timers, emit connection_close per.
        let drained: Vec<RemoteEntry> = {
            let mut g = self.connections.lock().await;
            g.drain().map(|(_, v)| v).collect()
        };
        for entry in drained {
            if let Some(handle) = entry.idle_timer {
                handle.abort();
            }
            let _ = self.connection_close_tx.send(entry.conn);
        }
        let _ = self.close_tx.send(());
        Ok(())
    }

    async fn destroy(&self) {
        *self.is_destroyed.lock().await = true;
        let _ = self.close().await;
    }

    fn is_open(&self) -> bool {
        if let Ok(guard) = self.is_open.try_lock() {
            *guard
        } else {
            false
        }
    }

    fn is_destroyed(&self) -> bool {
        if let Ok(guard) = self.is_destroyed.try_lock() {
            *guard
        } else {
            false
        }
    }

    fn subscribe_data(&self) -> broadcast::Receiver<DataEvent> {
        self.data_tx.subscribe()
    }

    fn subscribe_write(&self) -> broadcast::Receiver<Vec<u8>> {
        self.write_tx.subscribe()
    }

    fn subscribe_error(&self) -> broadcast::Receiver<ModbusError> {
        self.error_tx.subscribe()
    }

    fn subscribe_connection_close(&self) -> broadcast::Receiver<ConnectionId> {
        self.connection_close_tx.subscribe()
    }

    fn subscribe_close(&self) -> broadcast::Receiver<()> {
        self.close_tx.subscribe()
    }
}

/// Look up or insert an entry for `addr`. Server mode arms an idle eviction
/// timer (replacing the previous one) whenever a datagram arrives; client
/// mode never arms a timer. Returns the connection id for this rinfo.
async fn ensure_entry(
    map: &RemoteMap,
    addr: SocketAddr,
    is_server: bool,
    idle_timeout_ms: u64,
    connection_close_tx: &broadcast::Sender<ConnectionId>,
) -> ConnectionId {
    let label = if is_server {
        "udp-server"
    } else {
        "udp-client"
    };
    let mut guard = map.lock().await;
    let entry = guard.entry(addr).or_insert_with(|| RemoteEntry {
        conn: Arc::from(gen_connection_id(label)),
        idle_timer: None,
    });
    let conn = Arc::clone(&entry.conn);

    if is_server && idle_timeout_ms > 0 {
        if let Some(handle) = entry.idle_timer.take() {
            handle.abort();
        }
        let map = Arc::clone(map);
        let conn_id = Arc::clone(&conn);
        let close_tx = connection_close_tx.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(idle_timeout_ms)).await;
            let removed = {
                let mut g = map.lock().await;
                match g.get(&addr) {
                    Some(e) if Arc::ptr_eq(&e.conn, &conn_id) => g.remove(&addr),
                    _ => None,
                }
            };
            if removed.is_some() {
                let _ = close_tx.send(conn_id);
            }
        });
        entry.idle_timer = Some(timer);
    }

    conn
}
