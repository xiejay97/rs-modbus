use crate::error::ModbusError;
use crate::layers::physical::{
    ConnectionId, DataEvent, PhysicalLayer, PhysicalLayerType, ResponseFn,
};
use crate::utils::gen_connection_id;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

struct ClientState {
    write_half: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    read_task: JoinHandle<()>,
}

pub struct TcpServerPhysicalLayer {
    is_open: Arc<Mutex<bool>>,
    is_opening: Arc<Mutex<bool>>,
    is_destroyed: Arc<Mutex<bool>>,
    pub(crate) addr: Arc<Mutex<Option<String>>>,
    clients: Arc<Mutex<HashMap<ConnectionId, ClientState>>>,
    accept_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    data_tx: broadcast::Sender<DataEvent>,
    write_tx: broadcast::Sender<Vec<u8>>,
    error_tx: broadcast::Sender<ModbusError>,
    connection_close_tx: broadcast::Sender<ConnectionId>,
    close_tx: broadcast::Sender<()>,
}

impl TcpServerPhysicalLayer {
    pub fn new() -> Arc<Self> {
        let (data_tx, _) = broadcast::channel(16);
        let (write_tx, _) = broadcast::channel(16);
        let (error_tx, _) = broadcast::channel(16);
        let (connection_close_tx, _) = broadcast::channel(16);
        let (close_tx, _) = broadcast::channel(16);
        Arc::new(Self {
            is_open: Arc::new(Mutex::new(false)),
            is_opening: Arc::new(Mutex::new(false)),
            is_destroyed: Arc::new(Mutex::new(false)),
            addr: Arc::new(Mutex::new(None)),
            clients: Arc::new(Mutex::new(HashMap::new())),
            accept_task: Arc::new(Mutex::new(None)),
            data_tx,
            write_tx,
            error_tx,
            connection_close_tx,
            close_tx,
        })
    }

    pub async fn set_addr(&self, addr: String) {
        *self.addr.lock().await = Some(addr);
    }

    pub async fn get_addr(&self) -> Option<String> {
        self.addr.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl PhysicalLayer for TcpServerPhysicalLayer {
    type OpenOptions = Option<String>;

    fn layer_type(&self) -> PhysicalLayerType {
        PhysicalLayerType::Net
    }

    async fn open(&self, options: Self::OpenOptions) -> Result<(), ModbusError> {
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

        let addr = if let Some(addr) = options {
            addr
        } else {
            self.addr.lock().await.clone().unwrap_or_else(|| "[::]:502".to_string())
        };
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                *self.is_opening.lock().await = false;
                return Err(ModbusError::ConnectionError(e.to_string()));
            }
        };
        *self.addr.lock().await = Some(listener.local_addr().unwrap().to_string());
        // Fresh session — drop any lingering state from prior open/close cycle.
        self.clients.lock().await.clear();

        let data_tx = self.data_tx.clone();
        let error_tx = self.error_tx.clone();
        let connection_close_tx = self.connection_close_tx.clone();
        let close_tx = self.close_tx.clone();
        let is_open_for_accept = Arc::clone(&self.is_open);
        let clients = Arc::clone(&self.clients);

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let (mut read_half, write_half) = stream.into_split();
                        let write_half = Arc::new(Mutex::new(write_half));
                        let conn_id: ConnectionId = Arc::from(gen_connection_id("tcp-server"));
                        let data_tx = data_tx.clone();
                        let error_tx = error_tx.clone();
                        let connection_close_tx = connection_close_tx.clone();
                        let clients_for_task = Arc::clone(&clients);
                        let conn_id_for_task = Arc::clone(&conn_id);
                        let write_half_for_task = Arc::clone(&write_half);

                        let task = tokio::spawn(async move {
                            let mut buf = vec![0u8; 1024];
                            loop {
                                match read_half.read(&mut buf).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        let data = buf[..n].to_vec();
                                        let wh = Arc::clone(&write_half_for_task);
                                        let response: ResponseFn =
                                            Arc::new(move |data: Vec<u8>| {
                                                let wh = Arc::clone(&wh);
                                                Box::pin(async move {
                                                    let mut s = wh.lock().await;
                                                    s.write_all(&data).await.map_err(|e| {
                                                        ModbusError::ConnectionError(e.to_string())
                                                    })?;
                                                    Ok(())
                                                })
                                            });
                                        let _ = data_tx.send(DataEvent {
                                            data,
                                            response,
                                            connection: Arc::clone(&conn_id_for_task),
                                        });
                                    }
                                    Err(e) => {
                                        let _ = error_tx
                                            .send(ModbusError::ConnectionError(e.to_string()));
                                        break;
                                    }
                                }
                            }
                            // Natural disconnect: remove from the clients map. If
                            // close() already drained the map, our removal returns
                            // None and we skip the emit. This keeps connection_close
                            // exactly-once per client per session.
                            let removed = clients_for_task
                                .lock()
                                .await
                                .remove(&conn_id_for_task)
                                .is_some();
                            if removed {
                                let _ = connection_close_tx.send(conn_id_for_task);
                            }
                        });

                        clients.lock().await.insert(
                            Arc::clone(&conn_id),
                            ClientState {
                                write_half: Arc::clone(&write_half),
                                read_task: task,
                            },
                        );
                    }
                    Err(e) => {
                        let _ = error_tx.send(ModbusError::ConnectionError(e.to_string()));
                        break;
                    }
                }
            }
            // Natural exit of accept loop (listener errored). Emit close exactly
            // once via the is_open transition gate; if close() already flipped
            // is_open to false, it owns the emit.
            let was_open = {
                let mut g = is_open_for_accept.lock().await;
                let prev = *g;
                *g = false;
                prev
            };
            if was_open {
                let _ = close_tx.send(());
            }
        });

        *self.accept_task.lock().await = Some(handle);
        *self.is_open.lock().await = true;
        *self.is_opening.lock().await = false;
        Ok(())
    }

    async fn write(&self, _data: &[u8]) -> Result<(), ModbusError> {
        Err(ModbusError::NotSupported)
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
        // Stop accepting new connections; aborting drops the listener and
        // releases the bound port for the next open().
        if let Some(handle) = self.accept_task.lock().await.take() {
            handle.abort();
        }
        // Drain active clients, abort their read tasks, drop their write halves
        // (sends FIN to each peer), emit connection_close exactly once each.
        let drained: Vec<(ConnectionId, ClientState)> = {
            let mut g = self.clients.lock().await;
            g.drain().collect()
        };
        for (conn_id, state) in drained {
            state.read_task.abort();
            // Drop the write half so the socket fully releases.
            drop(state.write_half);
            let _ = self.connection_close_tx.send(conn_id);
        }
        let _ = self.close_tx.send(());
        Ok(())
    }

    async fn destroy(&self) {
        if *self.is_destroyed.lock().await {
            return;
        }
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
