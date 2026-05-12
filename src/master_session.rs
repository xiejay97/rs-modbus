//! `MasterSession` — owns the in-flight "awaiting response" slots of a
//! [`ModbusMaster`]. Multi-slot, keyed by [`WaiterKey`]: TCP requests key
//! by their transaction ID (TID), FIFO/RTU/ASCII requests share the
//! [`WaiterKey::Fifo`] slot since they have no TID to disambiguate by.
//!
//! Mirrors njs-modbus `MasterSession` after the FIFO + TID-validation
//! commit. The master pushes framing events into [`MasterSession::handle_frame`];
//! the session looks up the keyed waiter, applies the pre-check chain, and
//! either resolves the awaiting receiver with the frame or rejects it with
//! a [`ModbusError`].

use crate::error::ModbusError;
use crate::layers::application::Framing;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Outcome of a single `PreCheck` evaluation against an incoming [`Framing`].
///
/// Mirrors the return values of `master-session.ts`'s `preCheck` functions
/// (`undefined | number | boolean`).
#[derive(Clone, Debug)]
pub enum PreCheckOutcome {
    /// This check accepts the frame; move on to the next check.
    Pass,
    /// The frame's `data` must be exactly this many bytes:
    /// - `data.len() < n`  → rejected as `InsufficientData`
    /// - `data.len() != n` → rejected as `InvalidResponse`
    /// - `data.len() == n` → passes
    NeedLength(usize),
    /// Reject with the given error and stop pre-checking.
    Fail(ModbusError),
    /// Equivalent to njs `undefined`: the check can't decide yet; treated
    /// as `InsufficientData`.
    InsufficientData,
}

/// A single pre-check predicate applied to a [`Framing`].
pub type PreCheck = Arc<dyn Fn(&Framing) -> PreCheckOutcome + Send + Sync>;

/// Key used to route an incoming frame to the right pending waiter.
///
/// - [`WaiterKey::Tid`]: TCP requests get a fresh transaction ID per
///   request; the slave echoes that TID in its response, so the master can
///   demux pipelined responses back to their requesters even when several
///   are in flight at once.
/// - [`WaiterKey::Fifo`]: RTU and ASCII have no transaction ID, and FIFO
///   serialization guarantees there is at most one outstanding waiter at
///   any given time, so a single shared key is sufficient.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum WaiterKey {
    Tid(u16),
    Fifo,
}

struct WaitingState {
    pre_check: Vec<PreCheck>,
    sender: oneshot::Sender<Result<Framing, ModbusError>>,
}

/// Owns the "awaiting response" slot(s) for a master. The master calls
/// [`MasterSession::start`] when it sends a request, and [`MasterSession::stop`]
/// when the response arrives or times out. [`MasterSession::handle_frame`] /
/// [`MasterSession::handle_error`] are called by the master's framing /
/// framing_error subscription tasks.
pub struct MasterSession {
    waiters: Mutex<HashMap<WaiterKey, WaitingState>>,
}

impl MasterSession {
    pub fn new() -> Self {
        Self {
            waiters: Mutex::new(HashMap::new()),
        }
    }

    /// Arm a waiter under `key`. Returns a receiver that resolves with
    /// either the first matching `Framing` or a rejection reason.
    ///
    /// If `key` already has a waiter (e.g. TID wrap collision), the
    /// previous waiter's receiver is dropped — equivalent to the caller
    /// calling [`MasterSession::stop`] first.
    pub fn start(
        &self,
        key: WaiterKey,
        pre_check: Vec<PreCheck>,
    ) -> oneshot::Receiver<Result<Framing, ModbusError>> {
        let (tx, rx) = oneshot::channel();
        let mut guard = self.waiters.lock().unwrap();
        guard.insert(
            key,
            WaitingState {
                pre_check,
                sender: tx,
            },
        );
        rx
    }

    /// Drop the waiter under `key` without notifying it. Used on timeout,
    /// where the caller has already given up on the receiver.
    pub fn stop(&self, key: WaiterKey) {
        self.waiters.lock().unwrap().remove(&key);
    }

    /// Reject every armed waiter with `err`. Used by `handle_error`
    /// (framing errors lose transaction context) and on master
    /// close/destroy.
    pub fn stop_all(&self, err: ModbusError) {
        let drained: Vec<WaitingState> = {
            let mut guard = self.waiters.lock().unwrap();
            guard.drain().map(|(_, v)| v).collect()
        };
        for w in drained {
            let _ = w.sender.send(Err(err.clone()));
        }
    }

    /// True if a waiter is currently armed under `key`.
    pub fn has(&self, key: WaiterKey) -> bool {
        self.waiters.lock().unwrap().contains_key(&key)
    }

    /// Push a successfully framed PDU at the session. Looks up the waiter
    /// keyed by `frame.adu.transaction` (TCP) or `WaiterKey::Fifo`
    /// (RTU/ASCII). If found, removes it and runs the pre-checks; on the
    /// first failing check, rejects with the corresponding error. On all
    /// checks passing, resolves with the frame. No-op if no waiter matches.
    pub fn handle_frame(&self, frame: Framing) {
        let key = match frame.adu.transaction {
            Some(tid) => WaiterKey::Tid(tid),
            None => WaiterKey::Fifo,
        };
        let state = {
            let mut guard = self.waiters.lock().unwrap();
            guard.remove(&key)
        };
        let Some(state) = state else { return };
        match run_pre_checks(&frame, &state.pre_check) {
            CheckResult::Pass => {
                let _ = state.sender.send(Ok(frame));
            }
            CheckResult::Reject(err) => {
                let _ = state.sender.send(Err(err));
            }
        }
    }

    /// Push a framing error at the session. Framing errors arrive without
    /// transaction context (CRC/LRC failure, bogus MBAP header, etc.), so
    /// every in-flight waiter is rejected.
    pub fn handle_error(&self, err: ModbusError) {
        self.stop_all(err);
    }
}

impl Default for MasterSession {
    fn default() -> Self {
        Self::new()
    }
}

enum CheckResult {
    Pass,
    Reject(ModbusError),
}

fn run_pre_checks(frame: &Framing, checks: &[PreCheck]) -> CheckResult {
    for check in checks {
        match check(frame) {
            PreCheckOutcome::Pass => continue,
            PreCheckOutcome::NeedLength(n) => {
                if frame.adu.data.len() < n {
                    return CheckResult::Reject(ModbusError::InsufficientData);
                }
                if frame.adu.data.len() != n {
                    return CheckResult::Reject(ModbusError::InvalidResponse);
                }
                // exact length — continue
            }
            PreCheckOutcome::Fail(err) => return CheckResult::Reject(err),
            PreCheckOutcome::InsufficientData => {
                return CheckResult::Reject(ModbusError::InsufficientData);
            }
        }
    }
    CheckResult::Pass
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::physical::{ConnectionId, ResponseFn};
    use crate::types::ApplicationDataUnit;

    fn fake_framing(unit: u8, fc: u8, data: Vec<u8>) -> Framing {
        let response: ResponseFn = Arc::new(|_| Box::pin(async { Ok(()) }));
        let connection: ConnectionId = Arc::from("test");
        Framing {
            adu: ApplicationDataUnit::new(unit, fc, data.clone()),
            raw: data,
            response,
            connection,
        }
    }

    fn fake_framing_with_tid(unit: u8, fc: u8, data: Vec<u8>, tid: u16) -> Framing {
        let mut f = fake_framing(unit, fc, data);
        f.adu.transaction = Some(tid);
        f
    }

    fn always_pass() -> PreCheck {
        Arc::new(|_| PreCheckOutcome::Pass)
    }

    #[tokio::test]
    async fn test_fifo_waiter_resolves_on_matching_frame() {
        let session = MasterSession::new();
        let rx = session.start(WaiterKey::Fifo, vec![always_pass()]);
        session.handle_frame(fake_framing(1, 0x03, vec![0x01]));
        let result = rx.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().adu.unit, 1);
    }

    #[tokio::test]
    async fn test_handle_frame_with_no_waiter_is_noop() {
        let session = MasterSession::new();
        session.handle_frame(fake_framing(1, 0x03, vec![]));
        let rx = session.start(WaiterKey::Fifo, vec![always_pass()]);
        session.handle_frame(fake_framing(2, 0x04, vec![]));
        let resolved = rx.await.unwrap().unwrap();
        assert_eq!(resolved.adu.unit, 2);
    }

    #[tokio::test]
    async fn test_handle_error_rejects_every_waiter() {
        let session = MasterSession::new();
        let rx_fifo = session.start(WaiterKey::Fifo, vec![always_pass()]);
        let rx_tid = session.start(WaiterKey::Tid(7), vec![always_pass()]);
        session.handle_error(ModbusError::Timeout);
        assert!(matches!(rx_fifo.await.unwrap(), Err(ModbusError::Timeout)));
        assert!(matches!(rx_tid.await.unwrap(), Err(ModbusError::Timeout)));
    }

    #[tokio::test]
    async fn test_stop_drops_waiter_silently() {
        let session = MasterSession::new();
        let rx = session.start(WaiterKey::Fifo, vec![always_pass()]);
        session.stop(WaiterKey::Fifo);
        session.handle_frame(fake_framing(1, 0x03, vec![]));
        assert!(rx.await.is_err()); // sender dropped → RecvError
    }

    #[tokio::test]
    async fn test_stop_all_rejects_each_waiter_independently() {
        let session = MasterSession::new();
        let rx_a = session.start(WaiterKey::Tid(1), vec![always_pass()]);
        let rx_b = session.start(WaiterKey::Tid(2), vec![always_pass()]);
        session.stop_all(ModbusError::InvalidState("Master closed".into()));
        assert!(matches!(
            rx_a.await.unwrap(),
            Err(ModbusError::InvalidState(ref s)) if s == "Master closed"
        ));
        assert!(matches!(
            rx_b.await.unwrap(),
            Err(ModbusError::InvalidState(ref s)) if s == "Master closed"
        ));
    }

    #[tokio::test]
    async fn test_has_returns_correct_state() {
        let session = MasterSession::new();
        assert!(!session.has(WaiterKey::Fifo));
        let _rx = session.start(WaiterKey::Fifo, vec![always_pass()]);
        assert!(session.has(WaiterKey::Fifo));
        assert!(!session.has(WaiterKey::Tid(0)));
        session.stop(WaiterKey::Fifo);
        assert!(!session.has(WaiterKey::Fifo));
    }

    #[tokio::test]
    async fn test_tid_routing_isolates_independent_waiters() {
        let session = MasterSession::new();
        let rx_tid7 = session.start(WaiterKey::Tid(7), vec![always_pass()]);
        let rx_tid8 = session.start(WaiterKey::Tid(8), vec![always_pass()]);

        // Push tid=8 first — only tid8 waiter should resolve.
        session.handle_frame(fake_framing_with_tid(1, 0x03, vec![], 8));
        let resolved8 = rx_tid8.await.unwrap().unwrap();
        assert_eq!(resolved8.adu.transaction, Some(8));

        // tid=7 still pending.
        assert!(session.has(WaiterKey::Tid(7)));

        session.handle_frame(fake_framing_with_tid(1, 0x03, vec![], 7));
        let resolved7 = rx_tid7.await.unwrap().unwrap();
        assert_eq!(resolved7.adu.transaction, Some(7));
    }

    #[tokio::test]
    async fn test_fifo_frame_does_not_resolve_tid_waiter() {
        let session = MasterSession::new();
        let rx = session.start(WaiterKey::Tid(7), vec![always_pass()]);
        // RTU-style frame without TID lands on the FIFO slot — not tid=7.
        session.handle_frame(fake_framing(1, 0x03, vec![]));
        assert!(session.has(WaiterKey::Tid(7)));
        session.stop(WaiterKey::Tid(7));
        assert!(rx.await.is_err());
    }

    #[tokio::test]
    async fn test_pre_check_fail_returns_error() {
        let session = MasterSession::new();
        let fail: PreCheck = Arc::new(|_| PreCheckOutcome::Fail(ModbusError::IllegalDataAddress));
        let rx = session.start(WaiterKey::Fifo, vec![fail]);
        session.handle_frame(fake_framing(1, 0x03, vec![]));
        assert!(matches!(
            rx.await.unwrap(),
            Err(ModbusError::IllegalDataAddress)
        ));
    }

    #[tokio::test]
    async fn test_pre_check_insufficient_data_returns_error() {
        let session = MasterSession::new();
        let insuff: PreCheck = Arc::new(|_| PreCheckOutcome::InsufficientData);
        let rx = session.start(WaiterKey::Fifo, vec![insuff]);
        session.handle_frame(fake_framing(1, 0x03, vec![]));
        assert!(matches!(
            rx.await.unwrap(),
            Err(ModbusError::InsufficientData)
        ));
    }

    #[tokio::test]
    async fn test_need_length_exact_passes() {
        let session = MasterSession::new();
        let check: PreCheck = Arc::new(|_| PreCheckOutcome::NeedLength(3));
        let rx = session.start(WaiterKey::Fifo, vec![check]);
        session.handle_frame(fake_framing(1, 0x03, vec![1, 2, 3]));
        assert!(rx.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_need_length_too_short_rejects_insufficient() {
        let session = MasterSession::new();
        let check: PreCheck = Arc::new(|_| PreCheckOutcome::NeedLength(5));
        let rx = session.start(WaiterKey::Fifo, vec![check]);
        session.handle_frame(fake_framing(1, 0x03, vec![1, 2, 3]));
        assert!(matches!(
            rx.await.unwrap(),
            Err(ModbusError::InsufficientData)
        ));
    }

    #[tokio::test]
    async fn test_need_length_too_long_rejects_invalid_response() {
        let session = MasterSession::new();
        let check: PreCheck = Arc::new(|_| PreCheckOutcome::NeedLength(2));
        let rx = session.start(WaiterKey::Fifo, vec![check]);
        session.handle_frame(fake_framing(1, 0x03, vec![1, 2, 3, 4]));
        assert!(matches!(
            rx.await.unwrap(),
            Err(ModbusError::InvalidResponse)
        ));
    }
}
