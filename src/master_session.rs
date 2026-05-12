//! `MasterSession` — owns the "currently awaiting response" state of a
//! `ModbusMaster`. Mirrors njs-modbus' `master-session.ts`.
//!
//! The session is decoupled from any concrete transport. The master pushes
//! framing events into [`MasterSession::handle_frame`]; the session applies a
//! pre-check chain and either resolves the awaiting receiver with the frame
//! or rejects it with a [`ModbusError`].

use crate::error::ModbusError;
use crate::layers::application::Framing;
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
    /// Equivalent to njs `undefined`: the check can't decide yet; this is
    /// always treated as `InsufficientData` (we don't keep partial state in
    /// the session because the application layer has already buffered the
    /// physical input — by the time `handle_frame` is called, the frame is
    /// complete or the framing layer would have raised `framing_error`).
    InsufficientData,
}

/// A single pre-check predicate applied to a [`Framing`].
pub type PreCheck = Arc<dyn Fn(&Framing) -> PreCheckOutcome + Send + Sync>;

struct WaitingState {
    pre_check: Vec<PreCheck>,
    sender: oneshot::Sender<Result<Framing, ModbusError>>,
}

/// Owns the "currently awaiting response" slot for a master. The master calls
/// [`start_waiting`] when it sends a request, and [`stop_waiting`] when the
/// response arrives or times out. [`handle_frame`] /[`handle_error`] are
/// called by the master's framing/framing_error subscription tasks.
pub struct MasterSession {
    waiting: Mutex<Option<WaitingState>>,
}

impl MasterSession {
    pub fn new() -> Self {
        Self {
            waiting: Mutex::new(None),
        }
    }

    /// Arm the session to wait for a frame that satisfies every `pre_check`.
    /// Returns a receiver that resolves with either the matching `Framing` or
    /// the first rejection reason.
    pub fn start_waiting(
        &self,
        pre_check: Vec<PreCheck>,
    ) -> oneshot::Receiver<Result<Framing, ModbusError>> {
        let (tx, rx) = oneshot::channel();
        let mut guard = self.waiting.lock().unwrap();
        *guard = Some(WaitingState {
            pre_check,
            sender: tx,
        });
        rx
    }

    /// Drop any armed waiter without notifying it. Used on timeout, where the
    /// caller has already given up on the receiver.
    pub fn stop_waiting(&self) {
        *self.waiting.lock().unwrap() = None;
    }

    /// Push a successfully framed PDU at the session. If the session is
    /// armed, run the pre-checks; on the first failing check, reject with
    /// the corresponding error and clear the slot. On all checks passing,
    /// resolve with the frame and clear the slot.
    pub fn handle_frame(&self, frame: Framing) {
        let mut guard = self.waiting.lock().unwrap();
        if guard.is_none() {
            return;
        }
        let outcome = run_pre_checks(&frame, &guard.as_ref().unwrap().pre_check);
        let state = guard.take().unwrap();
        drop(guard);
        match outcome {
            CheckResult::Pass => {
                let _ = state.sender.send(Ok(frame));
            }
            CheckResult::Reject(err) => {
                let _ = state.sender.send(Err(err));
            }
        }
    }

    /// Push a framing error at the session. If armed, reject the waiter and
    /// clear the slot; otherwise no-op.
    pub fn handle_error(&self, err: ModbusError) {
        let state = self.waiting.lock().unwrap().take();
        if let Some(s) = state {
            let _ = s.sender.send(Err(err));
        }
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

    fn always_pass() -> PreCheck {
        Arc::new(|_| PreCheckOutcome::Pass)
    }

    #[tokio::test]
    async fn test_start_then_matching_frame_resolves() {
        let session = MasterSession::new();
        let rx = session.start_waiting(vec![always_pass()]);
        session.handle_frame(fake_framing(1, 0x03, vec![0x01]));
        let result = rx.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().adu.unit, 1);
    }

    #[tokio::test]
    async fn test_handle_frame_when_not_waiting_is_noop() {
        let session = MasterSession::new();
        session.handle_frame(fake_framing(1, 0x03, vec![]));
        // No waiter armed — nothing to assert beyond "did not panic". Arm
        // afterwards and confirm the slot is fresh.
        let rx = session.start_waiting(vec![always_pass()]);
        session.handle_frame(fake_framing(2, 0x04, vec![]));
        let resolved = rx.await.unwrap().unwrap();
        assert_eq!(resolved.adu.unit, 2);
    }

    #[tokio::test]
    async fn test_handle_error_rejects_waiter() {
        let session = MasterSession::new();
        let rx = session.start_waiting(vec![always_pass()]);
        session.handle_error(ModbusError::Timeout);
        let result = rx.await.unwrap();
        assert!(matches!(result, Err(ModbusError::Timeout)));
    }

    #[tokio::test]
    async fn test_pre_check_fail_returns_error() {
        let session = MasterSession::new();
        let fail: PreCheck =
            Arc::new(|_| PreCheckOutcome::Fail(ModbusError::IllegalDataAddress));
        let rx = session.start_waiting(vec![fail]);
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
        let rx = session.start_waiting(vec![insuff]);
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
        let rx = session.start_waiting(vec![check]);
        session.handle_frame(fake_framing(1, 0x03, vec![1, 2, 3]));
        assert!(rx.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_need_length_too_short_rejects_insufficient() {
        let session = MasterSession::new();
        let check: PreCheck = Arc::new(|_| PreCheckOutcome::NeedLength(5));
        let rx = session.start_waiting(vec![check]);
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
        let rx = session.start_waiting(vec![check]);
        session.handle_frame(fake_framing(1, 0x03, vec![1, 2, 3, 4]));
        assert!(matches!(
            rx.await.unwrap(),
            Err(ModbusError::InvalidResponse)
        ));
    }

    #[tokio::test]
    async fn test_stop_waiting_makes_handle_frame_noop() {
        let session = MasterSession::new();
        let rx = session.start_waiting(vec![always_pass()]);
        session.stop_waiting();
        session.handle_frame(fake_framing(1, 0x03, vec![]));
        // The receiver should be dropped, so awaiting yields RecvError.
        assert!(rx.await.is_err());
    }
}
