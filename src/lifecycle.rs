//! The server lifecycle as a finite state machine.
//!
//! ```mermaid
//! stateDiagram-v2
//!     [*] --> Idle
//!     Idle --> Serving: Bound
//!     Idle --> Failed: BindFailed
//!     Idle --> Stopped: ShutdownRequested
//!     Serving --> Draining: ShutdownRequested
//!     Serving --> Failed: ServerExited
//!     Draining --> Stopped: Drained / ServerExited
//!     Stopped --> [*]
//!     Failed --> [*]
//! ```
//!
//! [`Lifecycle::next`] is the only transition function. It is pure and
//! total. Illegal events return `None` and do not change the state.

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};

/// The state of one gRPC server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lifecycle {
    /// Not started yet.
    Idle,
    /// The listener is bound and accepts calls.
    Serving,
    /// Shutdown started. No new connections. In-flight calls finish.
    Draining,
    /// The server stopped. Terminal.
    Stopped,
    /// The server could not start, or stopped unexpectedly. Terminal.
    Failed,
}

/// An input to the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleEvent {
    /// The listener is bound.
    Bound,
    /// The listener could not start.
    BindFailed,
    /// The app asked the server to stop.
    ShutdownRequested,
    /// All in-flight calls ended, or the grace period expired.
    Drained,
    /// The server task ended.
    ServerExited,
}

impl Lifecycle {
    /// The next state after `event`, or `None` if `event` is illegal here.
    #[must_use]
    pub const fn next(self, event: LifecycleEvent) -> Option<Self> {
        use LifecycleEvent::{BindFailed, Bound, Drained, ServerExited, ShutdownRequested};
        match (self, event) {
            (Self::Idle, Bound) => Some(Self::Serving),
            (Self::Idle, BindFailed) => Some(Self::Failed),
            (Self::Idle, ShutdownRequested) => Some(Self::Stopped),
            (Self::Serving, ShutdownRequested) => Some(Self::Draining),
            (Self::Serving, ServerExited) => Some(Self::Failed),
            (Self::Draining, Drained | ServerExited) => Some(Self::Stopped),
            _ => None,
        }
    }

    /// `true` only in [`Serving`](Self::Serving).
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Serving)
    }

    /// `true` in [`Stopped`](Self::Stopped) and [`Failed`](Self::Failed).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }

    /// Lower-case name, as used in health details.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Serving => "serving",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }

    const fn to_u8(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Serving => 1,
            Self::Draining => 2,
            Self::Stopped => 3,
            Self::Failed => 4,
        }
    }

    const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Idle,
            1 => Self::Serving,
            2 => Self::Draining,
            3 => Self::Stopped,
            _ => Self::Failed,
        }
    }
}

impl fmt::Display for Lifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A [`Lifecycle`] shared between threads. It changes only through
/// [`Lifecycle::next`].
#[derive(Debug)]
pub struct LifecycleCell(AtomicU8);

impl LifecycleCell {
    /// A cell in [`Lifecycle::Idle`].
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicU8::new(Lifecycle::Idle.to_u8()))
    }

    /// The current state.
    #[must_use]
    pub fn get(&self) -> Lifecycle {
        Lifecycle::from_u8(self.0.load(Ordering::Acquire))
    }

    /// Apply `event` atomically.
    ///
    /// # Errors
    ///
    /// Returns the unchanged state when `event` is illegal in it.
    pub fn apply(&self, event: LifecycleEvent) -> Result<Lifecycle, Lifecycle> {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            let state = Lifecycle::from_u8(current);
            let next = state.next(event).ok_or(state)?;
            match self.0.compare_exchange_weak(
                current,
                next.to_u8(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(next),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Default for LifecycleCell {
    fn default() -> Self {
        Self::new()
    }
}
