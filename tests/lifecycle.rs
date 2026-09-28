//! Lifecycle state machine: AC12.
//!
//! The machine is finite (5 states × 5 events). The table test checks every
//! pair, so it is a complete model check. The property tests check the
//! invariants over random event sequences.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use autumn_plugin_grpc::{Lifecycle, LifecycleEvent};
use proptest::prelude::*;

use Lifecycle::{Draining, Failed, Idle, Serving, Stopped};
use LifecycleEvent::{BindFailed, Bound, Drained, ServerExited, ShutdownRequested};

const STATES: [Lifecycle; 5] = [Idle, Serving, Draining, Stopped, Failed];
const EVENTS: [LifecycleEvent; 5] = [Bound, BindFailed, ShutdownRequested, Drained, ServerExited];

/// The specification: the only legal transitions.
fn spec(state: Lifecycle, event: LifecycleEvent) -> Option<Lifecycle> {
    match (state, event) {
        (Idle, Bound) => Some(Serving),
        (Idle, BindFailed) => Some(Failed),
        (Idle, ShutdownRequested) => Some(Stopped),
        (Serving, ShutdownRequested) => Some(Draining),
        (Serving, ServerExited) => Some(Failed),
        (Draining, Drained | ServerExited) => Some(Stopped),
        _ => None,
    }
}

#[test]
fn every_state_event_pair_matches_the_spec() {
    for state in STATES {
        for event in EVENTS {
            assert_eq!(
                state.next(event),
                spec(state, event),
                "{state:?} + {event:?}"
            );
        }
    }
}

#[test]
fn only_serving_is_ready() {
    for state in STATES {
        assert_eq!(state.is_ready(), state == Serving, "{state:?}");
    }
}

#[test]
fn terminal_states_are_stopped_and_failed() {
    for state in STATES {
        assert_eq!(
            state.is_terminal(),
            matches!(state, Stopped | Failed),
            "{state:?}"
        );
    }
}

#[test]
fn names_are_stable() {
    let names: Vec<&str> = STATES.iter().map(|s| s.as_str()).collect();
    assert_eq!(names, ["idle", "serving", "draining", "stopped", "failed"]);
    assert_eq!(Serving.to_string(), "serving");
}

#[test]
fn the_shared_cell_applies_legal_events_only() {
    let cell = autumn_plugin_grpc::LifecycleCell::new();
    assert_eq!(cell.get(), Idle);
    assert_eq!(cell.apply(Drained), Err(Idle));
    assert_eq!(cell.apply(Bound), Ok(Serving));
    assert_eq!(cell.apply(Bound), Err(Serving));
    assert_eq!(cell.apply(ShutdownRequested), Ok(Draining));
    assert_eq!(cell.apply(Drained), Ok(Stopped));
    assert_eq!(cell.get(), Stopped);
}

fn event() -> impl Strategy<Value = LifecycleEvent> {
    prop::sample::select(EVENTS.to_vec())
}

const fn rank(state: Lifecycle) -> u8 {
    match state {
        Idle => 0,
        Serving => 1,
        Draining => 2,
        Stopped | Failed => 3,
    }
}

proptest! {
    #[test]
    fn progress_is_monotonic_and_terminals_absorb(events in prop::collection::vec(event(), 0..64)) {
        let mut state = Idle;
        for event in events {
            let before = state;
            if let Some(after) = state.next(event) {
                prop_assert!(rank(after) > rank(before), "{before:?} -> {after:?}");
                prop_assert!(!before.is_terminal());
                state = after;
            }
            if before.is_terminal() {
                prop_assert_eq!(state, before);
            }
        }
    }

    #[test]
    fn serving_is_reached_only_through_bound(events in prop::collection::vec(event(), 0..64)) {
        let mut state = Idle;
        for event in events {
            if let Some(after) = state.next(event) {
                if after == Serving {
                    prop_assert_eq!(state, Idle);
                    prop_assert_eq!(event, Bound);
                }
                state = after;
            }
        }
    }

    #[test]
    fn the_cell_agrees_with_the_pure_function(events in prop::collection::vec(event(), 0..64)) {
        let cell = autumn_plugin_grpc::LifecycleCell::new();
        let mut state = Idle;
        for event in events {
            let expected = state.next(event);
            let got = cell.apply(event);
            match expected {
                Some(next) => { prop_assert_eq!(got, Ok(next)); state = next; }
                None => prop_assert_eq!(got, Err(state)),
            }
            prop_assert_eq!(cell.get(), state);
        }
    }
}
