use std::sync::Arc;

use crate::api::schema::{
    InputIntentOperation, InputIntentPolicy, InputIntentSession, InputIntentState,
    TerminalInputIntents,
};
use crate::terminal::TerminalId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputIntentError {
    InvalidRequest,
    NoLease,
    StaleLease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreUpdate {
    pub generation: u64,
    pub changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionLifecycle {
    Open,
    Active,
    Blurred,
    Suspended,
    Retired,
}

impl SessionLifecycle {
    fn included_in_roster(self) -> bool {
        matches!(
            self,
            Self::Open | Self::Active | Self::Blurred | Self::Suspended
        )
    }
}

#[derive(Debug, Clone)]
struct StoredSession {
    terminal_id: TerminalId,
    session: String,
    generation: u64,
    policy: InputIntentPolicy,
    state: InputIntentState,
    lifecycle: SessionLifecycle,
}

#[derive(Debug, Default)]
pub(crate) struct InputIntentStore {
    sessions: Vec<StoredSession>,
    snapshot: Arc<[TerminalInputIntents]>,
}

impl InputIntentStore {
    pub(crate) fn snapshot(&self) -> Arc<[TerminalInputIntents]> {
        Arc::clone(&self.snapshot)
    }

    #[cfg(test)]
    pub(crate) fn owner_for(&self, terminal_id: &TerminalId) -> Option<&str> {
        self.sessions
            .iter()
            .find(|session| {
                session.terminal_id == *terminal_id && session.lifecycle == SessionLifecycle::Active
            })
            .map(|session| session.session.as_str())
    }

    pub(crate) fn generation_for(&self, session: &str) -> Option<u64> {
        self.sessions
            .iter()
            .find(|record| record.session == session)
            .map(|record| record.generation)
    }

    pub(crate) fn open(
        &mut self,
        terminal_id: TerminalId,
        session: String,
    ) -> Result<StoreUpdate, InputIntentError> {
        if session.is_empty() || self.sessions.iter().any(|record| record.session == session) {
            return Err(InputIntentError::InvalidRequest);
        }
        self.sessions.push(StoredSession {
            terminal_id,
            session,
            generation: 1,
            policy: InputIntentPolicy::Unknown,
            state: InputIntentState::Unknown,
            lifecycle: SessionLifecycle::Open,
        });
        self.rebuild_snapshot();
        Ok(StoreUpdate {
            generation: 1,
            changed: true,
        })
    }

    pub(crate) fn apply(
        &mut self,
        session: &str,
        operation: &InputIntentOperation,
    ) -> Result<StoreUpdate, InputIntentError> {
        if matches!(operation, InputIntentOperation::Close {}) {
            return self.close(session);
        }
        let index = self
            .sessions
            .iter()
            .position(|record| record.session == session)
            .ok_or(InputIntentError::NoLease)?;
        let current = &self.sessions[index];
        if current.lifecycle == SessionLifecycle::Retired {
            return Err(InputIntentError::StaleLease);
        }
        let next_generation = current
            .generation
            .checked_add(1)
            .ok_or(InputIntentError::InvalidRequest)?;
        let terminal_id = current.terminal_id.clone();

        match operation {
            InputIntentOperation::Enter {} => {
                if !matches!(
                    current.policy,
                    InputIntentPolicy::Unknown | InputIntentPolicy::Entry
                ) {
                    return Err(InputIntentError::InvalidRequest);
                }
                self.supersede_active_owners(&terminal_id, session);
                let record = &mut self.sessions[index];
                record.policy = InputIntentPolicy::Entry;
                record.state = InputIntentState::Command;
                record.lifecycle = SessionLifecycle::Active;
                record.generation = next_generation;
            }
            InputIntentOperation::Activate { state, policy } => {
                if *policy != InputIntentPolicy::Mode
                    || !valid_state(*state)
                    || !matches!(
                        current.policy,
                        InputIntentPolicy::Unknown | InputIntentPolicy::Mode
                    )
                {
                    return Err(InputIntentError::InvalidRequest);
                }
                self.supersede_active_owners(&terminal_id, session);
                let record = &mut self.sessions[index];
                record.policy = InputIntentPolicy::Mode;
                record.state = *state;
                record.lifecycle = SessionLifecycle::Active;
                record.generation = next_generation;
            }
            InputIntentOperation::State { state } => {
                if current.policy != InputIntentPolicy::Mode || !valid_state(*state) {
                    return Err(InputIntentError::InvalidRequest);
                }
                let record = &mut self.sessions[index];
                record.state = *state;
                record.generation = next_generation;
            }
            InputIntentOperation::Blur {} => {
                let record = &mut self.sessions[index];
                record.lifecycle = SessionLifecycle::Blurred;
                record.generation = next_generation;
            }
            InputIntentOperation::Suspend {} => {
                let record = &mut self.sessions[index];
                record.lifecycle = SessionLifecycle::Suspended;
                record.generation = next_generation;
            }
            InputIntentOperation::Resume { state } => {
                if !valid_state(*state)
                    || !(current.policy == InputIntentPolicy::Mode
                        || (current.policy == InputIntentPolicy::Entry
                            && *state == InputIntentState::Command))
                {
                    return Err(InputIntentError::InvalidRequest);
                }
                self.supersede_active_owners(&terminal_id, session);
                let record = &mut self.sessions[index];
                record.state = *state;
                record.lifecycle = SessionLifecycle::Active;
                record.generation = next_generation;
            }
            InputIntentOperation::Close {} => unreachable!("close returned above"),
        }

        self.rebuild_snapshot();
        Ok(StoreUpdate {
            generation: next_generation,
            changed: true,
        })
    }

    pub(crate) fn close(&mut self, session: &str) -> Result<StoreUpdate, InputIntentError> {
        let Some(index) = self
            .sessions
            .iter()
            .position(|record| record.session == session)
        else {
            return Ok(StoreUpdate {
                generation: 0,
                changed: false,
            });
        };
        let record = self.sessions.swap_remove(index);
        let generation = record.generation.saturating_add(1);
        self.rebuild_snapshot();
        Ok(StoreUpdate {
            generation,
            changed: true,
        })
    }

    pub(crate) fn retire_terminal(&mut self, terminal_id: &TerminalId) -> bool {
        let mut changed = false;
        for record in &mut self.sessions {
            if record.terminal_id == *terminal_id && record.lifecycle.included_in_roster() {
                record.lifecycle = SessionLifecycle::Retired;
                record.generation = record.generation.saturating_add(1);
                changed = true;
            }
        }
        if changed {
            self.rebuild_snapshot();
        }
        changed
    }

    fn supersede_active_owners(&mut self, terminal_id: &TerminalId, except: &str) {
        for record in &mut self.sessions {
            if record.terminal_id == *terminal_id
                && record.session != except
                && record.lifecycle == SessionLifecycle::Active
            {
                record.lifecycle = SessionLifecycle::Retired;
                record.generation = record.generation.saturating_add(1);
            }
        }
    }

    fn rebuild_snapshot(&mut self) {
        let mut terminals: Vec<TerminalInputIntents> = Vec::new();
        for record in &self.sessions {
            if !record.lifecycle.included_in_roster() {
                continue;
            }
            let terminal_id = record.terminal_id.to_string();
            let group_index = terminals
                .iter()
                .position(|terminal| terminal.terminal_id == terminal_id);
            let terminal = if let Some(index) = group_index {
                &mut terminals[index]
            } else {
                terminals.push(TerminalInputIntents {
                    terminal_id,
                    sessions: Vec::new(),
                });
                let Some(terminal) = terminals.last_mut() else {
                    continue;
                };
                terminal
            };
            terminal.sessions.push(InputIntentSession {
                session: record.session.clone(),
                generation: record.generation,
                policy: record.policy,
                state: record.state,
                active: record.lifecycle == SessionLifecycle::Active,
            });
        }
        self.snapshot = terminals.into();
    }
}

fn valid_state(state: InputIntentState) -> bool {
    matches!(state, InputIntentState::Command | InputIntentState::Text)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::api::schema::{
        InputIntentOperation as Operation, InputIntentPolicy as Policy, InputIntentState as State,
    };
    use crate::terminal::TerminalId;

    use super::InputIntentStore;

    fn activate_mode(store: &mut InputIntentStore, session: &str, state: State) {
        store
            .apply(
                session,
                &Operation::Activate {
                    state,
                    policy: Policy::Mode,
                },
            )
            .unwrap();
    }

    fn sessions_for(store: &InputIntentStore, terminal_id: &TerminalId) -> Vec<(String, bool)> {
        store
            .snapshot()
            .iter()
            .find(|terminal| terminal.terminal_id == terminal_id.to_string())
            .map(|terminal| {
                terminal
                    .sessions
                    .iter()
                    .map(|session| (session.session.clone(), session.active))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn suspended_parent_survives_child_close_until_explicit_resume() {
        let terminal_id = TerminalId::alloc();
        let mut store = InputIntentStore::default();
        store.open(terminal_id.clone(), "parent".into()).unwrap();
        activate_mode(&mut store, "parent", State::Command);
        store.apply("parent", &Operation::Suspend {}).unwrap();

        store.open(terminal_id.clone(), "child".into()).unwrap();
        activate_mode(&mut store, "child", State::Command);
        store.apply("child", &Operation::Close {}).unwrap();

        assert_eq!(
            sessions_for(&store, &terminal_id),
            vec![("parent".into(), false)]
        );
        store
            .apply(
                "parent",
                &Operation::Resume {
                    state: State::Command,
                },
            )
            .unwrap();
        assert_eq!(
            sessions_for(&store, &terminal_id),
            vec![("parent".into(), true)]
        );
    }

    #[test]
    fn mode_text_resume_preserves_inactive_updates_without_command_promotion() {
        let terminal_id = TerminalId::alloc();
        let mut store = InputIntentStore::default();
        store.open(terminal_id.clone(), "mode".into()).unwrap();
        activate_mode(&mut store, "mode", State::Command);
        store.apply("mode", &Operation::Suspend {}).unwrap();
        store
            .apply("mode", &Operation::State { state: State::Text })
            .unwrap();
        let before = store.snapshot();
        assert!(!before[0].sessions[0].active);
        assert_eq!(before[0].sessions[0].state, State::Text);

        store
            .apply("mode", &Operation::Resume { state: State::Text })
            .unwrap();
        let after = store.snapshot();
        assert!(after[0].sessions[0].active);
        assert_eq!(after[0].sessions[0].state, State::Text);
        assert!(after[0].sessions[0].generation > before[0].sessions[0].generation);
    }

    #[test]
    fn eof_from_superseded_owner_does_not_clear_new_owner() {
        let terminal_id = TerminalId::alloc();
        let mut store = InputIntentStore::default();
        store.open(terminal_id.clone(), "old".into()).unwrap();
        activate_mode(&mut store, "old", State::Command);
        store.open(terminal_id.clone(), "new".into()).unwrap();
        activate_mode(&mut store, "new", State::Text);

        store.apply("old", &Operation::Close {}).unwrap();

        assert_eq!(
            sessions_for(&store, &terminal_id),
            vec![("new".into(), true)]
        );
        assert_eq!(store.owner_for(&terminal_id), Some("new"));
    }

    #[test]
    fn invalid_operations_leave_roster_and_generation_unchanged() {
        let terminal_id = TerminalId::alloc();
        let mut store = InputIntentStore::default();
        store.open(terminal_id, "entry".into()).unwrap();
        store.apply("entry", &Operation::Enter {}).unwrap();
        let before = store.snapshot();
        let generation = before[0].sessions[0].generation;

        assert!(store
            .apply("entry", &Operation::State { state: State::Text })
            .is_err());
        assert!(store
            .apply("entry", &Operation::Resume { state: State::Text })
            .is_err());
        assert!(store
            .apply(
                "entry",
                &Operation::Activate {
                    state: State::Command,
                    policy: Policy::Unknown,
                },
            )
            .is_err());

        let after = store.snapshot();
        assert!(Arc::ptr_eq(&before, &after));
        assert_eq!(after[0].sessions[0].generation, generation);
    }

    #[test]
    fn retiring_terminal_removes_live_sessions_and_rejects_old_streams() {
        let terminal_id = TerminalId::alloc();
        let mut store = InputIntentStore::default();
        store.open(terminal_id.clone(), "reporter".into()).unwrap();
        activate_mode(&mut store, "reporter", State::Command);

        assert!(store.retire_terminal(&terminal_id));
        assert!(store.snapshot().is_empty());
        assert!(store
            .apply(
                "reporter",
                &Operation::Resume {
                    state: State::Command
                }
            )
            .is_err());
        assert!(store.apply("reporter", &Operation::Close {}).is_ok());
        assert!(!store.retire_terminal(&terminal_id));
    }
}
