use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::Serialize;

use crate::api::schema::{
    InputIntentOperation, Method, PaneInputIntentStreamOperationParams,
    PaneInputIntentStreamParams, Request, ResponseResult, TerminalInputIntents,
};
use crate::terminal::{InputIntentError, TerminalId};

use super::App;

#[derive(Serialize)]
struct InputIntentOperationAck<'a> {
    ok: bool,
    generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenTargetError {
    InvalidRequest,
    TargetNotFound,
}

impl App {
    pub(crate) fn terminal_input_intents(&self) -> Arc<[TerminalInputIntents]> {
        self.input_intents.snapshot()
    }

    pub(crate) fn retire_input_intents_for_terminal(&mut self, terminal_id: &TerminalId) {
        if self.input_intents.retire_terminal(terminal_id) {
            self.input_intents_changed();
        }
    }

    pub(crate) fn input_intent_cancellation_response(
        &self,
        request: &Request,
        stream_active: Option<&AtomicBool>,
    ) -> Option<String> {
        let cancelable = match &request.method {
            Method::PaneInputIntentStreamOpen(_) => true,
            Method::PaneInputIntentStreamOperation(params) => {
                !matches!(&params.operation, InputIntentOperation::Close {})
            }
            _ => false,
        };
        if !cancelable || stream_active.is_none_or(|active| active.load(Ordering::Acquire)) {
            return None;
        }

        match &request.method {
            Method::PaneInputIntentStreamOpen(_) => Some(crate::app::api::responses::encode_error(
                request.id.clone(),
                "request_cancelled",
                "input intent stream request was cancelled before it started",
            )),
            Method::PaneInputIntentStreamOperation(params) => Some(
                self.operation_ack(
                    false,
                    self.input_intents
                        .generation_for(&params.session)
                        .unwrap_or(0),
                    &params.session,
                    None,
                    Some("TIMEOUT"),
                ),
            ),
            _ => None,
        }
    }

    pub(crate) fn handle_input_intent_stream_open(
        &mut self,
        id: String,
        params: PaneInputIntentStreamParams,
        stream_active: Option<&AtomicBool>,
    ) -> String {
        if stream_active.is_some_and(|active| !active.load(Ordering::Acquire)) {
            return crate::app::api::responses::encode_error(
                id,
                "request_cancelled",
                "input intent stream request was cancelled before it started",
            );
        }
        if !self.ime_control_enabled {
            return crate::app::api::responses::encode_error(
                id,
                "ime_control_disabled",
                "pane input intent streams require experimental.ime_control=true",
            );
        }
        if !cfg!(unix) {
            return crate::app::api::responses::encode_error(
                id,
                "ime_control_unsupported_platform",
                "pane input intent streams are only available on Unix servers",
            );
        }
        let terminal_id = match self.resolve_input_intent_target(&params) {
            Ok(terminal_id) => terminal_id,
            Err(OpenTargetError::InvalidRequest) => {
                return crate::app::api::responses::encode_error(
                    id,
                    "invalid_request",
                    "exactly one non-empty pane_id or popup_terminal_id is required",
                );
            }
            Err(OpenTargetError::TargetNotFound) => {
                return crate::app::api::responses::encode_error(
                    id,
                    "target_not_found",
                    "the input intent target is not a live pane or popup",
                );
            }
        };
        if stream_active.is_some_and(|active| !active.load(Ordering::Acquire)) {
            return crate::app::api::responses::encode_error(
                id,
                "request_cancelled",
                "input intent stream request was cancelled before it started",
            );
        }
        let update = match self.input_intents.open(terminal_id, params.owner.clone()) {
            Ok(update) => update,
            Err(InputIntentError::InvalidRequest) => {
                return crate::app::api::responses::encode_error(
                    id,
                    "invalid_request",
                    "input intent stream session is invalid",
                );
            }
            Err(_) => {
                return crate::app::api::responses::encode_error(
                    id,
                    "invalid_request",
                    "input intent stream session is invalid",
                );
            }
        };
        if update.changed {
            self.input_intents_changed();
        }
        crate::app::api::responses::encode_success(
            id,
            ResponseResult::PaneInputIntentStreamOpened {
                session: params.owner,
                generation: update.generation,
            },
        )
    }

    pub(crate) fn handle_input_intent_stream_operation(
        &mut self,
        _id: String,
        params: PaneInputIntentStreamOperationParams,
        stream_active: Option<&AtomicBool>,
    ) -> String {
        if !matches!(params.operation, InputIntentOperation::Close {})
            && stream_active.is_some_and(|active| !active.load(Ordering::Acquire))
        {
            return self.operation_ack(
                false,
                self.input_intents
                    .generation_for(&params.session)
                    .unwrap_or(0),
                &params.session,
                None,
                Some("TIMEOUT"),
            );
        }
        self.apply_input_intent_operation(&params.session, &params.operation)
    }

    pub(crate) fn handle_input_intent_stream_close(
        &mut self,
        _id: String,
        params: PaneInputIntentStreamOperationParams,
    ) -> String {
        if !matches!(params.operation, InputIntentOperation::Close {}) {
            return self.operation_ack(
                false,
                self.input_intents
                    .generation_for(&params.session)
                    .unwrap_or(0),
                &params.session,
                None,
                Some("INVALID_REQUEST"),
            );
        }
        self.apply_input_intent_operation(&params.session, &InputIntentOperation::Close {})
    }

    fn apply_input_intent_operation(
        &mut self,
        session: &str,
        operation: &InputIntentOperation,
    ) -> String {
        match self.input_intents.apply(session, operation) {
            Ok(update) => {
                if update.changed {
                    self.input_intents_changed();
                }
                self.operation_ack(true, update.generation, session, Some("recorded"), None)
            }
            Err(error) => self.operation_ack(
                false,
                self.input_intents.generation_for(session).unwrap_or(0),
                session,
                None,
                Some(input_intent_error_code(error)),
            ),
        }
    }

    fn operation_ack(
        &self,
        ok: bool,
        generation: u64,
        session: &str,
        scope: Option<&'static str>,
        error: Option<&'static str>,
    ) -> String {
        serde_json::to_string(&InputIntentOperationAck {
            ok,
            generation,
            session: ok.then_some(session),
            scope,
            error,
        })
        .unwrap_or_else(|_| {
            r#"{"ok":false,"generation":0,"session":"","error":"INVALID_REQUEST"}"#.to_owned()
        })
    }

    fn input_intents_changed(&mut self) {
        self.render_dirty.request_generic();
        self.render_notify.notify_one();
    }

    fn resolve_input_intent_target(
        &self,
        params: &PaneInputIntentStreamParams,
    ) -> Result<TerminalId, OpenTargetError> {
        match (&params.pane_id, &params.popup_terminal_id) {
            (Some(pane_id), None) if !pane_id.is_empty() => {
                let current_alias = self
                    .parse_pane_id(pane_id)
                    .and_then(|(workspace_index, pane_id)| {
                        self.public_pane_id(workspace_index, pane_id)
                    })
                    .as_deref()
                    == Some(pane_id.as_str());
                if !current_alias && !self.state.public_pane_id_aliases.contains_key(pane_id) {
                    return Err(OpenTargetError::TargetNotFound);
                }
                let Some((_, pane)) = self
                    .parse_pane_id(pane_id)
                    .and_then(|(_, pane_id)| self.find_pane(pane_id))
                else {
                    return Err(OpenTargetError::TargetNotFound);
                };
                let terminal_id = pane.attached_terminal_id.clone();
                if self.state.terminals.contains_key(&terminal_id)
                    && self.terminal_runtimes.get(&terminal_id).is_some()
                {
                    Ok(terminal_id)
                } else {
                    Err(OpenTargetError::TargetNotFound)
                }
            }
            (None, Some(terminal_id)) if !terminal_id.is_empty() => {
                let Some(popup) = self.state.popup_pane.as_ref() else {
                    return Err(OpenTargetError::TargetNotFound);
                };
                if popup.terminal_id.as_str() != terminal_id
                    || !self.state.terminals.contains_key(&popup.terminal_id)
                    || self.terminal_runtimes.get(&popup.terminal_id).is_none()
                {
                    return Err(OpenTargetError::TargetNotFound);
                }
                Ok(popup.terminal_id.clone())
            }
            _ => Err(OpenTargetError::InvalidRequest),
        }
    }
}

fn input_intent_error_code(error: InputIntentError) -> &'static str {
    match error {
        InputIntentError::InvalidRequest => "INVALID_REQUEST",
        InputIntentError::NoLease => "NO_LEASE",
        InputIntentError::StaleLease => "STALE_LEASE",
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use crate::api::schema::{
        InputIntentOperation, InputIntentPolicy, InputIntentState, Method,
        PaneInputIntentStreamOperationParams, PaneInputIntentStreamParams, PaneMoveDestination,
        PaneMoveParams, Request, SuccessResponse,
    };
    use crate::app::{App, AppPolicy};
    use crate::terminal::{TerminalId, TerminalRuntime, TerminalState};
    use crate::workspace::Workspace;
    use ratatui::layout::Direction;

    fn app_with_pane_target() -> (App, String, TerminalId) {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.ime_control_enabled = true;
        let workspace = Workspace::test_new("input-intent");
        let pane_id = workspace.tabs[0].root_pane;
        let terminal_id = workspace.terminal_id(pane_id).unwrap().clone();
        app.state.workspaces.push(workspace);
        app.state.terminals.insert(
            terminal_id.clone(),
            TerminalState::new(terminal_id.clone(), PathBuf::from("/pane")),
        );
        app.install_terminal_runtime(
            terminal_id.clone(),
            TerminalRuntime::test_with_screen_bytes(80, 24, b""),
        );
        let alias = app.public_pane_id(0, pane_id).unwrap();
        (app, alias, terminal_id)
    }

    fn open_pane_request(alias: String, session: &str) -> Request {
        Request {
            id: "open-input-intent".into(),
            method: Method::PaneInputIntentStreamOpen(PaneInputIntentStreamParams {
                pane_id: Some(alias),
                popup_terminal_id: None,
                owner: session.into(),
            }),
        }
    }

    fn operation_request(session: &str, operation: InputIntentOperation) -> Request {
        Request {
            id: "input-intent-operation".into(),
            method: Method::PaneInputIntentStreamOperation(PaneInputIntentStreamOperationParams {
                session: session.into(),
                operation,
            }),
        }
    }

    fn open_success(response: &str) -> SuccessResponse {
        let success: SuccessResponse = serde_json::from_str(response).unwrap();
        assert!(matches!(
            success.result,
            crate::api::schema::ResponseResult::PaneInputIntentStreamOpened { .. }
        ));
        success
    }

    #[tokio::test]
    async fn stream_open_binds_pane_alias_to_real_terminal_and_caches_roster() {
        let (mut app, pane_alias, terminal_id) = app_with_pane_target();
        let success =
            open_success(&app.handle_api_request(open_pane_request(pane_alias, "pane-reporter")));
        let crate::api::schema::ResponseResult::PaneInputIntentStreamOpened {
            session,
            generation,
        } = success.result
        else {
            panic!("expected input intent stream open result");
        };
        assert_eq!(session, "pane-reporter");
        assert_eq!(generation, 1);

        let roster = app.terminal_input_intents();
        assert_eq!(roster.len(), 1);
        assert_eq!(roster[0].terminal_id, terminal_id.to_string());
        assert_eq!(roster[0].sessions[0].session, "pane-reporter");
        assert!(!roster[0].sessions[0].active);
    }

    #[tokio::test]
    async fn disabled_ime_control_rejects_open_without_creating_session() {
        let (mut app, pane_alias, _) = app_with_pane_target();
        app.ime_control_enabled = false;

        let response = app.handle_api_request(open_pane_request(pane_alias, "disabled"));
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();

        assert_eq!(response["error"]["code"], "ime_control_disabled");
        assert!(app.terminal_input_intents().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn popup_open_requires_the_exact_live_popup_terminal_id() {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.ime_control_enabled = true;
        let (_, popup_id) =
            app.install_test_popup_runtime(TerminalRuntime::test_with_screen_bytes(80, 24, b""));
        let wrong_id = TerminalId::alloc().to_string();
        let wrong = app.handle_api_request(Request {
            id: "wrong-popup".into(),
            method: Method::PaneInputIntentStreamOpen(PaneInputIntentStreamParams {
                pane_id: None,
                popup_terminal_id: Some(wrong_id),
                owner: "wrong-popup".into(),
            }),
        });
        let wrong: serde_json::Value = serde_json::from_str(&wrong).unwrap();
        assert_eq!(wrong["error"]["code"], "target_not_found");
        assert!(app.terminal_input_intents().is_empty());

        let opened = app.handle_api_request(Request {
            id: "popup".into(),
            method: Method::PaneInputIntentStreamOpen(PaneInputIntentStreamParams {
                pane_id: None,
                popup_terminal_id: Some(popup_id.to_string()),
                owner: "popup-reporter".into(),
            }),
        });
        let opened = open_success(&opened);
        assert_eq!(
            app.terminal_input_intents()[0].terminal_id,
            popup_id.to_string()
        );
        assert!(matches!(
            opened.result,
            crate::api::schema::ResponseResult::PaneInputIntentStreamOpened { .. }
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pane_move_keeps_stream_bound_to_its_terminal_identity() {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.ime_control_enabled = true;
        let mut workspace = Workspace::test_new("moving");
        let retained_pane = workspace.test_split(Direction::Horizontal);
        let terminal_id = workspace.terminal_id(retained_pane).unwrap().clone();
        let original_root_id = workspace
            .terminal_id(workspace.tabs[0].root_pane)
            .unwrap()
            .clone();
        app.state.workspaces.push(workspace);
        for id in [terminal_id.clone(), original_root_id.clone()] {
            app.state.terminals.insert(
                id.clone(),
                TerminalState::new(id.clone(), PathBuf::from("/pane")),
            );
        }
        app.install_terminal_runtime(
            terminal_id.clone(),
            TerminalRuntime::test_with_screen_bytes(80, 24, b""),
        );
        let pane_alias = app.public_pane_id(0, retained_pane).unwrap();
        open_success(
            &app.handle_api_request(open_pane_request(pane_alias.clone(), "moving-reporter")),
        );

        let response = app.handle_api_request(Request {
            id: "move-pane".into(),
            method: Method::PaneMove(PaneMoveParams {
                pane_id: pane_alias,
                destination: PaneMoveDestination::NewWorkspace {
                    label: Some("destination".into()),
                    tab_label: None,
                },
                focus: true,
            }),
        });
        let moved: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert!(matches!(
            moved.result,
            crate::api::schema::ResponseResult::PaneMove { .. }
        ));
        assert_eq!(
            app.terminal_input_intents()[0].terminal_id,
            terminal_id.to_string()
        );
        assert_eq!(
            app.state.workspaces[1].terminal_id(retained_pane),
            Some(&terminal_id)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn canceled_open_and_operation_do_not_change_the_roster() {
        let (mut app, pane_alias, _) = app_with_pane_target();
        let canceled_open = Arc::new(AtomicBool::new(false));
        let before_open = app.terminal_input_intents();
        let response = app.handle_api_request_after_internal_events_drained_with_active(
            open_pane_request(pane_alias.clone(), "canceled-open"),
            Some(canceled_open.as_ref()),
        );
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["error"]["code"], "request_cancelled");
        assert!(Arc::ptr_eq(&before_open, &app.terminal_input_intents()));

        open_success(&app.handle_api_request(open_pane_request(pane_alias, "live-reporter")));
        let before_operation = app.terminal_input_intents();
        let canceled_operation = Arc::new(AtomicBool::new(false));
        let response = app.handle_api_request_after_internal_events_drained_with_active(
            operation_request(
                "live-reporter",
                InputIntentOperation::Activate {
                    state: InputIntentState::Command,
                    policy: InputIntentPolicy::Mode,
                },
            ),
            Some(canceled_operation.as_ref()),
        );
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["ok"], false);
        assert_eq!(response["error"], "TIMEOUT");
        assert!(Arc::ptr_eq(
            &before_operation,
            &app.terminal_input_intents()
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replacing_runtime_for_same_terminal_retires_its_reporters() {
        let (mut app, pane_alias, terminal_id) = app_with_pane_target();
        open_success(&app.handle_api_request(open_pane_request(pane_alias, "replace-reporter")));
        let response = app.handle_api_request(operation_request(
            "replace-reporter",
            InputIntentOperation::Enter {},
        ));
        let ack: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(ack["scope"], "recorded");
        assert_eq!(
            app.input_intents.owner_for(&terminal_id),
            Some("replace-reporter")
        );

        app.install_terminal_runtime(
            terminal_id,
            TerminalRuntime::test_with_screen_bytes(80, 24, b"replacement"),
        );

        assert!(app.terminal_input_intents().is_empty());
        let response = app.handle_api_request(operation_request(
            "replace-reporter",
            InputIntentOperation::Resume {
                state: InputIntentState::Command,
            },
        ));
        let ack: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(ack["error"], "STALE_LEASE");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn input_intent_open_and_operation_have_recorded_raw_ack_contract() {
        let (mut app, pane_alias, _) = app_with_pane_target();
        open_success(&app.handle_api_request(open_pane_request(pane_alias, "ack-reporter")));
        let response = app.handle_api_request(operation_request(
            "ack-reporter",
            InputIntentOperation::Activate {
                state: InputIntentState::Text,
                policy: InputIntentPolicy::Mode,
            },
        ));
        assert_eq!(response.matches('\n').count(), 0);
        let ack: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(ack["ok"], true);
        assert_eq!(ack["generation"], 2);
        assert_eq!(ack["session"], "ack-reporter");
        assert_eq!(ack["scope"], "recorded");
        assert!(ack.get("id").is_none());
    }
}
