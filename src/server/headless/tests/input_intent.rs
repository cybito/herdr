use super::*;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::api::schema::{
    InputIntentOperation, InputIntentPolicy, InputIntentState, Method,
    PaneInputIntentStreamOperationParams, PaneInputIntentStreamParams, Request, ResponseResult,
    SuccessResponse,
};
use crate::terminal::{TerminalRuntime, TerminalState};
use crate::protocol::ClientMessage;
use crate::workspace::Workspace;

#[tokio::test]
async fn canceled_stream_requests_do_not_reach_the_intent_store() {
    let mut server = test_headless_server();
    server.app.ime_control_enabled = true;

    let workspace = Workspace::test_new("intent-cancellation");
    let pane_id = workspace.tabs[0].root_pane;
    let terminal_id = workspace.terminal_id(pane_id).unwrap().clone();
    server.app.state.workspaces = vec![workspace];
    server.app.state.active = Some(0);
    server.app.state.terminals.insert(
        terminal_id.clone(),
        TerminalState::new(terminal_id.clone(), PathBuf::from("/pane")),
    );
    server.app.install_terminal_runtime(
        terminal_id,
        TerminalRuntime::test_with_screen_bytes(80, 24, b""),
    );
    let pane_id = server.app.public_pane_id(0, pane_id).unwrap();

    let active = Arc::new(AtomicBool::new(true));
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request: Request {
            id: "open-live".into(),
            method: Method::PaneInputIntentStreamOpen(PaneInputIntentStreamParams {
                pane_id: Some(pane_id.clone()),
                popup_terminal_id: None,
                owner: "reporter".into(),
            }),
        },
        respond_to,
        response_write_complete: None,
        stream_active: Some(Arc::clone(&active)),
    });
    let opened: SuccessResponse = serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
    let ResponseResult::PaneInputIntentStreamOpened { session, generation } = opened.result else {
        panic!("expected input intent stream open result");
    };
    assert_eq!(session, "reporter");
    assert_eq!(generation, 1);

    let before = server.app.terminal_input_intents();
    active.store(false, Ordering::Release);
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request: Request {
            id: "canceled-operation".into(),
            method: Method::PaneInputIntentStreamOperation(PaneInputIntentStreamOperationParams {
                session: "reporter".into(),
                operation: InputIntentOperation::Activate {
                    state: InputIntentState::Command,
                    policy: InputIntentPolicy::Mode,
                },
            }),
        },
        respond_to,
        response_write_complete: None,
        stream_active: Some(Arc::clone(&active)),
    });
    let operation: serde_json::Value = serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
    assert_eq!(operation["ok"], false);
    assert_eq!(operation["generation"], 1);
    assert_eq!(operation["error"], "TIMEOUT");
    assert!(Arc::ptr_eq(&before, &server.app.terminal_input_intents()));

    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request: Request {
            id: "canceled-open".into(),
            method: Method::PaneInputIntentStreamOpen(PaneInputIntentStreamParams {
                pane_id: Some(pane_id),
                popup_terminal_id: None,
                owner: "late-reporter".into(),
            }),
        },
        respond_to,
        response_write_complete: None,
        stream_active: Some(active),
    });
    let canceled_open: crate::api::schema::ErrorResponse =
        serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
    assert_eq!(canceled_open.error.code, "request_cancelled");
    assert!(Arc::ptr_eq(&before, &server.app.terminal_input_intents()));

    shutdown_test_runtimes(&mut server);
}

#[cfg(unix)]
#[tokio::test]
async fn pane_input_intent_negotiation_uses_the_server_setting_and_an_explicit_empty_roster() {
    use crate::protocol::endpoint::{
        EndpointServerWelcome, ENDPOINT_HELLO_KIND, ENDPOINT_WELCOME_KIND,
        PANE_INPUT_INTENT_CAPABILITY,
    };

    for enabled in [false, true] {
        let mut server = test_headless_server();
        server.app.ime_control_enabled = enabled;
        let mut client =
            std::os::unix::net::UnixStream::connect(&server.client_socket_path).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        protocol::write_message(
            &mut client,
            &ClientMessage::EndpointControl {
                kind: ENDPOINT_HELLO_KIND.into(),
                data: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/endpoint-hello-v1.json"
                ))
                .into(),
            },
        )
        .unwrap();
        server.accept_client_connections().unwrap();
        let welcome: ServerMessage = protocol::read_message(&mut client, MAX_FRAME_SIZE).unwrap();
        let ServerMessage::EndpointControl { kind, data } = welcome else {
            panic!("expected stable endpoint welcome");
        };
        assert_eq!(kind, ENDPOINT_WELCOME_KIND);
        let welcome: EndpointServerWelcome = serde_json::from_str(&data).unwrap();
        assert!(welcome.error.is_none());
        assert_eq!(
            welcome
                .capabilities
                .iter()
                .any(|capability| capability == PANE_INPUT_INTENT_CAPABILITY),
            enabled
        );
        let connected = server.server_event_rx.recv().await.unwrap();
        assert!(matches!(&connected, ServerEvent::ClientShellConnected { .. }));
        assert!(server.handle_server_event(connected));
        let snapshot = client_shell_snapshot(
            protocol::read_message(&mut client, MAX_FRAME_SIZE).unwrap(),
        );
        if enabled {
            assert_eq!(snapshot.input_intents.as_deref(), Some(&[][..]));
        } else {
            assert!(snapshot.input_intents.is_none());
        }
        drop(client);
        server.should_quit.store(true, Ordering::Release);
    }
}

#[cfg(unix)]
fn intent_request(server: &mut HeadlessServer, method: Method) -> serde_json::Value {
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    server.handle_api_request_with_shutdown_check(api::ApiRequestMessage {
        request: Request {
            id: "roster-regression".into(),
            method,
        },
        respond_to,
        response_write_complete: None,
        stream_active: None,
    });
    serde_json::from_str(&response_rx.recv().unwrap()).unwrap()
}

#[cfg(unix)]
fn install_intent_workspace(
    server: &mut HeadlessServer,
    name: &str,
) -> (String, crate::terminal::TerminalId) {
    let workspace = Workspace::test_new(name);
    let pane_id = workspace.tabs[0].root_pane;
    let terminal_id = workspace.terminal_id(pane_id).unwrap().clone();
    let workspace_index = server.app.state.workspaces.len();
    server.app.state.workspaces.push(workspace);
    server.app.state.terminals.insert(
        terminal_id.clone(),
        TerminalState::new(terminal_id.clone(), PathBuf::from("/pane")),
    );
    server.app.install_terminal_runtime(
        terminal_id.clone(),
        TerminalRuntime::test_with_screen_bytes(80, 24, b""),
    );
    (
        server.app.public_pane_id(workspace_index, pane_id).unwrap(),
        terminal_id,
    )
}

#[cfg(unix)]
fn open_reporter(server: &mut HeadlessServer, params: PaneInputIntentStreamParams) {
    let response = intent_request(server, Method::PaneInputIntentStreamOpen(params));
    assert_eq!(response["result"]["type"], "pane_input_intent_stream_opened");
}

#[cfg(unix)]
fn update_reporter(server: &mut HeadlessServer, session: &str, operation: InputIntentOperation) {
    let response = intent_request(
        server,
        Method::PaneInputIntentStreamOperation(PaneInputIntentStreamOperationParams {
            session: session.into(),
            operation,
        }),
    );
    assert_eq!(response["ok"], true);
}

#[cfg(unix)]
fn recv_intent_snapshot(
    receiver: &std::sync::mpsc::Receiver<Vec<u8>>,
) -> Box<crate::protocol::ClientShellSnapshot> {
    loop {
        let message = read_server_message(receiver.recv_timeout(Duration::from_secs(2)).unwrap());
        if matches!(
            &message,
            ServerMessage::EndpointControl { kind, .. }
                if kind == crate::protocol::endpoint::ENDPOINT_SNAPSHOT_KIND
        ) {
            return client_shell_snapshot(message);
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn replacement_roster_includes_hidden_workspace_and_popup_sessions_for_every_client() {
    use crate::api::schema::InputIntentSession;

    let mut server = test_headless_server();
    server.app.ime_control_enabled = true;
    let (visible_pane, visible_terminal) = install_intent_workspace(&mut server, "visible");
    let (hidden_pane, hidden_terminal) = install_intent_workspace(&mut server, "hidden");
    server.app.state.active = Some(0);
    server.app.state.selected = 0;
    server.app.state.mode = crate::app::Mode::Terminal;
    let (_, popup_terminal) = server
        .app
        .install_test_popup_runtime(TerminalRuntime::test_with_screen_bytes(20, 5, b""));
    for (session, pane_id, popup_terminal_id) in [
        ("visible", Some(visible_pane), None),
        ("hidden", Some(hidden_pane), None),
        ("popup", None, Some(popup_terminal.to_string())),
    ] {
        open_reporter(
            &mut server,
            PaneInputIntentStreamParams {
                pane_id,
                popup_terminal_id,
                owner: session.into(),
            },
        );
        update_reporter(
            &mut server,
            session,
            InputIntentOperation::Activate {
                state: if session == "hidden" {
                    InputIntentState::Text
                } else {
                    InputIntentState::Command
                },
                policy: InputIntentPolicy::Mode,
            },
        );
    }
    update_reporter(&mut server, "hidden", InputIntentOperation::Blur {});
    let (first_control, _first_render) = connect_test_shell(&mut server, 71, 80, 24);
    let first = recv_intent_snapshot(&first_control);
    let roster = first.input_intents.as_deref().unwrap();
    assert_eq!(roster.len(), 3);
    for (terminal, session, generation, state, active) in [
        (&visible_terminal, "visible", 2, InputIntentState::Command, true),
        (&hidden_terminal, "hidden", 3, InputIntentState::Text, false),
        (&popup_terminal, "popup", 2, InputIntentState::Command, true),
    ] {
        let terminal_roster = roster
            .iter()
            .find(|record| record.terminal_id == terminal.as_str())
            .unwrap();
        assert_eq!(
            terminal_roster.sessions,
            vec![InputIntentSession {
                session: session.into(),
                generation,
                policy: InputIntentPolicy::Mode,
                state,
                active,
            }]
        );
    }
    let (second_control, _second_render) = connect_test_shell(&mut server, 72, 80, 24);
    let _initial_second = recv_intent_snapshot(&second_control);
    let hidden_tab = server.app.public_tab_id(1, 0).unwrap();
    assert!(server.focus_shell_client_on_tab(72, &hidden_tab));
    server.render_and_stream();
    let second = recv_intent_snapshot(&second_control);
    assert_ne!(first.focused_workspace_id, second.focused_workspace_id);
    assert_eq!(first.input_intents, second.input_intents);
    shutdown_test_runtimes(&mut server);
}

#[cfg(unix)]
#[tokio::test]
async fn suspended_parent_eof_replaces_roster_without_changing_the_active_child() {
    use crate::api::schema::InputIntentSession;

    let mut server = test_headless_server();
    server.app.ime_control_enabled = true;
    let (pane_id, terminal_id) = install_intent_workspace(&mut server, "handoff");
    server.app.state.active = Some(0);
    server.app.state.selected = 0;
    server.app.state.mode = crate::app::Mode::Terminal;
    let initial_empty = server.app.terminal_input_intents();
    open_reporter(
        &mut server,
        PaneInputIntentStreamParams {
            pane_id: Some(pane_id.clone()),
            popup_terminal_id: None,
            owner: "parent".into(),
        },
    );
    update_reporter(&mut server, "parent", InputIntentOperation::Enter {});
    let (control, _render) = connect_test_shell(&mut server, 73, 80, 24);
    let active_parent = recv_intent_snapshot(&control);
    assert_eq!(
        active_parent.input_intents.as_ref().unwrap()[0].terminal_id,
        terminal_id.as_str()
    );
    assert_eq!(
        active_parent.input_intents.as_ref().unwrap()[0].sessions,
        vec![InputIntentSession {
            session: "parent".into(),
            generation: 2,
            policy: InputIntentPolicy::Entry,
            state: InputIntentState::Command,
            active: true,
        }]
    );
    let parent_cache = server.app.terminal_input_intents();
    assert!(!Arc::ptr_eq(&initial_empty, &parent_cache));
    assert!(Arc::ptr_eq(
        &parent_cache,
        server.clients[&73].shell_snapshot.as_ref().unwrap().input_intents.as_ref().unwrap()
    ));
    server.render_and_stream();
    assert!(Arc::ptr_eq(&parent_cache, &server.app.terminal_input_intents()));

    update_reporter(&mut server, "parent", InputIntentOperation::Suspend {});
    server.render_and_stream();
    let suspended_parent = recv_intent_snapshot(&control);
    assert!(suspended_parent.revision > active_parent.revision);
    assert!(!suspended_parent.input_intents.as_ref().unwrap()[0].sessions[0].active);

    open_reporter(
        &mut server,
        PaneInputIntentStreamParams {
            pane_id: Some(pane_id),
            popup_terminal_id: None,
            owner: "child".into(),
        },
    );
    update_reporter(
        &mut server,
        "child",
        InputIntentOperation::Activate {
            state: InputIntentState::Command,
            policy: InputIntentPolicy::Mode,
        },
    );
    server.render_and_stream();
    let child_active = recv_intent_snapshot(&control);
    assert!(child_active.revision > suspended_parent.revision);
    let expected_child = InputIntentSession {
        session: "child".into(),
        generation: 2,
        policy: InputIntentPolicy::Mode,
        state: InputIntentState::Command,
        active: true,
    };
    assert_eq!(
        child_active.input_intents.as_ref().unwrap()[0].sessions,
        vec![
            InputIntentSession {
                session: "parent".into(),
                generation: 3,
                policy: InputIntentPolicy::Entry,
                state: InputIntentState::Command,
                active: false,
            },
            expected_child.clone(),
        ]
    );
    let child_cache = server.app.terminal_input_intents();
    let response = intent_request(
        &mut server,
        Method::PaneInputIntentStreamClose(PaneInputIntentStreamOperationParams {
            session: "parent".into(),
            operation: InputIntentOperation::Close {},
        }),
    );
    assert_eq!(response["ok"], true);
    server.render_and_stream();
    let parent_eof = recv_intent_snapshot(&control);
    assert_eq!(parent_eof.boot_id, child_active.boot_id);
    assert!(parent_eof.revision > child_active.revision);
    assert_eq!(
        parent_eof.input_intents.as_ref().unwrap()[0].sessions,
        vec![expected_child]
    );
    assert!(!Arc::ptr_eq(&child_cache, &server.app.terminal_input_intents()));

    update_reporter(&mut server, "child", InputIntentOperation::Close {});
    server.render_and_stream();
    let empty = recv_intent_snapshot(&control);
    assert!(empty.revision > parent_eof.revision);
    assert_eq!(empty.input_intents.as_deref(), Some(&[][..]));
    let empty_cache = server.app.terminal_input_intents();
    update_reporter(&mut server, "child", InputIntentOperation::Close {});
    server.render_and_stream();
    assert!(Arc::ptr_eq(&empty_cache, &server.app.terminal_input_intents()));
    assert!(Arc::ptr_eq(
        &empty_cache,
        server.clients[&73].shell_snapshot.as_ref().unwrap().input_intents.as_ref().unwrap()
    ));
    shutdown_test_runtimes(&mut server);
}

#[cfg(unix)]
#[tokio::test]
async fn moved_pane_keeps_its_reporter_identity_but_respawn_requires_a_new_reporter() {
    use crate::api::schema::{PaneMoveDestination, PaneMoveParams};

    let mut server = test_headless_server();
    server.app.ime_control_enabled = true;
    install_intent_workspace(&mut server, "moving");
    let moved_pane = server.app.state.workspaces[0].test_split(ratatui::layout::Direction::Horizontal);
    let terminal_id = server.app.state.workspaces[0]
        .terminal_id(moved_pane)
        .unwrap()
        .clone();
    server.app.state.terminals.insert(
        terminal_id.clone(),
        TerminalState::new(
            terminal_id.clone(),
            server.client_socket_path.parent().unwrap().to_path_buf(),
        ),
    );
    server.app.install_terminal_runtime(
        terminal_id.clone(),
        TerminalRuntime::test_with_screen_bytes(80, 24, b""),
    );
    server.app.state.active = Some(0);
    server.app.state.selected = 0;
    server.app.state.mode = crate::app::Mode::Terminal;
    let pane_alias = server.app.public_pane_id(0, moved_pane).unwrap();
    open_reporter(
        &mut server,
        PaneInputIntentStreamParams {
            pane_id: Some(pane_alias.clone()),
            popup_terminal_id: None,
            owner: "before-respawn".into(),
        },
    );
    update_reporter(&mut server, "before-respawn", InputIntentOperation::Enter {});
    let (control, _render) = connect_test_shell(&mut server, 74, 80, 24);
    let before_move = recv_intent_snapshot(&control);
    let before_cache = server.app.terminal_input_intents();
    let response = intent_request(
        &mut server,
        Method::PaneMove(PaneMoveParams {
            pane_id: pane_alias,
            destination: PaneMoveDestination::NewWorkspace {
                label: Some("destination".into()),
                tab_label: None,
            },
            focus: true,
        }),
    );
    let response: SuccessResponse = serde_json::from_value(response).unwrap();
    let ResponseResult::PaneMove { move_result } = response.result else {
        panic!("expected successful pane move");
    };
    assert!(move_result.changed);
    assert_ne!(move_result.pane.workspace_id, move_result.previous_workspace_id);
    assert!(server.focus_shell_client_on_tab(74, &move_result.pane.tab_id));
    server.render_and_stream();
    let after_move = recv_intent_snapshot(&control);
    assert!(after_move.revision > before_move.revision);
    assert_eq!(after_move.input_intents, before_move.input_intents);
    assert!(Arc::ptr_eq(&before_cache, &server.app.terminal_input_intents()));
    let current_pane = after_move
        .panes
        .iter()
        .find(|pane| pane.pane_id == move_result.pane.pane_id)
        .unwrap();
    assert_eq!(current_pane.terminal_id.as_deref(), Some(terminal_id.as_str()));
    let roster = after_move.input_intents.as_ref().unwrap();
    let matching = roster
        .iter()
        .find(|record| Some(record.terminal_id.as_str()) == current_pane.terminal_id.as_deref())
        .unwrap();
    assert_eq!(matching.sessions[0].session, "before-respawn");
    assert!(matching.sessions[0].active);

    server.app.state.default_shell = "/bin/sh".into();
    server.app.state.shell_mode = crate::config::ShellModeConfig::NonLogin;
    server
        .app
        .state
        .terminals
        .get_mut(&terminal_id)
        .unwrap()
        .respawn_shell_on_exit = true;
    server.app.handle_internal_event(crate::events::AppEvent::PaneDied {
        pane_id: moved_pane,
        exit_reason: crate::platform::ChildExitReason::Exited,
    });
    server.render_and_stream();
    let after_respawn = recv_intent_snapshot(&control);
    assert!(after_respawn.revision > after_move.revision);
    let respawned_pane = after_respawn
        .panes
        .iter()
        .find(|pane| pane.pane_id == move_result.pane.pane_id)
        .expect("respawn keeps the pane attached");
    assert_eq!(respawned_pane.terminal_id, current_pane.terminal_id);
    assert_eq!(after_respawn.input_intents.as_deref(), Some(&[][..]));
    let stale = intent_request(
        &mut server,
        Method::PaneInputIntentStreamOperation(PaneInputIntentStreamOperationParams {
            session: "before-respawn".into(),
            operation: InputIntentOperation::Resume {
                state: InputIntentState::Command,
            },
        }),
    );
    assert_eq!(stale["ok"], false);
    assert_eq!(stale["error"], "STALE_LEASE");
    open_reporter(
        &mut server,
        PaneInputIntentStreamParams {
            pane_id: Some(move_result.pane.pane_id),
            popup_terminal_id: None,
            owner: "after-respawn".into(),
        },
    );
    update_reporter(&mut server, "after-respawn", InputIntentOperation::Enter {});
    server.render_and_stream();
    let renewed = recv_intent_snapshot(&control);
    let roster = renewed.input_intents.as_ref().unwrap();
    let matching = roster
        .iter()
        .find(|record| Some(record.terminal_id.as_str()) == respawned_pane.terminal_id.as_deref())
        .unwrap();
    assert_eq!(
        matching.sessions,
        vec![crate::api::schema::InputIntentSession {
            session: "after-respawn".into(),
            generation: 2,
            policy: InputIntentPolicy::Entry,
            state: InputIntentState::Command,
            active: true,
        }]
    );
    shutdown_test_runtimes(&mut server);
}
