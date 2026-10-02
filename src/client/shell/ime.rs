use super::*;
use crate::api::schema::{InputIntentPolicy, InputIntentState, TerminalInputIntents};
use crate::client::endpoint::EndpointRegistry;
use crate::client::ime_control::{
    AckScope, AuthorizationKey, Completion, DesiredLease, ImeWorker, LeaseIdentity, LeaseTarget,
    WorkerPlan,
};
use crate::client::{ClientError, ClientLoopEvent};
use crate::raw_input::RawInputEvent;
use crossterm::event::{KeyEventKind, MouseButton, MouseEventKind};
use std::sync::Arc;
use std::time::{Duration, Instant};

const INPUT_BATCH_LIMIT: usize = 256;
const INPUT_BYTE_LIMIT: usize = 2 * 1024 * 1024;
const INPUT_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq)]
struct InputBinding {
    route: Arc<ClientInputRoute>,
    terminal: Option<Arc<str>>,
    popup_pending: bool,
    focus_epoch: u64,
}

struct InputBatch {
    // Retain the original allocation for image-paste/file-drop recognition. Events move
    // out of their parser allocation; advancing never reparses or replays a trigger.
    raw: Vec<u8>,
    events: VecDeque<RawInputEvent>,
    cursor: usize,
    image_checked: bool,
    forward_prefix: bool,
    focus_target: Option<(String, Arc<str>)>,
    pending_outcome: Option<ClientShellInput>,
    replay: bool,
    pixels: Option<crate::input::mouse::HostPixels>,
    binding: InputBinding,
    deadline: Instant,
}

struct CachedEndpoint {
    route: Arc<ClientInputRoute>,
    roster: Arc<[TerminalInputIntents]>,
    panes: Arc<HashMap<String, Arc<str>>>,
    reporters: HashMap<String, Arc<LeaseIdentity>>,
    local_ui: Arc<LeaseIdentity>,
}

/// Serial semantic input authorization. All source I/O lives in ImeWorker; this
/// module owns causal keys, replacement-roster identity caching, and the FIFO.
pub(in crate::client) struct ImeGate {
    worker: Option<ImeWorker>,
    cache: Vec<CachedEndpoint>,
    live: Arc<[Arc<LeaseIdentity>]>,
    batches: VecDeque<InputBatch>,
    bytes: usize,
    focus_epoch: u64,
    arbitration_epoch: u64,
    focused: bool,
    episode: bool,
    binding: Option<InputBinding>,
    sample_pending: bool,
    authorization: Option<Arc<AuthorizationKey>>,
    desired: Option<DesiredLease>,
    ui_context: Option<(ClientShellMode, Option<ClientShellOverlayKind>, bool)>,
    unreported: Option<Arc<LeaseIdentity>>,
    fallback: Arc<LeaseIdentity>,
    copy_blocked: bool,
    repaint_pending: bool,
    pending_releases: ClientShellInput,
    release_ready: bool,
    waiting_target: bool,
    applied: bool,
    inactive: bool,
    submitted: bool,
    pending_plan: Option<WorkerPlan>,
}

impl Drop for ImeGate {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.as_mut() {
            worker.shutdown();
        }
    }
}

impl ImeGate {
    pub(in crate::client) fn start(
        sender: tokio::sync::mpsc::Sender<ClientLoopEvent>,
    ) -> Result<Self, ClientError> {
        let worker = ImeWorker::start(sender).map_err(ClientError::ImeControl)?;
        Ok(Self::with_worker(Some(worker)))
    }

    fn with_worker(worker: Option<ImeWorker>) -> Self {
        Self {
            worker,
            cache: Vec::new(),
            live: Arc::from([]),
            batches: VecDeque::new(),
            bytes: 0,
            focus_epoch: 1,
            arbitration_epoch: 1,
            focused: true,
            episode: true,
            binding: None,
            authorization: None,
            desired: None,
            sample_pending: true,
            ui_context: None,
            unreported: None,
            fallback: Arc::new(LeaseIdentity {
                endpoint_id: ClientEndpointId::Local,
                connection_generation: 0,
                boot_id: Arc::from(""),
                terminal_target: LeaseTarget::LocalUi,
                reporter_session: None,
            }),
            copy_blocked: false,
            repaint_pending: false,
            pending_releases: ClientShellInput::default(),
            release_ready: false,
            waiting_target: false,
            applied: false,
            inactive: false,
            submitted: false,
            pending_plan: None,
        }
    }

    pub(in crate::client) fn ready(&self) -> bool {
        !self.pending_releases.routed_requests.is_empty()
            || self.release_ready
            || (self.applied
                && !self.waiting_target
                && self
                    .batches
                    .front()
                    .is_some_and(|batch| batch.pending_outcome.is_some() || !self.copy_blocked))
    }

    pub(in crate::client) fn deadline(&self) -> Option<Instant> {
        self.batches.front().map(|batch| batch.deadline)
    }

    pub(in crate::client) fn take_releases(&mut self) -> Option<ClientShellInput> {
        (!self.pending_releases.routed_requests.is_empty())
            .then(|| std::mem::take(&mut self.pending_releases))
    }

    pub(in crate::client) fn take_repaint(&mut self) -> bool {
        std::mem::take(&mut self.repaint_pending)
    }

    pub(in crate::client) fn authorized(&self) -> bool {
        self.applied && self.focused && self.binding.is_some()
    }

    fn report_cancelled(&mut self, shell: &mut ClientShellState, count: usize) {
        if count != 0 {
            shell.endpoint_error = Some(format!(
                "IME_INPUT_CANCELLED: {count} waiting input batches cancelled"
            ));
            tracing::warn!(batches = count, "IME_INPUT_CANCELLED");
            self.repaint_pending = true;
        }
    }

    fn flush_balancing_release(
        shell: &mut ClientShellState,
        event: &RawInputEvent,
        releases: &mut ClientShellInput,
    ) {
        match event {
            RawInputEvent::Key(key) if key.kind == KeyEventKind::Release => {
                shell.release_tracked_key(key, releases);
            }
            RawInputEvent::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Up(button) if shell.pane_mouse_gesture.as_ref().is_some_and(|gesture| gesture.button == button)) =>
            {
                merge(
                    releases,
                    shell.handle_raw_events([RawInputEvent::Mouse(*mouse)]),
                );
            }
            _ => {}
        }
    }

    fn cancel(&mut self, shell: &mut ClientShellState) {
        let count = self.batches.len();
        for batch in self.batches.drain(..) {
            if let Some(outcome) = batch.pending_outcome {
                for action in outcome.actions {
                    if let ClientShellAction::Endpoint { request, .. } = action {
                        self.repaint_pending |= shell.cancel_endpoint_request(&request.id);
                    }
                }
            }
            for event in batch.events {
                Self::flush_balancing_release(shell, &event, &mut self.pending_releases);
            }
        }
        self.bytes = 0;
        shell.ime_forward_prefix = false;
        self.report_cancelled(shell, count);
    }
    fn cancel_buffered_key(
        &mut self,
        shell: &mut ClientShellState,
        key: &crate::input::TerminalKey,
    ) {
        let identity = crate::input::InputLeaseKey::new(0_u8, key);
        let mut cancelled = 0;
        for batch in &mut self.batches {
            let before = batch.events.len();
            batch.events.retain(|event| !matches!(event, RawInputEvent::Key(pending) if crate::input::InputLeaseKey::new(0_u8, pending) == identity));
            cancelled += usize::from(before != batch.events.len());
        }
        let mut removed_bytes = 0;
        self.batches.retain(|batch| {
            if batch.events.is_empty() && batch.pending_outcome.is_none() {
                removed_bytes += batch.raw.len();
                false
            } else {
                true
            }
        });
        self.bytes -= removed_bytes;
        self.report_cancelled(shell, cancelled);
    }

    fn update_cache(
        &mut self,
        shell: &ClientShellState,
        endpoints: &EndpointRegistry,
    ) -> Result<bool, ClientError> {
        let mut changed = false;
        self.cache.retain(|cached| {
            let live = shell.endpoints.iter().any(|endpoint| {
                endpoint.endpoint_id == cached.route.endpoint_id
                    && endpoint.status == ClientEndpointStatus::Online
                    && endpoints
                        .connection(&endpoint.endpoint_id)
                        .is_some_and(|connection| {
                            connection.generation == cached.route.connection_generation
                        })
                    && endpoint.snapshot.as_deref().is_some_and(|snapshot| {
                        snapshot.boot_id.as_str() == cached.route.boot_id.as_ref()
                    })
            });
            changed |= !live;
            live
        });
        for endpoint in &shell.endpoints {
            if endpoint.status != ClientEndpointStatus::Online {
                continue;
            }
            let Some(connection) = endpoints.connection(&endpoint.endpoint_id) else {
                continue;
            };
            let Some(snapshot) = endpoint
                .snapshot
                .as_deref()
                .filter(|_| endpoint.snapshot_generation == Some(connection.generation))
            else {
                continue;
            };
            if let Err(error) = connection.negotiation.require_pane_input_intent(true) {
                // A disconnected/frozen surface has no input route. Release first;
                // require the capability again once that surface is actually usable.
                if &endpoint.endpoint_id == endpoints.active_id()
                    && endpoints.active_surface_available()
                {
                    return Err(error);
                }
                continue;
            }
            let roster = snapshot
                .input_intents
                .as_ref()
                .ok_or(ClientError::InputIntentProtocolError)?;
            let index = self
                .cache
                .iter()
                .position(|cached| cached.route.endpoint_id == endpoint.endpoint_id);
            let previous = index.map(|index| &self.cache[index]);
            let same_route = previous.is_some_and(|cached| {
                cached.route.connection_generation == connection.generation
                    && cached.route.boot_id.as_ref() == snapshot.boot_id
            });
            let same_roster = same_route
                && previous.is_some_and(|cached| {
                    Arc::ptr_eq(&cached.roster, roster) || cached.roster.as_ref() == roster.as_ref()
                });
            let same_panes = same_route
                && previous.is_some_and(|cached| {
                    cached.panes.len()
                        == snapshot
                            .panes
                            .iter()
                            .filter(|pane| pane.terminal_id.is_some())
                            .count()
                        && snapshot
                            .panes
                            .iter()
                            .all(|pane| match pane.terminal_id.as_deref() {
                                Some(terminal) => cached
                                    .panes
                                    .get(&pane.pane_id)
                                    .is_some_and(|value| value.as_ref() == terminal),
                                None => !cached.panes.contains_key(&pane.pane_id),
                            })
                });
            if same_roster && same_panes {
                continue;
            }
            changed = true;
            let route = if same_route {
                previous.unwrap().route.clone()
            } else {
                Arc::new(ClientInputRoute {
                    endpoint_id: endpoint.endpoint_id.clone(),
                    connection_generation: connection.generation,
                    boot_id: Arc::from(snapshot.boot_id.as_str()),
                    terminal_target: None,
                })
            };
            let local_ui = if same_route {
                previous.unwrap().local_ui.clone()
            } else {
                Arc::new(LeaseIdentity {
                    endpoint_id: route.endpoint_id.clone(),
                    connection_generation: route.connection_generation,
                    boot_id: route.boot_id.clone(),
                    terminal_target: LeaseTarget::LocalUi,
                    reporter_session: None,
                })
            };
            let mut reporters = HashMap::new();
            let mut terminals = HashSet::new();
            for terminal in roster.iter() {
                if terminal.terminal_id.is_empty()
                    || !terminals.insert(terminal.terminal_id.as_str())
                    || terminal
                        .sessions
                        .iter()
                        .filter(|session| session.active)
                        .count()
                        > 1
                {
                    return Err(ClientError::InputIntentProtocolError);
                }
                let target: Arc<str> = previous
                    .filter(|_| same_route)
                    .and_then(|cached| {
                        cached.reporters.values().find_map(|identity| {
                            match &identity.terminal_target {
                                LeaseTarget::Terminal(value)
                                    if value.as_ref() == terminal.terminal_id =>
                                {
                                    Some(value.clone())
                                }
                                _ => None,
                            }
                        })
                    })
                    .unwrap_or_else(|| Arc::from(terminal.terminal_id.as_str()));
                for session in &terminal.sessions {
                    if session.session.is_empty()
                        || session.generation == 0
                        || reporters.contains_key(&session.session)
                    {
                        return Err(ClientError::InputIntentProtocolError);
                    }
                    let identity = previous
                        .filter(|_| same_route)
                        .and_then(|cached| cached.reporters.get(&session.session))
                        .filter(|identity| {
                            identity.terminal_target == LeaseTarget::Terminal(target.clone())
                        })
                        .cloned()
                        .unwrap_or_else(|| {
                            Arc::new(LeaseIdentity {
                                endpoint_id: route.endpoint_id.clone(),
                                connection_generation: route.connection_generation,
                                boot_id: route.boot_id.clone(),
                                terminal_target: LeaseTarget::Terminal(target.clone()),
                                reporter_session: Some(Arc::from(session.session.as_str())),
                            })
                        });
                    reporters.insert(session.session.clone(), identity);
                }
            }
            let panes = if same_panes {
                previous.unwrap().panes.clone()
            } else {
                Arc::new(
                    snapshot
                        .panes
                        .iter()
                        .filter_map(|pane| {
                            pane.terminal_id
                                .as_deref()
                                .map(|terminal| (pane.pane_id.clone(), Arc::from(terminal)))
                        })
                        .collect(),
                )
            };
            let cached = CachedEndpoint {
                route,
                roster: roster.clone(),
                panes,
                reporters,
                local_ui,
            };
            match index {
                Some(index) => self.cache[index] = cached,
                None => self.cache.push(cached),
            }
        }
        if changed {
            let mut live = Vec::new();
            for cached in &self.cache {
                live.push(cached.local_ui.clone());
                live.extend(cached.reporters.values().cloned());
            }
            self.live = live.into();
        }
        Ok(changed)
    }

    fn physical_binding(
        &self,
        shell: &ClientShellState,
        endpoints: &EndpointRegistry,
    ) -> Option<InputBinding> {
        if !endpoints.active_surface_available()
            || !shell.endpoint_is_online(endpoints.active_id())
            || &shell.active_endpoint_id != endpoints.active_id()
        {
            return None;
        }
        let cached = self
            .cache
            .iter()
            .find(|cached| &cached.route.endpoint_id == endpoints.active_id())?;
        let projected = shell.snapshot.as_deref()?;
        shell
            .require_endpoint_input_intents(
                endpoints.active_id(),
                cached.route.connection_generation,
            )
            .ok()?;
        let terminal = if shell.popup_pending {
            None
        } else if let Some(popup) = shell.popup_terminal_id.as_deref() {
            Some(
                self.binding
                    .as_ref()
                    .and_then(|binding| binding.terminal.as_ref())
                    .filter(|terminal| terminal.as_ref() == popup)
                    .cloned()
                    .unwrap_or_else(|| Arc::from(popup)),
            )
        } else {
            projected
                .focused_pane_id
                .as_ref()
                .and_then(|pane| cached.panes.get(pane))
                .cloned()
        };
        if !shell.popup_pending
            && shell.popup_terminal_id.is_none()
            && projected.focused_pane_id.is_some()
            && terminal.is_none()
        {
            return None;
        }
        let route = self
            .binding
            .as_ref()
            .map(|binding| &binding.route)
            .filter(|route| {
                route.endpoint_id == cached.route.endpoint_id
                    && route.connection_generation == cached.route.connection_generation
                    && route.boot_id == cached.route.boot_id
                    && route.terminal_target == terminal
            })
            .cloned()
            .unwrap_or_else(|| {
                Arc::new(ClientInputRoute {
                    endpoint_id: cached.route.endpoint_id.clone(),
                    connection_generation: cached.route.connection_generation,
                    boot_id: cached.route.boot_id.clone(),
                    terminal_target: terminal.clone(),
                })
            });
        Some(InputBinding {
            route,
            terminal,
            popup_pending: shell.popup_pending,
            focus_epoch: self.focus_epoch,
        })
    }

    fn commit_mouse_focus(&mut self, shell: &mut ClientShellState, binding: &InputBinding) -> bool {
        let expected = self.batches.front().is_some_and(|batch| {
            batch.binding.route.endpoint_id == binding.route.endpoint_id
                && batch.binding.route.connection_generation == binding.route.connection_generation
                && batch.binding.route.boot_id == binding.route.boot_id
                && batch.binding.focus_epoch == binding.focus_epoch
                && batch
                    .focus_target
                    .as_ref()
                    .is_some_and(|(_, terminal)| binding.terminal.as_ref() == Some(terminal))
        });
        if !expected {
            return false;
        }
        let mut continuation = self.batches.pop_front().unwrap();
        let button = continuation.events.front().and_then(|event| match event {
            RawInputEvent::Mouse(mouse) => match mouse.kind {
                MouseEventKind::Down(button) => Some(button),
                _ => None,
            },
            _ => None,
        });
        // Only the intentionally targeted mouse event and its balancing up may follow
        // this explicit focus trigger. Other captured keys remain old-target input.
        let mut first = true;
        let before = continuation.events.len();
        continuation.events.retain(|event| {
            if first { first = false; return true }
            if matches!(event, RawInputEvent::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Up(value) if Some(value) == button)) {
                return true;
            }
            Self::flush_balancing_release(shell, event, &mut self.pending_releases);
            false
        });
        let mut cancelled = usize::from(before != continuation.events.len());
        for batch in self.batches.drain(..) {
            let mut discarded = false;
            for event in batch.events {
                if matches!(&event, RawInputEvent::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Up(value) if Some(value) == button))
                {
                    continuation.events.push_back(event);
                } else {
                    discarded = true;
                    Self::flush_balancing_release(shell, &event, &mut self.pending_releases);
                }
            }
            cancelled += usize::from(discarded);
        }
        continuation.binding = binding.clone();
        continuation.focus_target = None;
        self.bytes = continuation.raw.len();
        self.batches.push_back(continuation);
        self.report_cancelled(shell, cancelled);
        true
    }

    fn local_command_event(shell: &ClientShellState, event: Option<&RawInputEvent>) -> bool {
        match event {
            Some(RawInputEvent::Key(key))
                if key.kind != KeyEventKind::Release
                    && shell.mode == ClientShellMode::Terminal
                    && shell.overlay.is_none()
                    && !shell.popup_pending
                    && shell.popup_terminal_id.is_none()
                    && !shell.ime_forward_prefix =>
            {
                crate::input::resolve_direct_binding(&shell.config.keybinds.keybinds, key).is_some()
                    || shell.config.keybinds.matches_prefix(key)
                    || shell
                        .selection
                        .as_ref()
                        .is_some_and(crate::selection::Selection::is_visible)
            }
            // Chrome mouse actions are Herdr commands; forwarded mouse traffic uses the
            // current terminal intent. A click on a different pane is staged below.
            Some(RawInputEvent::Mouse(mouse))
                if matches!(mouse.kind, MouseEventKind::Up(_))
                    && shell.pane_mouse_gesture.is_some() =>
            {
                false
            }
            // Popup/Pending occludes all underlying pane/chrome hits, including
            // positions outside the popup. Match handle_mouse before pane hit-testing.
            Some(RawInputEvent::Mouse(_))
                if shell.popup_pending
                    || shell.popup_terminal_id.is_some()
                    || shell.hits.popup.is_some() =>
            {
                false
            }
            Some(RawInputEvent::Mouse(mouse)) => {
                let hit = shell
                    .hits
                    .panes
                    .iter()
                    .find(|hit| contains(hit.inner_rect, (mouse.column, mouse.row)));
                hit.is_some_and(|hit| {
                    shell.focused_pane_id().as_deref() != Some(hit.pane_id.as_str())
                }) || hit.is_none()
            }
            _ => false,
        }
    }

    pub(in crate::client) fn synchronize(
        &mut self,
        shell: &mut ClientShellState,
        endpoints: &EndpointRegistry,
        frozen: bool,
        now: Instant,
    ) -> Result<(), ClientError> {
        let cache_changed = self.update_cache(shell, endpoints)?;
        let focused = shell.outer_focused != Some(false);
        let focus_changed = focused != self.focused;
        if focused != self.focused {
            self.focused = focused;
            self.focus_epoch = self.focus_epoch.wrapping_add(1);
            self.episode = focused;
            self.sample_pending = focused;
        }
        let binding = if frozen {
            None
        } else {
            self.physical_binding(shell, endpoints)
        };
        if self.binding != binding {
            let mouse_focus = !focus_changed
                && focused
                && binding
                    .as_ref()
                    .is_some_and(|binding| self.commit_mouse_focus(shell, binding));
            if !mouse_focus {
                self.cancel(shell);
            }
            // A snapshot/boot/target change is not a keyboard/focus episode.
            self.authorization = None;
            self.episode = mouse_focus || (focus_changed && focused);
            self.sample_pending = mouse_focus || (focus_changed && focused);
            self.binding = binding;
            self.unreported = self.binding.as_ref().and_then(|binding| {
                binding.terminal.as_ref().map(|terminal| {
                    Arc::new(LeaseIdentity {
                        endpoint_id: binding.route.endpoint_id.clone(),
                        connection_generation: binding.route.connection_generation,
                        boot_id: binding.route.boot_id.clone(),
                        terminal_target: LeaseTarget::Terminal(terminal.clone()),
                        reporter_session: None,
                    })
                })
            });
        }
        shell.input_route = self.binding.as_ref().map(|binding| binding.route.clone());
        self.waiting_target = self
            .batches
            .front()
            .is_some_and(|batch| batch.focus_target.is_some());
        let event = self.batches.front().and_then(|batch| batch.events.front());
        let pending_action = self
            .batches
            .front()
            .is_some_and(|batch| batch.pending_outcome.is_some());
        self.release_ready =
            !pending_action && event.is_some_and(|event| is_balancing_release(event, shell));
        let local_command = pending_action || Self::local_command_event(shell, event);
        let cached = self
            .cache
            .iter()
            .find(|cached| &cached.route.endpoint_id == endpoints.active_id());
        if &self.fallback.endpoint_id != endpoints.active_id() {
            self.fallback = Arc::new(LeaseIdentity {
                endpoint_id: endpoints.active_id().clone(),
                connection_generation: 0,
                boot_id: Arc::from(""),
                terminal_target: LeaseTarget::LocalUi,
                reporter_session: None,
            });
        }
        let fallback = cached
            .map(|cached| cached.local_ui.clone())
            .unwrap_or_else(|| self.fallback.clone());
        let mut identity = fallback;
        let mut desired = None;
        let mut generation = 0;
        if self.focused && self.binding.is_some() && !frozen && !self.waiting_target {
            let cached = cached.expect("binding requires cache");
            if !shell.ime_forward_prefix
                && (shell.overlay.is_some()
                    || shell.mode != ClientShellMode::Terminal
                    || local_command
                    || shell
                        .copy_mode
                        .as_ref()
                        .is_some_and(|copy| copy.search_prompt.is_some()))
            {
                let state = if shell.modal_paste_target_active() {
                    InputIntentState::Text
                } else if local_command || shell.wants_ascii_input() {
                    InputIntentState::Command
                } else {
                    InputIntentState::Text
                };
                identity = cached.local_ui.clone();
                desired = Some(DesiredLease {
                    identity: identity.clone(),
                    policy: InputIntentPolicy::Mode,
                    state,
                });
            } else if let Some(terminal) = self
                .binding
                .as_ref()
                .and_then(|binding| binding.terminal.as_ref())
            {
                if let Some(roster) = cached
                    .roster
                    .iter()
                    .find(|roster| roster.terminal_id.as_str() == terminal.as_ref())
                {
                    if let Some(session) = roster.sessions.iter().find(|session| session.active) {
                        identity = cached.reporters[&session.session].clone();
                        generation = session.generation;
                        if session.policy != InputIntentPolicy::Unknown
                            && session.state != InputIntentState::Unknown
                            && !(session.policy == InputIntentPolicy::Entry
                                && session.state != InputIntentState::Command)
                        {
                            desired = Some(DesiredLease {
                                identity: identity.clone(),
                                policy: session.policy,
                                state: session.state,
                            });
                        }
                    }
                }
                if identity.reporter_session.is_none() {
                    identity = self
                        .unreported
                        .as_ref()
                        .expect("terminal binding has release identity")
                        .clone();
                }
            }
        }
        self.copy_blocked = shell.copy_operation_in_flight;
        let context = (
            shell.mode,
            shell.overlay.as_ref().map(ClientShellOverlay::kind),
            shell.modal_paste_target_active(),
        );
        let same = self.authorization.as_ref().is_some_and(|authorization| {
            authorization.identity == identity
                && authorization.intent_generation == generation
                && authorization.focus_epoch == self.focus_epoch
        }) && self.desired == desired
            && self.ui_context.as_ref() == Some(&context);
        self.ui_context = Some(context);
        if !same {
            self.arbitration_epoch = self.arbitration_epoch.wrapping_add(1);
            self.authorization = Some(Arc::new(AuthorizationKey {
                identity,
                intent_generation: generation,
                focus_epoch: self.focus_epoch,
                arbitration_epoch: self.arbitration_epoch,
            }));
            self.desired = desired;
            self.applied = false;
            self.inactive = false;
            self.submitted = false;
        }
        if let Some(deadline) = self.deadline() {
            if now >= deadline {
                return Err(ClientError::ImeAckTimeout);
            }
        }
        if !self.submitted || cache_changed {
            let plan = WorkerPlan {
                authorization: self.authorization.as_ref().unwrap().clone(),
                desired: self.desired.clone(),
                live_leases: self.live.clone(),
                start_episode: self.focused
                    && (self.sample_pending
                        || (self.episode
                            && self
                                .batches
                                .iter()
                                .any(|batch| batch.events.iter().any(|event| !is_release(event))))),
                deadline: self.deadline().unwrap_or(now + INPUT_WAIT),
            };
            if let Some(worker) = self.worker.as_ref() {
                worker.submit(plan).map_err(ClientError::ImeControl)?
            } else {
                self.pending_plan = Some(plan)
            }
            self.submitted = true;
            self.sample_pending = false;
        }
        shell.ime_authorization = self
            .applied
            .then(|| self.authorization.as_ref().unwrap().clone());
        Ok(())
    }

    pub(in crate::client) fn complete(
        &mut self,
        completion: Completion,
    ) -> Result<(), ClientError> {
        // Errors on obsolete work are not source authorization either. The latest release
        // remains queued by the worker; only the exact current tuple can affect this gate.
        if self.authorization.as_deref() != Some(completion.authorization.as_ref()) {
            return Ok(());
        }
        match completion.result.map_err(ClientError::ImeControl)? {
            AckScope::Applied => {
                self.applied = true;
                self.inactive = false;
            }
            AckScope::Inactive => {
                self.applied = false;
                self.inactive = true;
                self.episode = false;
            }
        }
        Ok(())
    }

    pub(in crate::client) fn enqueue(
        &mut self,
        shell: &mut ClientShellState,
        endpoints: &EndpointRegistry,
        raw: Vec<u8>,
        events: Vec<RawInputEvent>,
        pixels: Option<crate::input::mouse::HostPixels>,
        frozen: bool,
        now: Instant,
    ) -> Result<ClientShellInput, ClientError> {
        self.synchronize(shell, endpoints, frozen, now)?;
        // A disconnected or incoherent projection has no input route.
        // Cancel new semantic input; retain independent releases and focus handling below.
        let frozen = frozen || self.binding.is_none();
        let mut semantic: VecDeque<RawInputEvent> = events.into();
        let mut outcome = ClientShellInput::default();
        let mut discarded = false;
        // Rotate kept semantic events into this same parser allocation. The first
        // `remaining` events are still unread; only the tail belongs to this episode.
        for remaining in (0..semantic.len()).rev() {
            let event = semantic.pop_front().unwrap();
            if let RawInputEvent::Key(key) = &event {
                if key.kind == KeyEventKind::Release {
                    let identity = crate::input::InputLeaseKey::new(0_u8, key);
                    let buffered = self.batches.iter().any(|batch| batch.events.iter().any(|event| matches!(event, RawInputEvent::Key(pending) if crate::input::InputLeaseKey::new(0_u8, pending) == identity)))
                        || semantic.iter().skip(remaining).any(|event| matches!(event, RawInputEvent::Key(pending) if crate::input::InputLeaseKey::new(0_u8, pending) == identity));
                    if shell.release_tracked_key(key, &mut outcome) {
                        self.cancel_buffered_key(shell, key);
                        let mut index = 0;
                        semantic.retain(|event| {
                            let keep = index < remaining || !matches!(event, RawInputEvent::Key(pending) if crate::input::InputLeaseKey::new(0_u8, pending) == identity);
                            index += 1;
                            keep
                        });
                    } else if buffered {
                        semantic.push_back(event);
                    }
                    continue;
                }
            }
            // Endpoint chrome is client-owned, not input for the frozen pane.
            // Keep its selection and balancing release on the native mouse route.
            let recovery_mouse = frozen
                && shell.outer_focused != Some(false)
                && shell.overlay.is_none()
                && matches!(&event, RawInputEvent::Mouse(mouse) if
                    (mouse.kind == MouseEventKind::Down(MouseButton::Left)
                        && shell.endpoint_recovery_hit((mouse.column, mouse.row)))
                    || (mouse.kind == MouseEventKind::Up(MouseButton::Left)
                        && shell.workspace_press.is_some()));
            let urgent = recovery_mouse
                || matches!(
                    &event,
                    RawInputEvent::OuterFocusLost | RawInputEvent::OuterFocusGained
                )
                || matches!(&event, RawInputEvent::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Up(button) if shell.pane_mouse_gesture.as_ref().is_some_and(|gesture| gesture.button == button)))
                || !matches!(
                    &event,
                    RawInputEvent::Key(_)
                        | RawInputEvent::Text(_)
                        | RawInputEvent::Paste(_)
                        | RawInputEvent::Mouse(_)
                );
            if urgent {
                if matches!(event, RawInputEvent::OuterFocusGained) {
                    discarded |= semantic.len() > remaining;
                    semantic.truncate(remaining);
                    // A real focus report starts a fresh episode even if the prior
                    // FocusLost was omitted. The daemon independently verifies focus.
                    self.focused = false;
                }
                if matches!(event, RawInputEvent::OuterFocusLost) {
                    discarded |= semantic.len() > remaining;
                    semantic.truncate(remaining);
                    self.cancel(shell);
                    shell.outer_focused = Some(false);
                    self.synchronize(shell, endpoints, frozen, now)?;
                }
                merge(&mut outcome, shell.handle_raw_events([event]));
                self.synchronize(shell, endpoints, frozen, now)?;
            } else if shell.outer_focused != Some(false) && !frozen {
                semantic.push_back(event);
            } else {
                discarded = true;
            }
        }
        if !semantic.is_empty() {
            let was_empty = self.batches.is_empty();
            crate::client::shell_runtime::require_client_input_intents(
                Some(shell),
                endpoints,
                true,
            )?;
            let binding = self
                .binding
                .clone()
                .ok_or(ClientError::InputIntentProtocolError)?;
            if self.batches.len() >= INPUT_BATCH_LIMIT
                || raw.len() > INPUT_BYTE_LIMIT.saturating_sub(self.bytes)
            {
                return Err(ClientError::ImeInputOverflow);
            }
            // Share an outstanding foreground read for this exact tuple, never a
            // cached Applied result. Invalidating every waiting batch would starve
            // slow RPCs indefinitely while a user continues typing.
            if semantic.iter().any(|event| !is_release(event))
                && (was_empty || self.applied || self.inactive || !self.episode || !self.submitted)
            {
                self.episode = true;
                self.sample_pending = true;
                self.arbitration_epoch = self.arbitration_epoch.wrapping_add(1);
                if let Some(key) = self.authorization.as_ref() {
                    self.authorization = Some(Arc::new(AuthorizationKey {
                        identity: key.identity.clone(),
                        intent_generation: key.intent_generation,
                        focus_epoch: key.focus_epoch,
                        arbitration_epoch: self.arbitration_epoch,
                    }));
                }
                self.applied = false;
                self.inactive = false;
                self.submitted = false;
            }
            self.bytes += raw.len();
            self.batches.push_back(InputBatch {
                raw,
                events: semantic,
                cursor: 0,
                image_checked: false,
                forward_prefix: false,
                focus_target: None,
                pending_outcome: None,
                replay: false,
                pixels,
                binding,
                deadline: now + INPUT_WAIT,
            });
            // Submit with the original oldest deadline, not an earlier idle plan's budget.
            if was_empty {
                self.submitted = false;
            }
            self.synchronize(shell, endpoints, frozen, now)?;
        }
        if discarded {
            self.report_cancelled(shell, 1);
        }
        if let Some(releases) = self.take_releases() {
            merge(&mut outcome, releases);
        }
        outcome.repaint |= self.take_repaint();
        Ok(outcome)
    }
    pub(in crate::client) fn enqueue_replay(
        &mut self,
        shell: &mut ClientShellState,
        endpoints: &EndpointRegistry,
        replay: ClientMouseReplay,
        frozen: bool,
        now: Instant,
    ) -> Result<ClientShellInput, ClientError> {
        self.synchronize(shell, endpoints, frozen, now)?;
        let origin = replay.origin.ok_or(ClientError::InputIntentProtocolError)?;
        if !self.focused
            || frozen
            || self.authorization.as_deref() != Some(origin.authorization.as_ref())
            || !self
                .binding
                .as_ref()
                .is_some_and(|binding| binding.route == origin.route)
        {
            self.report_cancelled(shell, 1);
            return Ok(ClientShellInput {
                repaint: self.take_repaint(),
                ..Default::default()
            });
        }
        if now >= origin.deadline {
            return Err(ClientError::ImeAckTimeout);
        }
        if self.batches.len() >= INPUT_BATCH_LIMIT {
            return Err(ClientError::ImeInputOverflow);
        }
        self.arbitration_epoch = self.arbitration_epoch.wrapping_add(1);
        let key = self.authorization.as_ref().unwrap();
        self.authorization = Some(Arc::new(AuthorizationKey {
            identity: key.identity.clone(),
            intent_generation: key.intent_generation,
            focus_epoch: key.focus_epoch,
            arbitration_epoch: self.arbitration_epoch,
        }));
        self.applied = false;
        self.submitted = false;
        // This is the retained original TTY click, not a business-message input
        // episode. Its original tuple and five-second deadline are both retained.
        self.batches.push_back(InputBatch {
            raw: Vec::new(),
            events: replay
                .events
                .into_iter()
                .map(RawInputEvent::Mouse)
                .collect(),
            cursor: 0,
            image_checked: true,
            forward_prefix: false,
            focus_target: None,
            pending_outcome: None,
            replay: true,
            pixels: None,
            binding: self.binding.as_ref().unwrap().clone(),
            deadline: origin.deadline,
        });
        self.synchronize(shell, endpoints, frozen, now)?;
        Ok(ClientShellInput::default())
    }

    pub(in crate::client) fn dispatch(
        &mut self,
        shell: &mut ClientShellState,
        endpoints: &mut EndpointRegistry,
        remote: bool,
        paste_key: Option<(crossterm::event::KeyCode, crossterm::event::KeyModifiers)>,
        frozen: bool,
        now: Instant,
    ) -> Result<Option<ClientShellInput>, ClientError> {
        self.synchronize(shell, endpoints, frozen, now)?;
        if let Some(releases) = self.take_releases() {
            return Ok(Some(releases));
        }
        let Some(batch) = self.batches.front_mut() else {
            return Ok(None);
        };
        if Some(&batch.binding) != self.binding.as_ref() {
            self.cancel(shell);
            return Ok(None);
        }
        if batch.pending_outcome.is_some() {
            if !self.applied || self.waiting_target {
                return Ok(None);
            }
            let outcome = batch.pending_outcome.take().unwrap();
            if batch.events.is_empty() {
                let batch = self.batches.pop_front().unwrap();
                self.bytes -= batch.raw.len();
            }
            self.sample_pending = true;
            return Ok(Some(outcome));
        }
        // Double-prefix changes mode AND forwards the trigger. Consume only the pure
        // transition now, and route/track the press once after the new target ACK.
        if !batch.forward_prefix && shell.mode == ClientShellMode::Prefix && shell.overlay.is_none() && batch.events.front().is_some_and(|event| matches!(event, RawInputEvent::Key(key) if key.kind == KeyEventKind::Press && shell.config.keybinds.matches_prefix(key))) {
            shell.mode = shell.copy_or_terminal_mode();
            shell.ime_forward_prefix = true;
            batch.forward_prefix = true;
            self.sample_pending = true;
            self.synchronize(shell, endpoints, frozen, now)?;
            return Ok(Some(ClientShellInput { repaint: true, ..Default::default() }));
        }
        let release = batch
            .events
            .front()
            .is_some_and(|event| is_balancing_release(event, shell));
        if !release && (!self.applied || self.copy_blocked || self.waiting_target) {
            return Ok(None);
        }
        let batch = self.batches.front_mut().unwrap();
        if !release
            && shell.overlay.is_none()
            && !shell.popup_pending
            && shell.popup_terminal_id.is_none()
            && shell.pane_mouse_gesture.is_none()
        {
            let hit = batch.events.front().and_then(|event| match event {
                RawInputEvent::Mouse(mouse)
                    if matches!(
                        mouse.kind,
                        MouseEventKind::Down(_)
                            | MouseEventKind::ScrollUp
                            | MouseEventKind::ScrollDown
                            | MouseEventKind::ScrollLeft
                            | MouseEventKind::ScrollRight
                    ) =>
                {
                    shell.hits.panes.iter().find(|hit| {
                        contains(hit.inner_rect, (mouse.column, mouse.row))
                            && shell.focused_pane_id().as_deref() != Some(hit.pane_id.as_str())
                    })
                }
                _ => None,
            });
            if let Some(hit) = hit {
                let terminal = self
                    .cache
                    .iter()
                    .find(|cached| {
                        cached.route.endpoint_id == batch.binding.route.endpoint_id
                            && cached.route.connection_generation
                                == batch.binding.route.connection_generation
                            && cached.route.boot_id == batch.binding.route.boot_id
                    })
                    .and_then(|cached| cached.panes.get(&hit.pane_id))
                    .cloned()
                    .ok_or(ClientError::InputIntentProtocolError)?;
                batch.focus_target = Some((hit.pane_id.clone(), terminal));
                batch.image_checked = true;
                let mut outcome = ClientShellInput::default();
                shell.push_endpoint_method(
                    crate::api::schema::Method::PaneFocus(crate::api::schema::PaneTarget {
                        pane_id: hit.pane_id.clone(),
                    }),
                    &mut outcome,
                );
                return Ok(Some(outcome));
            }
        }
        if !release && !batch.image_checked {
            batch.image_checked = true;
            #[cfg(unix)]
            if let Some(target) = shell.clipboard_image_target() {
                let bridge = crate::client::endpoint_accepts_local_images(
                    remote,
                    endpoints.active_id(),
                    endpoints.active_surface_available(),
                );
                let image = if crate::client::should_bridge_clipboard_image_paste(
                    &batch.raw, bridge, paste_key,
                ) {
                    crate::platform::read_clipboard_image()
                } else {
                    crate::client::read_image_file_from_terminal_drop(&batch.raw, bridge)
                };
                if let Some(image) = image {
                    crate::client::write_remote_image_to_server(
                        endpoints,
                        target,
                        image,
                        "IME-authorized clipboard paste",
                    )?;
                    let batch = self.batches.pop_front().unwrap();
                    self.bytes -= batch.raw.len();
                    return Ok(Some(ClientShellInput::default()));
                }
            }
        }
        let Some(event) = batch.events.pop_front() else {
            let batch = self.batches.pop_front().unwrap();
            self.bytes -= batch.raw.len();
            return Ok(Some(ClientShellInput::default()));
        };
        batch.cursor += 1;
        shell.host_mouse_pixels = batch.pixels;
        shell.ime_forward_prefix = batch.forward_prefix;
        shell.ime_input_deadline = Some(batch.deadline);
        shell.replaying_url_click = batch.replay;
        let prior_ui = (
            shell.mode,
            shell.overlay.as_ref().map(ClientShellOverlay::kind),
            shell.modal_paste_target_active(),
        );
        let command_trigger = Self::local_command_event(shell, Some(&event));
        batch.forward_prefix = false;
        let outcome = shell.handle_raw_events([event]);
        shell.host_mouse_pixels = None;
        shell.ime_forward_prefix = false;
        shell.ime_input_deadline = None;
        shell.replaying_url_click = false;
        let resulting_ui = (
            shell.mode,
            shell.overlay.as_ref().map(ClientShellOverlay::kind),
            shell.modal_paste_target_active(),
        );
        if !release
            && (resulting_ui != prior_ui
                || (command_trigger
                    && shell.mode == ClientShellMode::Terminal
                    && shell.overlay.is_none()))
        {
            self.sample_pending = true;
        }
        // A mode-trigger's pure UI mutation is already consumed, but its remaining
        // command must wait for that new arbitration. Retain the outcome, not the
        // trigger, so it cannot run twice or forward under the old authorization.
        if !release
            && resulting_ui != prior_ui
            && (!outcome.actions.is_empty()
                || !outcome.requests.is_empty()
                || !outcome.routed_requests.is_empty()
                || outcome.detach
                || outcome.resize
                || outcome.query_host_appearance
                || outcome.query_host_theme)
        {
            self.batches.front_mut().unwrap().pending_outcome = Some(outcome);
            self.synchronize(shell, endpoints, frozen, now)?;
            return Ok(Some(ClientShellInput {
                repaint: true,
                ..Default::default()
            }));
        }
        if self.batches.front().unwrap().events.is_empty() {
            let batch = self.batches.pop_front().unwrap();
            self.bytes -= batch.raw.len();
        }
        Ok(Some(outcome))
    }
}
fn is_release(event: &RawInputEvent) -> bool {
    matches!(event, RawInputEvent::Key(key) if key.kind == KeyEventKind::Release)
        || matches!(event, RawInputEvent::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Up(_)))
}

fn is_balancing_release(event: &RawInputEvent, shell: &ClientShellState) -> bool {
    matches!(event, RawInputEvent::Key(key) if key.kind == KeyEventKind::Release)
        || matches!(event, RawInputEvent::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Up(button) if shell.pane_mouse_gesture.as_ref().is_some_and(|gesture| gesture.button == button)))
}

fn merge(target: &mut ClientShellInput, mut source: ClientShellInput) {
    target.detach |= source.detach;
    target.repaint |= source.repaint;
    target.resize |= source.resize;
    target.query_host_appearance |= source.query_host_appearance;
    target.query_host_theme |= source.query_host_theme;
    target.requests.append(&mut source.requests);
    target.routed_requests.append(&mut source.routed_requests);
    target.actions.append(&mut source.actions);
}

impl ClientShellState {
    pub(crate) fn enable_ime_control(&mut self) {
        self.ime_control_enabled = true;
        self.pending_input_source_changes.clear();
    }
    pub(super) fn mouse_replay_origin(&self) -> Option<ClientReplayOrigin> {
        Some(ClientReplayOrigin {
            route: self.input_route.as_ref()?.clone(),
            authorization: self.ime_authorization.as_ref()?.clone(),
            deadline: self.ime_input_deadline?,
        })
    }

    pub(crate) fn endpoint_route_boot_matches(
        &self,
        endpoint_id: &ClientEndpointId,
        boot: &str,
    ) -> bool {
        self.endpoints
            .iter()
            .find(|endpoint| &endpoint.endpoint_id == endpoint_id)
            .and_then(|endpoint| endpoint.snapshot.as_deref())
            .is_some_and(|snapshot| snapshot.boot_id == boot)
    }

    pub(in crate::client) fn input_route_target_matches(
        &self,
        route: &ClientInputRoute,
        request: &ClientMessage,
    ) -> bool {
        let Some(terminal) = route.terminal_target.as_deref() else {
            return false;
        };
        match request {
            ClientMessage::ClientShellPaneInput { pane_id, .. } => self
                .endpoints
                .iter()
                .find(|endpoint| endpoint.endpoint_id == route.endpoint_id)
                .and_then(|endpoint| endpoint.snapshot.as_deref())
                .and_then(|snapshot| snapshot.panes.iter().find(|pane| &pane.pane_id == pane_id))
                .is_some_and(|pane| pane.terminal_id.as_deref() == Some(terminal)),
            ClientMessage::ClientShellPopupInput { terminal_id, .. } => terminal_id == terminal,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::InputIntentSession;
    use crate::client::endpoint::{
        EndpointNegotiation, EndpointTransport, ProfileId, SavedSshEndpoint,
    };
    use crossterm::event::{KeyCode, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
    use std::sync::mpsc;

    struct RecordingTransport(mpsc::Sender<ClientMessage>);

    impl EndpointTransport for RecordingTransport {
        fn send(&mut self, message: &ClientMessage) -> std::io::Result<()> {
            self.0.send(message.clone()).map_err(std::io::Error::other)
        }
    }

    fn negotiation() -> EndpointNegotiation {
        EndpointNegotiation::new(
            Vec::new(),
            vec![crate::protocol::endpoint::PANE_INPUT_INTENT_CAPABILITY.into()],
        )
    }

    fn reporter(state: InputIntentState) -> TerminalInputIntents {
        TerminalInputIntents {
            terminal_id: "terminal-1".into(),
            sessions: vec![InputIntentSession {
                session: "reporter-a".into(),
                generation: 1,
                policy: InputIntentPolicy::Mode,
                state,
                active: true,
            }],
        }
    }

    pub(super) fn fixture(
        state: Option<InputIntentState>,
    ) -> (
        ImeGate,
        ClientShellState,
        EndpointRegistry,
        mpsc::Receiver<ClientMessage>,
    ) {
        let (sender, messages) = mpsc::channel();
        let registry = EndpointRegistry::new(RecordingTransport(sender), 1, negotiation());
        let mut shell = ClientShellState::new(ClientShellConfig::from_config(
            &crate::config::Config::default(),
        ));
        shell.enable_ime_control();
        let mut snapshot = super::super::tests::snapshot();
        snapshot.panes[0].terminal_id = Some("terminal-1".into());
        snapshot.input_intents = Some(state.map(reporter).into_iter().collect::<Vec<_>>().into());
        shell.set_endpoint_snapshot_for_generation(&ClientEndpointId::Local, 1, Box::new(snapshot));
        shell.set_pane_surface(super::super::tests::surface());
        (ImeGate::with_worker(None), shell, registry, messages)
    }

    pub(super) fn enqueue(
        gate: &mut ImeGate,
        shell: &mut ClientShellState,
        registry: &EndpointRegistry,
        bytes: &[u8],
        now: Instant,
    ) {
        gate.enqueue(
            shell,
            registry,
            bytes.to_vec(),
            crate::raw_input::parse_raw_input_bytes_sync(bytes),
            None,
            false,
            now,
        )
        .unwrap();
    }

    pub(super) fn applied(gate: &mut ImeGate) {
        gate.complete(Completion {
            authorization: gate.authorization.as_ref().unwrap().clone(),
            result: Ok(AckScope::Applied),
        })
        .unwrap();
    }

    pub(super) fn dispatch(
        gate: &mut ImeGate,
        shell: &mut ClientShellState,
        registry: &mut EndpointRegistry,
        now: Instant,
    ) -> Option<ClientShellInput> {
        gate.dispatch(shell, registry, false, None, false, now)
            .unwrap()
    }

    fn send(shell: &ClientShellState, registry: &mut EndpointRegistry, outcome: ClientShellInput) {
        for (route, request) in outcome.routed_requests {
            crate::client::shell_runtime::send_bound_input(Some(shell), registry, &route, &request);
        }
        for request in outcome.requests {
            let target = registry.active_id().clone();
            registry.send_to(&target, &request);
        }
    }

    #[test]
    fn frozen_ime_input_keeps_local_recovery_controls_without_forwarding_pane_input() {
        for disconnected in [false, true] {
            for workspace in [false, true] {
                let (mut gate, mut shell, mut registry, messages) =
                    fixture(Some(InputIntentState::Command));
                let profile = SavedSshEndpoint {
                    id: ProfileId::parse("0123456789abcdef0123456789abcdef").unwrap(),
                    label: "remote".into(),
                    target: "dev@example".into(),
                    session: "fixture".into(),
                    enabled: true,
                };
                let remote = ClientEndpointId::Ssh(profile.id.clone());
                shell.set_endpoint_catalog(std::slice::from_ref(&profile));
                let mut snapshot = super::super::tests::snapshot();
                snapshot.boot_id = "remote-boot".into();
                snapshot.panes[0].terminal_id = Some("remote-terminal".into());
                snapshot.input_intents = Some(Arc::from([]));
                shell.set_endpoint_snapshot_for_generation(&remote, 7, Box::new(snapshot));
                shell.set_endpoint_status(&remote, ClientEndpointStatus::Online);
                let (remote_sender, remote_messages) = mpsc::channel();
                registry.insert(
                    remote.clone(),
                    RecordingTransport(remote_sender),
                    7,
                    negotiation(),
                    true,
                );
                if disconnected {
                    assert!(shell.activate_endpoint_projection(&remote));
                    assert!(registry.set_active(&remote));
                    shell.mark_endpoint_disconnected(&remote);
                    registry.set_surface_active(&remote, false);
                } else {
                    let mut pending = ClientShellInput::default();
                    assert!(shell.activate_endpoint(remote, &mut pending));
                }
                shell.compose(100, 28).unwrap();
                let rect = if workspace {
                    shell
                        .hits
                        .workspaces
                        .iter()
                        .find(|hit| hit.endpoint_id.is_local())
                        .unwrap()
                        .rect
                } else {
                    shell
                        .hits
                        .machines
                        .iter()
                        .find(|hit| hit.endpoint_id.is_local())
                        .unwrap()
                        .rect
                };
                let raw = format!(
                    "\x1b[<0;{};{}M\x1b[<0;{};{}mignored",
                    rect.x + 6,
                    rect.y + 1,
                    rect.x + 6,
                    rect.y + 1,
                )
                .into_bytes();
                let events = crate::raw_input::parse_raw_input_bytes_sync(&raw);
                let outcome = gate
                    .enqueue(
                        &mut shell,
                        &registry,
                        raw,
                        events,
                        None,
                        !disconnected,
                        Instant::now(),
                    )
                    .unwrap();
                assert!(
                    matches!(outcome.actions.as_slice(), [ClientShellAction::ActivateEndpoint {
                        endpoint_id: ClientEndpointId::Local, target,
                    }] if target.is_some() == workspace),
                    "Local recovery discarded: disconnected={disconnected}, workspace={workspace}"
                );
                assert!(outcome.requests.is_empty());
                assert!(outcome.routed_requests.is_empty());
                assert!(messages.try_recv().is_err());
                assert!(remote_messages.try_recv().is_err());
                assert!(gate.batches.is_empty());
            }
        }
    }

    #[test]
    fn unavailable_surface_releases_before_rechecking_capability() {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        applied(&mut gate);
        let old_authorization = gate.authorization.as_ref().unwrap().clone();
        let snapshot = shell.snapshot.as_ref().unwrap().as_ref().clone();
        let (sender, _receiver) = mpsc::channel();
        registry.insert(
            ClientEndpointId::Local,
            RecordingTransport(sender),
            2,
            EndpointNegotiation::default(),
            false,
        );
        registry.freeze_input();
        shell.set_endpoint_snapshot_for_generation(&ClientEndpointId::Local, 2, Box::new(snapshot));
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        assert!(gate.pending_plan.as_ref().unwrap().desired.is_none());
        assert!(gate.pending_plan.as_ref().unwrap().live_leases.is_empty());
        assert!(!gate.applied);
        gate.complete(Completion {
            authorization: old_authorization,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(!gate.applied);
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::TryRecvError::Disconnected) | Err(mpsc::TryRecvError::Empty)
        ));
        registry.set_surface_active(&ClientEndpointId::Local, true);
        registry.unfreeze_input();
        assert!(matches!(
            gate.synchronize(&mut shell, &registry, false, now),
            Err(ClientError::InputIntentUnsupported)
        ));
    }

    #[test]
    fn disconnected_input_is_cancelled_without_replay_after_reconnect() {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        applied(&mut gate);
        let old_authorization = gate.authorization.as_ref().unwrap().clone();
        let mut snapshot = shell.snapshot.as_ref().unwrap().as_ref().clone();
        registry.disconnect(&ClientEndpointId::Local);
        shell.mark_endpoint_disconnected(&ClientEndpointId::Local);
        let outcome = gate
            .enqueue(
                &mut shell,
                &registry,
                b"j".to_vec(),
                crate::raw_input::parse_raw_input_bytes_sync(b"j"),
                None,
                false,
                now,
            )
            .unwrap();
        assert!(outcome.requests.is_empty() && outcome.routed_requests.is_empty());
        assert!(gate.batches.is_empty());
        assert!(gate.pending_plan.as_ref().unwrap().desired.is_none());
        assert!(gate.pending_plan.as_ref().unwrap().live_leases.is_empty());
        assert!(shell
            .endpoint_error
            .as_deref()
            .is_some_and(|error| error.starts_with("IME_INPUT_CANCELLED:")));
        gate.complete(Completion {
            authorization: old_authorization,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(!gate.applied);
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        let (sender, reconnected) = mpsc::channel();
        registry.insert(
            ClientEndpointId::Local,
            RecordingTransport(sender),
            2,
            negotiation(),
            false,
        );
        shell.set_endpoint_status(&ClientEndpointId::Local, ClientEndpointStatus::Online);
        registry.set_surface_active(&ClientEndpointId::Local, true);
        enqueue(&mut gate, &mut shell, &registry, b"j", now);
        assert!(gate.batches.is_empty());
        assert!(matches!(
            reconnected.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        snapshot.revision += 1;
        shell.cache_endpoint_snapshot_for_generation(
            &ClientEndpointId::Local,
            2,
            Box::new(snapshot.clone()),
        );
        enqueue(&mut gate, &mut shell, &registry, b"j", now);
        assert!(gate.batches.is_empty());
        assert!(matches!(
            reconnected.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        registry.set_surface_active(&ClientEndpointId::Local, false);
        shell.set_endpoint_snapshot_for_generation(&ClientEndpointId::Local, 2, Box::new(snapshot));
        shell.set_endpoint_status(&ClientEndpointId::Local, ClientEndpointStatus::Online);
        enqueue(&mut gate, &mut shell, &registry, b"j", now);
        assert!(gate.batches.is_empty());
        assert!(matches!(
            reconnected.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        registry.set_surface_active(&ClientEndpointId::Local, true);
        enqueue(&mut gate, &mut shell, &registry, b"k", now);
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        assert!(matches!(
            reconnected.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        applied(&mut gate);
        let outcome = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, outcome);
        let forwarded = reconnected.try_iter().collect::<Vec<_>>();
        assert!(
            matches!(forwarded.as_slice(), [ClientMessage::ClientShellPaneInput { events, .. }]
            if matches!(events.as_slice(), [crate::protocol::ClientPaneInputEvent::Key { generated_text: Some(text), .. }] if text == "k"))
        );
        let mut malformed = shell.snapshot.as_ref().unwrap().as_ref().clone();
        malformed.input_intents = None;
        shell.set_endpoint_snapshot_for_generation(
            &ClientEndpointId::Local,
            2,
            Box::new(malformed),
        );
        assert!(matches!(
            gate.synchronize(&mut shell, &registry, false, now),
            Err(ClientError::InputIntentProtocolError)
        ));
    }

    #[test]
    fn blocked_ack_keeps_two_batches_fifo_and_dispatches_each_press_once() {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"x", now);
        enqueue(
            &mut gate,
            &mut shell,
            &registry,
            b"y",
            now + Duration::from_millis(1),
        );
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        applied(&mut gate);
        let first = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, first);
        let second = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, second);
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        let messages: Vec<_> = messages.try_iter().collect();
        let characters: Vec<_> = messages
            .iter()
            .flat_map(|message| match message {
                ClientMessage::ClientShellPaneInput { events, .. } => events
                    .iter()
                    .filter_map(|event| match event {
                        ClientPaneInputEvent::Key {
                            code: crate::protocol::ClientKeyCode::Char(character),
                            ..
                        } => Some(*character),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                _ => Vec::new(),
            })
            .collect();
        assert_eq!(characters, vec!['x', 'y']);
    }

    #[cfg(unix)]
    #[test]
    fn escape_prefixed_host_bindings_wait_for_ack_and_keep_their_pane_key_semantics() {
        use crate::protocol::{ClientKeyCode, ClientKeyKind};

        for (bytes, code, modifiers) in [
            (b"\x1b".as_slice(), ClientKeyCode::Esc, KeyModifiers::NONE),
            (b"\x1bb", ClientKeyCode::Char('b'), KeyModifiers::ALT),
            (b"\x1bf", ClientKeyCode::Char('f'), KeyModifiers::ALT),
            (b"\x1b\r", ClientKeyCode::Enter, KeyModifiers::ALT),
            (b"\x1b\x7f", ClientKeyCode::Backspace, KeyModifiers::ALT),
            (b"\x1b[13;2u", ClientKeyCode::Enter, KeyModifiers::SHIFT),
        ] {
            let (mut gate, mut shell, mut registry, messages) =
                fixture(Some(InputIntentState::Command));
            let now = Instant::now();
            let mut framer = crate::raw_input::RawInputByteFramer::for_host_input();
            framer.set_host_escape_disambiguation_active(true);
            let mut chunks = framer.push(bytes);
            chunks.extend(framer.flush_timeout());
            // Confirmed host input first waits for a possible mouse tail; the
            // following idle expiry releases a bare Escape without that tail.
            if framer.has_pending_input() {
                chunks.extend(framer.flush_timeout());
            }
            for chunk in chunks {
                let events = crate::raw_input::parse_raw_input_bytes_sync(&chunk);
                gate.enqueue(&mut shell, &registry, chunk, events, None, false, now)
                    .unwrap();
            }
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
            assert!(matches!(
                messages.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            applied(&mut gate);
            while let Some(outcome) = dispatch(&mut gate, &mut shell, &mut registry, now) {
                assert!(outcome.actions.is_empty());
                send(&shell, &mut registry, outcome);
            }
            let forwarded: Vec<_> = messages
                .try_iter()
                .flat_map(|message| match message {
                    ClientMessage::ClientShellPaneInput { pane_id, events } => {
                        assert_eq!(pane_id, "pane_1");
                        events
                    }
                    message => panic!("unexpected input message: {message:?}"),
                })
                .collect();
            assert!(
                matches!(forwarded.as_slice(), [
                ClientPaneInputEvent::Key { code: actual, modifiers: actual_modifiers, kind: ClientKeyKind::Press, .. }
            ] if actual == &code && *actual_modifiers == modifiers.bits()),
                "binding {bytes:?}: {forwarded:?}"
            );
        }
    }

    #[test]
    fn presentation_input_fence_cancels_new_presses_but_keeps_balancing_releases() {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"\x1b[120;1u", now);
        applied(&mut gate);
        let press = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, press);
        enqueue(&mut gate, &mut shell, &registry, b"y", now);
        let old = gate.authorization.as_ref().unwrap().clone();
        registry.freeze_input();
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        gate.complete(Completion {
            authorization: old,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        assert!(shell
            .endpoint_error
            .as_deref()
            .is_some_and(|message| message.starts_with("IME_INPUT_CANCELLED:")));
        let release = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![RawInputEvent::Key(
                    crate::input::TerminalKey::new(KeyCode::Char('x'), KeyModifiers::NONE)
                        .with_kind(KeyEventKind::Release),
                )],
                None,
                false,
                now,
            )
            .unwrap();
        send(&shell, &mut registry, release);
        registry.unfreeze_input();
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        applied(&mut gate);
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        let forwarded: Vec<_> = messages
            .try_iter()
            .flat_map(|message| match message {
                ClientMessage::ClientShellPaneInput { events, .. } => events,
                message => panic!("unexpected input message: {message:?}"),
            })
            .collect();
        assert!(matches!(
            forwarded.as_slice(),
            [
                ClientPaneInputEvent::Key {
                    code: crate::protocol::ClientKeyCode::Char('x'),
                    kind: crate::protocol::ClientKeyKind::Press,
                    ..
                },
                ClientPaneInputEvent::Key {
                    code: crate::protocol::ClientKeyCode::Char('x'),
                    kind: crate::protocol::ClientKeyKind::Release,
                    ..
                },
            ]
        ));
    }

    #[test]
    fn blocked_ack_prevents_local_resize_action() {
        let (mut gate, mut shell, mut registry, _) = fixture(Some(InputIntentState::Text));
        shell.mode = ClientShellMode::Resize;
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"h", now);
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        applied(&mut gate);
        let outcome = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert!(
            matches!(outcome.actions.as_slice(), [ClientShellAction::Endpoint { request, .. }] if matches!(&request.method, crate::api::schema::Method::PaneResize(_)))
        );
    }

    #[test]
    fn same_batch_prefix_resize_cursor_waits_for_each_changed_arbitration() {
        for prefix in [0x02, 0x18] {
            let (mut gate, mut shell, mut registry, _) = fixture(Some(InputIntentState::Text));
            shell
                .config
                .keybinds
                .prefix
                .push((KeyCode::Char('x'), KeyModifiers::CONTROL));
            let now = Instant::now();
            enqueue(&mut gate, &mut shell, &registry, &[prefix, b'r', b'h'], now);
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
            applied(&mut gate);
            let first = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
            assert!(first.actions.is_empty() && first.routed_requests.is_empty());
            assert_eq!(shell.mode, ClientShellMode::Prefix);
            assert_eq!(gate.batches.front().unwrap().cursor, 1);
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
            let prefix_key = gate.authorization.as_ref().unwrap().clone();
            applied(&mut gate);
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now)
                .unwrap()
                .actions
                .is_empty());
            assert_eq!(shell.mode, ClientShellMode::Resize);
            assert_eq!(gate.batches.front().unwrap().cursor, 2);
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
            gate.complete(Completion {
                authorization: prefix_key,
                result: Ok(AckScope::Applied),
            })
            .unwrap();
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
            applied(&mut gate);
            assert_eq!(
                dispatch(&mut gate, &mut shell, &mut registry, now)
                    .unwrap()
                    .actions
                    .len(),
                1
            );
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
            assert!(shell.take_input_source_changes().is_empty());
        }
    }

    #[test]
    fn double_prefix_stages_only_mode_then_waits_before_forwarding_trigger() {
        for (prefix, character) in [(0x02, 'b'), (0x18, 'x')] {
            let (mut gate, mut shell, mut registry, _) = fixture(Some(InputIntentState::Command));
            shell
                .config
                .keybinds
                .prefix
                .push((KeyCode::Char('x'), KeyModifiers::CONTROL));
            shell.mode = ClientShellMode::Prefix;
            let now = Instant::now();
            enqueue(&mut gate, &mut shell, &registry, &[prefix, b'y'], now);
            applied(&mut gate);
            let transition = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
            assert!(
                transition.actions.is_empty()
                    && transition.requests.is_empty()
                    && transition.routed_requests.is_empty()
            );
            assert_eq!(shell.mode, ClientShellMode::Terminal);
            assert_eq!(gate.batches.front().unwrap().cursor, 0);
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
            applied(&mut gate);
            let forwarded = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
            assert!(
                matches!(forwarded.routed_requests.as_slice(), [(_, ClientMessage::ClientShellPaneInput { pane_id, events })] if pane_id == "pane_1" && matches!(events.as_slice(), [ClientPaneInputEvent::Key { code: crate::protocol::ClientKeyCode::Char(actual), .. }] if *actual == character))
            );
            assert_eq!(shell.mode, ClientShellMode::Terminal);
            assert_eq!(gate.batches.front().unwrap().cursor, 1);
            let second = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
            assert!(
                matches!(second.routed_requests.as_slice(), [(_, ClientMessage::ClientShellPaneInput { events, .. })] if matches!(events.as_slice(), [ClientPaneInputEvent::Key { code: crate::protocol::ClientKeyCode::Char('y'), .. }]))
            );
            assert!(gate.batches.is_empty());
        }
    }

    #[test]
    fn no_reporter_and_unknown_intent_wait_for_applied_release_not_inactive() {
        for state in [None, Some(InputIntentState::Unknown)] {
            let (mut gate, mut shell, mut registry, _) = fixture(state);
            let now = Instant::now();
            enqueue(&mut gate, &mut shell, &registry, b"x", now);
            assert!(gate.desired.is_none());
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
            gate.complete(Completion {
                authorization: gate.authorization.as_ref().unwrap().clone(),
                result: Ok(AckScope::Inactive),
            })
            .unwrap();
            assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
            applied(&mut gate);
            assert_eq!(
                dispatch(&mut gate, &mut shell, &mut registry, now)
                    .unwrap()
                    .routed_requests
                    .len(),
                1
            );
        }
    }

    #[test]
    fn inactive_and_later_batch_do_not_restart_original_five_second_deadline() {
        let (mut gate, mut shell, mut registry, _) = fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"x", now);
        gate.complete(Completion {
            authorization: gate.authorization.as_ref().unwrap().clone(),
            result: Ok(AckScope::Inactive),
        })
        .unwrap();
        enqueue(
            &mut gate,
            &mut shell,
            &registry,
            b"y",
            now + Duration::from_secs(3),
        );
        assert_eq!(gate.deadline(), Some(now + INPUT_WAIT));
        assert_eq!(
            gate.pending_plan.as_ref().unwrap().deadline,
            now + INPUT_WAIT
        );
        assert!(matches!(
            gate.dispatch(
                &mut shell,
                &mut registry,
                false,
                None,
                false,
                now + INPUT_WAIT
            ),
            Err(ClientError::ImeAckTimeout)
        ));
    }

    #[test]
    fn waiting_batch_and_byte_limits_reject_without_replacing_retained_input() {
        let (mut gate, mut shell, registry, _) = fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        for _ in 0..INPUT_BATCH_LIMIT {
            enqueue(&mut gate, &mut shell, &registry, b"x", now);
        }
        let extra = gate.enqueue(
            &mut shell,
            &registry,
            b"y".to_vec(),
            crate::raw_input::parse_raw_input_bytes_sync(b"y"),
            None,
            false,
            now,
        );
        assert!(matches!(extra, Err(ClientError::ImeInputOverflow)));
        assert_eq!(gate.batches.len(), INPUT_BATCH_LIMIT);
        assert!(gate.batches.iter().all(|batch| batch.raw == b"x"));
        let (mut gate, mut shell, registry, _) = fixture(Some(InputIntentState::Command));
        let key = || {
            RawInputEvent::Key(crate::input::TerminalKey::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            ))
        };
        gate.enqueue(
            &mut shell,
            &registry,
            vec![b'x'; INPUT_BYTE_LIMIT],
            vec![key()],
            None,
            false,
            now,
        )
        .unwrap();
        let extra = gate.enqueue(
            &mut shell,
            &registry,
            vec![b'y'],
            vec![key()],
            None,
            false,
            now,
        );
        assert!(matches!(extra, Err(ClientError::ImeInputOverflow)));
        assert_eq!(gate.bytes, INPUT_BYTE_LIMIT);
        assert_eq!(gate.batches.len(), 1);
    }

    #[test]
    fn focus_loss_cancels_unsent_press_without_synthetic_release_or_old_ack_authorization() {
        let (mut gate, mut shell, registry, _) = fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"x", now);
        let old = gate.authorization.as_ref().unwrap().clone();
        let outcome = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![RawInputEvent::OuterFocusLost],
                None,
                false,
                now,
            )
            .unwrap();
        assert!(gate.batches.is_empty() && outcome.routed_requests.is_empty());
        assert!(outcome.repaint);
        assert!(shell
            .endpoint_error
            .as_deref()
            .is_some_and(|message| message.starts_with("IME_INPUT_CANCELLED: 1 ")));
        assert!(gate.desired.is_none());
        gate.complete(Completion {
            authorization: old,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(!gate.applied);
    }

    #[test]
    fn sent_key_release_routes_original_endpoint_after_endpoint_and_focus_change() {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"\x1b[120;1u", now);
        applied(&mut gate);
        let press = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, press);
        let profile = SavedSshEndpoint {
            id: ProfileId::parse("0123456789abcdef0123456789abcdef").unwrap(),
            label: "remote".into(),
            target: "dev@example".into(),
            session: "fixture".into(),
            enabled: true,
        };
        let remote = ClientEndpointId::Ssh(profile.id.clone());
        shell.set_endpoint_catalog(&[profile]);
        let mut snapshot = super::super::tests::snapshot();
        snapshot.boot_id = "remote-boot".into();
        snapshot.panes[0].terminal_id = Some("remote-terminal".into());
        snapshot.input_intents = Some(Arc::from([]));
        shell.set_endpoint_snapshot_for_generation(&remote, 7, Box::new(snapshot));
        shell.set_endpoint_status(&remote, ClientEndpointStatus::Online);
        let (remote_sender, remote_messages) = mpsc::channel();
        registry.insert(
            remote.clone(),
            RecordingTransport(remote_sender),
            7,
            negotiation(),
            true,
        );
        assert!(shell.activate_endpoint_projection(&remote));
        assert!(registry.set_active(&remote));
        let release = RawInputEvent::Key(
            crate::input::TerminalKey::new(KeyCode::Char('x'), KeyModifiers::NONE)
                .with_kind(KeyEventKind::Release),
        );
        let outcome = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![release, RawInputEvent::OuterFocusLost],
                None,
                false,
                now,
            )
            .unwrap();
        assert!(!gate.applied);
        assert!(
            matches!(outcome.routed_requests.as_slice(), [(route, ClientMessage::ClientShellPaneInput { pane_id, events })] if route.endpoint_id == ClientEndpointId::Local && route.connection_generation == 1 && route.boot_id.as_ref() == "boot-1" && pane_id == "pane_1" && matches!(events.as_slice(), [ClientPaneInputEvent::Key { kind: crate::protocol::ClientKeyKind::Release, .. }]))
        );
        send(&shell, &mut registry, outcome);
        let messages: Vec<_> = messages.try_iter().collect();
        assert!(
            matches!(&messages[1], ClientMessage::ClientShellPaneInput { pane_id, events } if pane_id == "pane_1" && matches!(events.as_slice(), [ClientPaneInputEvent::Key { kind: crate::protocol::ClientKeyKind::Release, .. }]))
        );
        assert!(!remote_messages.try_iter().any(|message| matches!(
            message,
            ClientMessage::ClientShellPaneInput { .. }
                | ClientMessage::ClientShellPopupInput { .. }
        )));
    }

    #[test]
    fn sent_mouse_up_is_not_ack_gated_and_reuses_original_gesture_route() {
        let (mut gate, mut shell, mut registry, _) = fixture(Some(InputIntentState::Command));
        let mut surface = super::super::tests::surface();
        surface.panes[0].mouse_reporting = true;
        shell.set_pane_surface(surface);
        shell.compose(100, 28).unwrap();
        let hit = shell.hits.panes[0].clone();
        let mouse = |kind| {
            RawInputEvent::Mouse(MouseEvent {
                kind,
                column: hit.inner_rect.x,
                row: hit.inner_rect.y,
                modifiers: KeyModifiers::NONE,
            })
        };
        let now = Instant::now();
        gate.enqueue(
            &mut shell,
            &registry,
            vec![1],
            vec![mouse(MouseEventKind::Down(MouseButton::Left))],
            None,
            false,
            now,
        )
        .unwrap();
        applied(&mut gate);
        let press = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert_eq!(press.routed_requests.len(), 1);
        assert!(shell.pane_mouse_gesture.is_some());
        gate.applied = false;
        let up = gate
            .enqueue(
                &mut shell,
                &registry,
                vec![2],
                vec![mouse(MouseEventKind::Up(MouseButton::Left))],
                None,
                false,
                now,
            )
            .unwrap();
        assert!(
            matches!(up.routed_requests.as_slice(), [(route, ClientMessage::ClientShellPaneInput { pane_id, events })] if route.connection_generation == 1 && pane_id == "pane_1" && matches!(events.as_slice(), [ClientPaneInputEvent::Mouse { kind: crate::protocol::ClientMouseKind::Up(_), .. }]))
        );
        assert!(shell.pane_mouse_gesture.is_none());
    }

    #[test]
    fn new_boot_cancels_waiting_input_and_old_generation_cannot_receive_release() {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"x", now);
        let old_route = gate.binding.as_ref().unwrap().route.clone();
        let old_key = gate.authorization.as_ref().unwrap().clone();
        let mut snapshot = shell.snapshot.as_ref().unwrap().as_ref().clone();
        snapshot.boot_id = "boot-2".into();
        snapshot.input_intents = Some(Arc::from([]));
        shell.set_endpoint_snapshot_for_generation(&ClientEndpointId::Local, 2, Box::new(snapshot));
        let (sender, replacement_messages) = mpsc::channel();
        registry.insert(
            ClientEndpointId::Local,
            RecordingTransport(sender),
            2,
            negotiation(),
            true,
        );
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        assert!(gate.batches.is_empty());
        gate.complete(Completion {
            authorization: old_key,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(!gate.applied);
        let request = ClientMessage::ClientShellPaneInput {
            pane_id: "pane_1".into(),
            events: vec![ClientPaneInputEvent::TextCommit("never-replayed".into())],
        };
        crate::client::shell_runtime::send_bound_input(
            Some(&shell),
            &mut registry,
            &old_route,
            &request,
        );
        assert!(messages.try_iter().next().is_none());
        assert!(matches!(
            replacement_messages.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
    }

    #[test]
    fn buffered_unsent_press_keeps_its_real_release_in_order_without_sampling_release() {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"\x1b[120;1u", now);
        let key = gate.authorization.as_ref().unwrap().clone();
        let release = RawInputEvent::Key(
            crate::input::TerminalKey::new(KeyCode::Char('x'), KeyModifiers::NONE)
                .with_kind(KeyEventKind::Release),
        );
        let immediate = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![release],
                None,
                false,
                now,
            )
            .unwrap();
        assert!(immediate.routed_requests.is_empty());
        assert_eq!(gate.authorization.as_deref(), Some(key.as_ref()));
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        applied(&mut gate);
        let press = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, press);
        gate.applied = false;
        let release = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, release);
        let events: Vec<_> = messages
            .try_iter()
            .flat_map(|message| match message {
                ClientMessage::ClientShellPaneInput { events, .. } => events,
                _ => Vec::new(),
            })
            .collect();
        assert!(matches!(
            events.as_slice(),
            [
                ClientPaneInputEvent::Key {
                    kind: crate::protocol::ClientKeyKind::Press,
                    ..
                },
                ClientPaneInputEvent::Key {
                    kind: crate::protocol::ClientKeyKind::Release,
                    ..
                },
            ]
        ));
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
    }

    #[test]
    fn focus_cancellation_flushes_queued_release_only_for_a_press_already_sent() {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        let press = RawInputEvent::Key(crate::input::TerminalKey::new(
            KeyCode::Char('x'),
            KeyModifiers::NONE,
        ));
        let release = RawInputEvent::Key(
            crate::input::TerminalKey::new(KeyCode::Char('x'), KeyModifiers::NONE)
                .with_kind(KeyEventKind::Release),
        );
        gate.enqueue(
            &mut shell,
            &registry,
            b"x".to_vec(),
            vec![press, release],
            None,
            false,
            now,
        )
        .unwrap();
        applied(&mut gate);
        let press = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, press);
        let immediate = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![RawInputEvent::OuterFocusLost],
                None,
                false,
                now,
            )
            .unwrap();
        send(&shell, &mut registry, immediate);
        let events: Vec<_> = messages
            .try_iter()
            .flat_map(|message| match message {
                ClientMessage::ClientShellPaneInput { events, .. } => events,
                _ => Vec::new(),
            })
            .collect();
        assert!(matches!(
            events.as_slice(),
            [
                ClientPaneInputEvent::Key {
                    kind: crate::protocol::ClientKeyKind::Press,
                    ..
                },
                ClientPaneInputEvent::Key {
                    kind: crate::protocol::ClientKeyKind::Release,
                    ..
                },
            ]
        ));
    }

    #[test]
    fn prefix_exit_command_waits_for_new_arbitration_without_rerunning_trigger() {
        let (mut gate, mut shell, mut registry, _) = fixture(Some(InputIntentState::Text));
        shell.config.prompt_new_tab_name = false;
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"\x02c", now);
        applied(&mut gate);
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now)
            .unwrap()
            .actions
            .is_empty());
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        applied(&mut gate);
        let trigger = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert!(
            trigger.actions.is_empty()
                && trigger.requests.is_empty()
                && trigger.routed_requests.is_empty()
        );
        assert_eq!(shell.mode, ClientShellMode::Terminal);
        assert_eq!(gate.batches.front().unwrap().cursor, 2);
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        assert_eq!(
            gate.desired.as_ref().unwrap().identity.terminal_target,
            LeaseTarget::LocalUi
        );
        applied(&mut gate);
        let command = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert!(
            matches!(command.actions.as_slice(), [ClientShellAction::Endpoint { request, .. }] if matches!(request.method, crate::api::schema::Method::TabCreate(_)))
        );
        assert!(gate.batches.is_empty());
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
    }

    #[test]
    fn unrelated_balancing_release_does_not_drop_a_deferred_prefix_command() {
        let (mut gate, mut shell, mut registry, _messages) = fixture(Some(InputIntentState::Text));
        shell.config.prompt_new_tab_name = false;
        let now = Instant::now();
        enqueue(&mut gate, &mut shell, &registry, b"\x1b[120;1u", now);
        applied(&mut gate);
        let press = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, press);
        enqueue(&mut gate, &mut shell, &registry, b"\x02c", now);
        applied(&mut gate);
        dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        applied(&mut gate);
        dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        let key = gate.authorization.as_ref().unwrap().clone();
        let release = RawInputEvent::Key(
            crate::input::TerminalKey::new(KeyCode::Char('x'), KeyModifiers::NONE)
                .with_kind(KeyEventKind::Release),
        );
        let immediate = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![release],
                None,
                false,
                now,
            )
            .unwrap();
        assert_eq!(immediate.routed_requests.len(), 1);
        assert_eq!(gate.authorization.as_deref(), Some(key.as_ref()));
        assert!(gate.batches.front().unwrap().pending_outcome.is_some());
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        applied(&mut gate);
        let command = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert!(
            matches!(command.actions.as_slice(), [ClientShellAction::Endpoint { request, .. }] if matches!(request.method, crate::api::schema::Method::TabCreate(_)))
        );
    }

    fn two_panes(shell: &mut ClientShellState, focused_second: bool) {
        let mut snapshot = shell.snapshot.as_ref().unwrap().as_ref().clone();
        snapshot.revision += 1;
        snapshot.panes[0].focused = !focused_second;
        if snapshot.panes.len() == 1 {
            let mut pane = snapshot.panes[0].clone();
            pane.pane_id = "pane_2".into();
            pane.terminal_id = Some("terminal-2".into());
            snapshot.panes.push(pane);
        }
        snapshot.panes[1].focused = focused_second;
        snapshot.focused_pane_id = Some(if focused_second { "pane_2" } else { "pane_1" }.into());
        let mut second = reporter(InputIntentState::Command);
        second.terminal_id = "terminal-2".into();
        second.sessions[0].session = "reporter-b".into();
        snapshot.input_intents = Some(vec![reporter(InputIntentState::Command), second].into());
        let revision = snapshot.revision;
        shell.set_endpoint_snapshot_for_generation(&ClientEndpointId::Local, 1, Box::new(snapshot));
        let mut surface = super::super::tests::surface();
        surface.projection_revision = revision;
        surface.surface_revision = revision;
        surface.frame = FrameData::from_ratatui_buffer_with_hyperlinks(
            &Buffer::with_lines(["LIVEPANE", "PANELIVE"]),
            None,
            &[],
        );
        surface.panes[0].focused = !focused_second;
        surface.panes[0].mouse_reporting = true;
        let mut pane = surface.panes[0].clone();
        pane.pane_id = "pane_2".into();
        pane.rect.x = 4;
        pane.inner_rect.x = 4;
        pane.focused = focused_second;
        surface.panes.push(pane);
        shell.set_pane_surface(surface);
        shell.compose(100, 28).unwrap();
    }

    fn popup_over_second_pane(
        shell: &mut ClientShellState,
        state: Option<InputIntentState>,
    ) -> (u16, u16) {
        two_panes(shell, false);
        let mut snapshot = shell.snapshot.as_ref().unwrap().as_ref().clone();
        snapshot.revision += 1;
        let mut roster = snapshot.input_intents.as_ref().unwrap().to_vec();
        if let Some(state) = state {
            let mut popup = reporter(state);
            popup.terminal_id = "terminal-popup".into();
            popup.sessions[0].session = "reporter-popup".into();
            roster.push(popup);
        }
        snapshot.input_intents = Some(roster.into());
        let revision = snapshot.revision;
        shell.set_endpoint_snapshot_for_generation(&ClientEndpointId::Local, 1, Box::new(snapshot));
        let area = shell.layout(100, 28).pane_surface;
        let split = area.width / 2;
        let mut surface = shell.pane_surface.as_ref().unwrap().clone();
        surface.projection_revision = revision;
        surface.surface_revision = revision;
        surface.frame = FrameData::from_ratatui_buffer_with_hyperlinks(
            &Buffer::empty(Rect::new(0, 0, area.width, area.height)),
            None,
            &[],
        );
        for (index, pane) in surface.panes.iter_mut().enumerate() {
            pane.rect.x = if index == 0 { 0 } else { split };
            pane.rect.y = 0;
            pane.rect.width = if index == 0 {
                split
            } else {
                area.width - split
            };
            pane.rect.height = area.height;
            pane.inner_rect = pane.rect;
        }
        surface.popup = Some(Box::new(crate::protocol::ClientShellPopupSurface {
            terminal_id: "terminal-popup".into(),
            title: "popup".into(),
            width: Some(crate::protocol::ClientShellPopupSize::Cells(12)),
            height: Some(crate::protocol::ClientShellPopupSize::Cells(5)),
            frame: FrameData::from_ratatui_buffer_with_hyperlinks(
                &Buffer::with_lines(["popup-live", "", ""]),
                None,
                &[],
            ),
            mouse_reporting: true,
            sgr_pixel_mouse: false,
            pixel_width: 0,
            pixel_height: 0,
        }));
        shell.set_pane_surface(surface);
        shell.compose(100, 28).unwrap();
        let pane = shell
            .hits
            .panes
            .iter()
            .find(|hit| hit.pane_id == "pane_2")
            .unwrap();
        let popup = shell.hits.popup.as_ref().unwrap();
        let point = (
            pane.inner_rect.x.max(popup.inner_rect.x),
            pane.inner_rect.y.max(popup.inner_rect.y),
        );
        assert!(contains(pane.inner_rect, point) && contains(popup.inner_rect, point));
        assert_eq!(shell.focused_pane_id().as_deref(), Some("pane_1"));
        point
    }

    fn popup_mouse_uses_its_own_intent(state: Option<InputIntentState>) {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        let (column, row) = popup_over_second_pane(&mut shell, state);
        let mouse = |kind| {
            RawInputEvent::Mouse(MouseEvent {
                kind,
                column,
                row,
                modifiers: KeyModifiers::NONE,
            })
        };
        let now = Instant::now();
        gate.enqueue(
            &mut shell,
            &registry,
            Vec::new(),
            vec![
                mouse(MouseEventKind::Down(MouseButton::Left)),
                mouse(MouseEventKind::Up(MouseButton::Left)),
                mouse(MouseEventKind::ScrollDown),
            ],
            None,
            false,
            now,
        )
        .unwrap();
        assert_eq!(
            gate.authorization
                .as_ref()
                .unwrap()
                .identity
                .terminal_target,
            LeaseTarget::Terminal(Arc::from("terminal-popup"))
        );
        match state {
            Some(state) => {
                let desired = gate.desired.as_ref().unwrap();
                assert_eq!(desired.state, state);
                assert_eq!(
                    desired.identity.reporter_session.as_deref(),
                    Some("reporter-popup")
                );
            }
            None => assert!(gate.desired.is_none()),
        }
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        gate.complete(Completion {
            authorization: gate.authorization.as_ref().unwrap().clone(),
            result: Ok(AckScope::Inactive),
        })
        .unwrap();
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        applied(&mut gate);
        for _ in 0..3 {
            let outcome = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
            assert!(outcome.actions.is_empty());
            assert!(
                matches!(outcome.routed_requests.as_slice(), [(route, ClientMessage::ClientShellPopupInput { terminal_id, .. })] if route.terminal_target.as_deref() == Some("terminal-popup") && terminal_id == "terminal-popup")
            );
            send(&shell, &mut registry, outcome);
        }
        let messages: Vec<_> = messages.try_iter().collect();
        assert!(matches!(messages.as_slice(), [
            ClientMessage::ClientShellPopupInput { terminal_id: first, events: down },
            ClientMessage::ClientShellPopupInput { terminal_id: second, events: up },
            ClientMessage::ClientShellPopupInput { terminal_id: third, events: scroll },
        ] if first == "terminal-popup" && second == "terminal-popup" && third == "terminal-popup"
            && matches!(down.as_slice(), [ClientPaneInputEvent::Mouse { kind: crate::protocol::ClientMouseKind::Down(_), .. }])
            && matches!(up.as_slice(), [ClientPaneInputEvent::Mouse { kind: crate::protocol::ClientMouseKind::Up(_), .. }])
            && matches!(scroll.as_slice(), [ClientPaneInputEvent::Mouse { kind: crate::protocol::ClientMouseKind::ScrollDown, .. }])));
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
    }

    #[test]
    fn text_popup_covering_an_unfocused_pane_keeps_mouse_under_text_authorization() {
        popup_mouse_uses_its_own_intent(Some(InputIntentState::Text));
    }

    #[test]
    fn unreported_popup_covering_an_unfocused_pane_waits_for_release_not_local_command() {
        popup_mouse_uses_its_own_intent(None);
    }

    #[test]
    fn mouse_focus_commit_flushes_sent_key_releases_from_continuation_and_later_batches_to_original_pane(
    ) {
        let (mut gate, mut shell, mut registry, messages) =
            fixture(Some(InputIntentState::Command));
        two_panes(&mut shell, false);
        let hit = shell
            .hits
            .panes
            .iter()
            .find(|hit| hit.pane_id == "pane_2")
            .unwrap()
            .clone();
        let mouse = |kind| {
            RawInputEvent::Mouse(MouseEvent {
                kind,
                column: hit.inner_rect.x,
                row: hit.inner_rect.y,
                modifiers: KeyModifiers::NONE,
            })
        };
        let key = |character, kind| {
            RawInputEvent::Key(
                crate::input::TerminalKey::new(KeyCode::Char(character), KeyModifiers::NONE)
                    .with_kind(kind),
            )
        };
        let now = Instant::now();
        gate.enqueue(
            &mut shell,
            &registry,
            b"xz".to_vec(),
            vec![key('x', KeyEventKind::Press), key('z', KeyEventKind::Press)],
            None,
            false,
            now,
        )
        .unwrap();
        gate.enqueue(
            &mut shell,
            &registry,
            b"y".to_vec(),
            vec![
                mouse(MouseEventKind::Down(MouseButton::Left)),
                key('x', KeyEventKind::Release),
                key('y', KeyEventKind::Press),
                key('y', KeyEventKind::Release),
            ],
            None,
            false,
            now,
        )
        .unwrap();
        let immediate = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![
                    key('z', KeyEventKind::Release),
                    mouse(MouseEventKind::Up(MouseButton::Left)),
                ],
                None,
                false,
                now,
            )
            .unwrap();
        assert!(immediate.routed_requests.is_empty());
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        applied(&mut gate);
        for _ in 0..2 {
            let press = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
            send(&shell, &mut registry, press);
        }
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        applied(&mut gate);
        let focus = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert!(
            matches!(focus.actions.as_slice(), [ClientShellAction::Endpoint { request, .. }] if matches!(&request.method, crate::api::schema::Method::PaneFocus(target) if target.pane_id == "pane_2"))
        );
        two_panes(&mut shell, true);
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        assert!(!gate.applied);
        let releases = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert_eq!(releases.routed_requests.len(), 2);
        for (route, request) in &releases.routed_requests {
            assert_eq!(route.endpoint_id, ClientEndpointId::Local);
            assert_eq!(route.connection_generation, 1);
            assert_eq!(route.boot_id.as_ref(), "boot-1");
            assert_eq!(route.terminal_target.as_deref(), Some("terminal-1"));
            assert!(
                matches!(request, ClientMessage::ClientShellPaneInput { pane_id, events } if pane_id == "pane_1" && matches!(events.as_slice(), [ClientPaneInputEvent::Key { kind: crate::protocol::ClientKeyKind::Release, .. }]))
            );
        }
        send(&shell, &mut registry, releases);
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        applied(&mut gate);
        let down = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, down);
        gate.applied = false;
        let up = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        send(&shell, &mut registry, up);
        let messages: Vec<_> = messages.try_iter().collect();
        assert!(matches!(messages.as_slice(), [
            ClientMessage::ClientShellPaneInput { pane_id: first, events: press_x },
            ClientMessage::ClientShellPaneInput { pane_id: second, events: press_z },
            ClientMessage::ClientShellPaneInput { pane_id: third, events: release_x },
            ClientMessage::ClientShellPaneInput { pane_id: fourth, events: release_z },
            ClientMessage::ClientShellPaneInput { pane_id: fifth, events: down },
            ClientMessage::ClientShellPaneInput { pane_id: sixth, events: up },
        ] if first == "pane_1" && second == "pane_1" && third == "pane_1" && fourth == "pane_1"
            && fifth == "pane_2" && sixth == "pane_2"
            && matches!(press_x.as_slice(), [ClientPaneInputEvent::Key { code: crate::protocol::ClientKeyCode::Char('x'), kind: crate::protocol::ClientKeyKind::Press, .. }])
            && matches!(press_z.as_slice(), [ClientPaneInputEvent::Key { code: crate::protocol::ClientKeyCode::Char('z'), kind: crate::protocol::ClientKeyKind::Press, .. }])
            && matches!(release_x.as_slice(), [ClientPaneInputEvent::Key { code: crate::protocol::ClientKeyCode::Char('x'), kind: crate::protocol::ClientKeyKind::Release, .. }])
            && matches!(release_z.as_slice(), [ClientPaneInputEvent::Key { code: crate::protocol::ClientKeyCode::Char('z'), kind: crate::protocol::ClientKeyKind::Release, .. }])
            && matches!(down.as_slice(), [ClientPaneInputEvent::Mouse { kind: crate::protocol::ClientMouseKind::Down(_), .. }])
            && matches!(up.as_slice(), [ClientPaneInputEvent::Mouse { kind: crate::protocol::ClientMouseKind::Up(_), .. }])));
        let lost = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![RawInputEvent::OuterFocusLost],
                None,
                false,
                now,
            )
            .unwrap();
        assert!(lost.routed_requests.is_empty());
    }

    #[test]
    fn pane_click_waits_for_actual_focus_then_ack_and_never_replays_old_target_keys() {
        let (mut gate, mut shell, mut registry, _) = fixture(Some(InputIntentState::Command));
        two_panes(&mut shell, false);
        let hit = shell
            .hits
            .panes
            .iter()
            .find(|hit| hit.pane_id == "pane_2")
            .unwrap()
            .clone();
        let mouse = |kind| {
            RawInputEvent::Mouse(MouseEvent {
                kind,
                column: hit.inner_rect.x,
                row: hit.inner_rect.y,
                modifiers: KeyModifiers::NONE,
            })
        };
        let now = Instant::now();
        gate.enqueue(
            &mut shell,
            &registry,
            b"x".to_vec(),
            vec![
                mouse(MouseEventKind::Down(MouseButton::Left)),
                RawInputEvent::Key(crate::input::TerminalKey::new(
                    KeyCode::Char('x'),
                    KeyModifiers::NONE,
                )),
                mouse(MouseEventKind::Up(MouseButton::Left)),
            ],
            None,
            false,
            now,
        )
        .unwrap();
        applied(&mut gate);
        let focus = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert!(
            matches!(focus.actions.as_slice(), [ClientShellAction::Endpoint { request, .. }] if matches!(&request.method, crate::api::schema::Method::PaneFocus(target) if target.pane_id == "pane_2"))
        );
        assert!(focus.routed_requests.is_empty());
        assert_eq!(gate.batches.front().unwrap().cursor, 0);
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        let old = gate.authorization.as_ref().unwrap().clone();
        two_panes(&mut shell, true);
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        assert!(shell
            .endpoint_error
            .as_deref()
            .is_some_and(|message| message.starts_with("IME_INPUT_CANCELLED:")));
        gate.complete(Completion {
            authorization: old,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        applied(&mut gate);
        let down = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert!(down.actions.is_empty());
        assert!(
            matches!(down.routed_requests.as_slice(), [(_, ClientMessage::ClientShellPaneInput { pane_id, events })] if pane_id == "pane_2" && matches!(events.as_slice(), [ClientPaneInputEvent::Mouse { kind: crate::protocol::ClientMouseKind::Down(_), .. }]))
        );
        gate.applied = false;
        let up = dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert!(
            matches!(up.routed_requests.as_slice(), [(_, ClientMessage::ClientShellPaneInput { pane_id, events })] if pane_id == "pane_2" && matches!(events.as_slice(), [ClientPaneInputEvent::Mouse { kind: crate::protocol::ClientMouseKind::Up(_), .. }]))
        );
        assert!(gate.batches.is_empty());
    }

    #[test]
    fn old_gesture_drag_cannot_follow_target_change_but_up_returns_to_original_pane() {
        let (mut gate, mut shell, mut registry, _) = fixture(Some(InputIntentState::Command));
        two_panes(&mut shell, false);
        let hit = shell
            .hits
            .panes
            .iter()
            .find(|hit| hit.pane_id == "pane_1")
            .unwrap()
            .clone();
        let mouse = |kind| {
            RawInputEvent::Mouse(MouseEvent {
                kind,
                column: hit.inner_rect.x,
                row: hit.inner_rect.y,
                modifiers: KeyModifiers::NONE,
            })
        };
        let now = Instant::now();
        gate.enqueue(
            &mut shell,
            &registry,
            Vec::new(),
            vec![mouse(MouseEventKind::Down(MouseButton::Left))],
            None,
            false,
            now,
        )
        .unwrap();
        applied(&mut gate);
        assert_eq!(
            dispatch(&mut gate, &mut shell, &mut registry, now)
                .unwrap()
                .routed_requests
                .len(),
            1
        );
        two_panes(&mut shell, true);
        gate.enqueue(
            &mut shell,
            &registry,
            Vec::new(),
            vec![mouse(MouseEventKind::Drag(MouseButton::Left))],
            None,
            false,
            now,
        )
        .unwrap();
        applied(&mut gate);
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now)
            .unwrap()
            .routed_requests
            .is_empty());
        gate.applied = false;
        let up = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![mouse(MouseEventKind::Up(MouseButton::Left))],
                None,
                false,
                now,
            )
            .unwrap();
        assert!(
            matches!(up.routed_requests.as_slice(), [(_, ClientMessage::ClientShellPaneInput { pane_id, events })] if pane_id == "pane_1" && matches!(events.as_slice(), [ClientPaneInputEvent::Mouse { kind: crate::protocol::ClientMouseKind::Up(_), .. }]))
        );
    }

    #[test]
    fn mouse_fallback_requires_fresh_ack_and_keeps_original_deadline_and_tuple() {
        let (mut gate, mut shell, mut registry, _) = fixture(Some(InputIntentState::Command));
        let mut surface = super::super::tests::surface();
        surface.panes[0].mouse_reporting = true;
        shell.set_pane_surface(surface);
        shell.compose(100, 28).unwrap();
        let hit = shell.hits.panes[0].clone();
        let now = Instant::now();
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        applied(&mut gate);
        let origin = ClientReplayOrigin {
            route: gate.binding.as_ref().unwrap().route.clone(),
            authorization: gate.authorization.as_ref().unwrap().clone(),
            deadline: now + INPUT_WAIT,
        };
        let event = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: hit.inner_rect.x,
            row: hit.inner_rect.y,
            modifiers: KeyModifiers::NONE,
        };
        gate.enqueue_replay(
            &mut shell,
            &registry,
            ClientMouseReplay {
                events: vec![event],
                origin: Some(origin.clone()),
            },
            false,
            now + Duration::from_secs(2),
        )
        .unwrap();
        assert_eq!(gate.deadline(), Some(origin.deadline));
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        gate.complete(Completion {
            authorization: origin.authorization.clone(),
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        applied(&mut gate);
        assert_eq!(
            dispatch(&mut gate, &mut shell, &mut registry, now)
                .unwrap()
                .routed_requests
                .len(),
            1
        );
        let stale = gate
            .enqueue_replay(
                &mut shell,
                &registry,
                ClientMouseReplay {
                    events: vec![event],
                    origin: Some(origin),
                },
                false,
                now,
            )
            .unwrap();
        assert!(stale.repaint && stale.routed_requests.is_empty());
        assert!(gate.batches.is_empty());
    }

    #[test]
    fn replacement_roster_removes_only_ended_reporter_and_keeps_live_active_owner() {
        let (mut gate, mut shell, registry, _) = fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        let mut snapshot = shell.snapshot.as_ref().unwrap().as_ref().clone();
        let mut terminal = reporter(InputIntentState::Command);
        terminal.sessions[0].active = false;
        terminal.sessions.push(InputIntentSession {
            session: "reporter-b".into(),
            generation: 7,
            policy: InputIntentPolicy::Mode,
            state: InputIntentState::Command,
            active: true,
        });
        snapshot.revision += 1;
        snapshot.input_intents = Some(vec![terminal.clone()].into());
        shell.set_endpoint_snapshot_for_generation(&ClientEndpointId::Local, 1, Box::new(snapshot));
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        applied(&mut gate);
        let active = gate.authorization.as_ref().unwrap().clone();
        assert!(gate
            .live
            .iter()
            .any(|identity| identity.reporter_session.as_deref() == Some("reporter-a")));
        let mut snapshot = shell.snapshot.as_ref().unwrap().as_ref().clone();
        snapshot.revision += 1;
        terminal.sessions.remove(0);
        snapshot.input_intents = Some(vec![terminal].into());
        shell.set_endpoint_snapshot_for_generation(&ClientEndpointId::Local, 1, Box::new(snapshot));
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        assert!(!gate
            .live
            .iter()
            .any(|identity| identity.reporter_session.as_deref() == Some("reporter-a")));
        assert!(gate
            .live
            .iter()
            .any(|identity| identity.reporter_session.as_deref() == Some("reporter-b")));
        assert_eq!(gate.authorization.as_deref(), Some(active.as_ref()));
        assert!(gate.applied);
        assert!(!gate.pending_plan.as_ref().unwrap().start_episode);
    }

    #[test]
    fn disconnected_endpoint_drops_all_local_reporter_connections_from_live_roster() {
        let (mut gate, mut shell, registry, _) = fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        applied(&mut gate);
        let old = gate.authorization.as_ref().unwrap().clone();
        shell.set_endpoint_status(&ClientEndpointId::Local, ClientEndpointStatus::Reconnecting);
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        assert!(gate.pending_plan.as_ref().unwrap().live_leases.is_empty());
        assert!(gate.desired.is_none());
        gate.complete(Completion {
            authorization: old,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(!gate.applied);
    }

    #[test]
    fn focus_gained_without_lost_retires_cached_authorization_and_samples_again() {
        let (mut gate, mut shell, registry, _) = fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        applied(&mut gate);
        let old = gate.authorization.as_ref().unwrap().clone();
        gate.enqueue(
            &mut shell,
            &registry,
            Vec::new(),
            vec![RawInputEvent::OuterFocusGained],
            None,
            false,
            now,
        )
        .unwrap();
        assert!(gate.authorization.as_ref().unwrap().focus_epoch > old.focus_epoch);
        assert!(gate.pending_plan.as_ref().unwrap().start_episode);
        gate.complete(Completion {
            authorization: old,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(!gate.applied);
    }
}

#[cfg(test)]
mod foreground_sample_tests {
    use super::*;

    #[test]
    fn cached_applied_never_skips_fresh_ack_for_a_new_real_tty_batch() {
        let (mut gate, mut shell, mut registry, _) =
            super::tests::fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        super::tests::enqueue(&mut gate, &mut shell, &registry, b"x", now);
        super::tests::applied(&mut gate);
        super::tests::dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        let old = gate.authorization.as_ref().unwrap().clone();
        // A compositor may pause the daemon while this client's last ACK remains cached.
        super::tests::enqueue(
            &mut gate,
            &mut shell,
            &registry,
            b"y",
            now + Duration::from_millis(1),
        );
        assert!(!gate.applied);
        assert!(gate.pending_plan.as_ref().unwrap().start_episode);
        assert!(gate.authorization.as_ref().unwrap().arbitration_epoch > old.arbitration_epoch);
        gate.complete(Completion {
            authorization: old,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        assert!(super::tests::dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
        super::tests::applied(&mut gate);
        assert_eq!(
            super::tests::dispatch(&mut gate, &mut shell, &mut registry, now)
                .unwrap()
                .routed_requests
                .len(),
            1
        );
    }

    #[test]
    fn batches_waiting_on_same_uncached_foreground_read_do_not_starve_its_completion() {
        let (mut gate, mut shell, mut registry, _) =
            super::tests::fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        super::tests::enqueue(&mut gate, &mut shell, &registry, b"x", now);
        let pending = gate.authorization.as_ref().unwrap().clone();
        super::tests::enqueue(
            &mut gate,
            &mut shell,
            &registry,
            b"y",
            now + Duration::from_secs(1),
        );
        assert_eq!(gate.authorization.as_deref(), Some(pending.as_ref()));
        assert_eq!(gate.deadline(), Some(now + INPUT_WAIT));
        gate.complete(Completion {
            authorization: pending,
            result: Ok(AckScope::Applied),
        })
        .unwrap();
        let first = super::tests::dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        let second = super::tests::dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        assert!(
            matches!(first.routed_requests.as_slice(), [(_, ClientMessage::ClientShellPaneInput { events, .. })] if matches!(events.as_slice(), [ClientPaneInputEvent::Key { code: crate::protocol::ClientKeyCode::Char('x'), .. }]))
        );
        assert!(
            matches!(second.routed_requests.as_slice(), [(_, ClientMessage::ClientShellPaneInput { events, .. })] if matches!(events.as_slice(), [ClientPaneInputEvent::Key { code: crate::protocol::ClientKeyCode::Char('y'), .. }]))
        );
        assert!(super::tests::dispatch(&mut gate, &mut shell, &mut registry, now).is_none());
    }

    #[test]
    fn background_mode_generations_do_not_reopen_inactive_episode_but_real_input_does() {
        let (mut gate, mut shell, registry, _) =
            super::tests::fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        super::tests::enqueue(&mut gate, &mut shell, &registry, b"x", now);
        gate.complete(Completion {
            authorization: gate.authorization.as_ref().unwrap().clone(),
            result: Ok(AckScope::Inactive),
        })
        .unwrap();
        let mut snapshot = shell.snapshot.as_ref().unwrap().as_ref().clone();
        snapshot.revision += 1;
        let mut roster = snapshot.input_intents.as_ref().unwrap().to_vec();
        roster[0].sessions[0].generation += 1;
        snapshot.input_intents = Some(roster.into());
        shell.set_endpoint_snapshot_for_generation(&ClientEndpointId::Local, 1, Box::new(snapshot));
        gate.synchronize(&mut shell, &registry, false, now).unwrap();
        assert!(!gate.pending_plan.as_ref().unwrap().start_episode);
        assert!(!gate.applied);
        super::tests::enqueue(&mut gate, &mut shell, &registry, b"y", now);
        assert!(gate.pending_plan.as_ref().unwrap().start_episode);
        assert_eq!(gate.deadline(), Some(now + INPUT_WAIT));
    }

    #[test]
    fn balancing_key_release_neither_samples_foreground_nor_waits_for_an_ack() {
        let (mut gate, mut shell, mut registry, _) =
            super::tests::fixture(Some(InputIntentState::Command));
        let now = Instant::now();
        super::tests::enqueue(&mut gate, &mut shell, &registry, b"\x1b[120;1u", now);
        super::tests::applied(&mut gate);
        super::tests::dispatch(&mut gate, &mut shell, &mut registry, now).unwrap();
        gate.pending_plan = None;
        gate.applied = false;
        let old = gate.authorization.as_ref().unwrap().clone();
        let key = crate::input::TerminalKey::new(
            crossterm::event::KeyCode::Char('x'),
            crossterm::event::KeyModifiers::NONE,
        )
        .with_kind(KeyEventKind::Release);
        let outcome = gate
            .enqueue(
                &mut shell,
                &registry,
                Vec::new(),
                vec![RawInputEvent::Key(key)],
                None,
                false,
                now,
            )
            .unwrap();
        assert_eq!(outcome.routed_requests.len(), 1);
        assert!(gate.pending_plan.is_none());
        assert_eq!(gate.authorization.as_deref(), Some(old.as_ref()));
    }
}
