//! The interactive client's only connection to the local IME daemon.
//!
//! One serial worker owns the sockets. A replacement plan never cancels an
//! already-started RPC, but is inspected between every RPC; release plans are
//! retained separately so that coalescing cannot erase a focus-loss release.

use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
#[cfg(unix)]
use std::thread;
use std::time::{Duration, Instant};

use crate::api::schema::panes::{InputIntentPolicy, InputIntentState};
use super::endpoint::ClientEndpointId;
use super::events::ClientLoopEvent;

#[cfg(unix)]
const CONNECT_LIMIT: Duration = Duration::from_secs(1);
#[cfg(unix)]
const RPC_LIMIT: Duration = Duration::from_secs(4);
const PLAN_LIMIT: Duration = Duration::from_secs(5);
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(1);
#[cfg(unix)]
const IDLE_POLL: Duration = Duration::from_millis(20);
#[cfg(unix)]
const MAX_FRAME: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum LeaseTarget {
    Terminal(Arc<str>),
    LocalUi,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct LeaseIdentity {
    pub(super) endpoint_id: ClientEndpointId,
    pub(super) connection_generation: u64,
    pub(super) boot_id: Arc<str>,
    pub(super) terminal_target: LeaseTarget,
    pub(super) reporter_session: Option<Arc<str>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AuthorizationKey {
    pub(super) identity: Arc<LeaseIdentity>,
    pub(super) intent_generation: u64,
    pub(super) focus_epoch: u64,
    pub(super) arbitration_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DesiredLease {
    pub(super) identity: Arc<LeaseIdentity>,
    pub(super) policy: InputIntentPolicy,
    pub(super) state: InputIntentState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct WorkerPlan {
    pub(super) authorization: Arc<AuthorizationKey>,
    pub(super) desired: Option<DesiredLease>,
    pub(super) live_leases: Arc<[Arc<LeaseIdentity>]>,
    pub(super) start_episode: bool,
    pub(super) deadline: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AckScope {
    Applied,
    Inactive,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Completion {
    pub(super) authorization: Arc<AuthorizationKey>,
    pub(super) result: Result<AckScope, ImeError>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImeError {
    GuiUnavailable,
    UnsupportedPlatform,
    UnsafeSocket(String),
    Transport(String),
    BadAck(String),
    Timeout,
    WorkerStopped,
    Daemon(String),
}

impl fmt::Display for ImeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GuiUnavailable => f.write_str("IME_CONTROL_GUI_UNAVAILABLE"),
            Self::UnsupportedPlatform => f.write_str("IME_CONTROL_UNSUPPORTED_PLATFORM"),
            Self::UnsafeSocket(reason) => write!(f, "IME_CONTROL_UNSAFE_SOCKET: {reason}"),
            Self::Transport(reason) => write!(f, "IME_CONTROL_TRANSPORT: {reason}"),
            Self::BadAck(reason) => write!(f, "IME_CONTROL_BAD_ACK: {reason}"),
            Self::Timeout => f.write_str("IME_CONTROL_TIMEOUT"),
            Self::WorkerStopped => f.write_str("IME_CONTROL_WORKER_STOPPED"),
            Self::Daemon(code) => write!(f, "IME_CONTROL_DAEMON: {code}"),
        }
    }
}

impl std::error::Error for ImeError {}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct QueuedPlan {
    revision: u64,
    plan: WorkerPlan,
}

#[derive(Default)]
struct Mailbox {
    revision: u64,
    latest_key: Option<Arc<AuthorizationKey>>,
    pending: Option<QueuedPlan>,
    release: Option<QueuedPlan>,
    stopped: bool,
    shutdown_requested: bool,
    failure: Option<ImeError>,
}

#[derive(Default)]
struct Shared {
    mailbox: Mutex<Mailbox>,
    wake: Condvar,
    #[cfg(unix)]
    sockets: Mutex<std::collections::HashMap<Arc<LeaseIdentity>, std::os::unix::net::UnixStream>>,
}

impl Shared {
    fn current(&self, revision: u64) -> bool {
        let mailbox = lock(&self.mailbox);
        !mailbox.stopped && mailbox.revision == revision
    }

    fn stop(&self) {
        let mut mailbox = lock(&self.mailbox);
        mailbox.stopped = true;
        mailbox.pending = None;
        mailbox.release = None;
        drop(mailbox);
        self.wake.notify_all();
        #[cfg(unix)]
        for socket in lock(&self.sockets).values() {
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
    }
}

pub(super) struct ImeWorker {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
    finished: std::sync::mpsc::Receiver<()>,
}

impl ImeWorker {
    pub(super) fn start(
        completion_sender: tokio::sync::mpsc::Sender<ClientLoopEvent>,
    ) -> Result<Self, ImeError> {
        #[cfg(unix)]
        {
            local::check_gui()?;
            Self::start_at(local::socket_path()?, completion_sender)
        }
        #[cfg(not(unix))]
        {
            let _ = completion_sender;
            Err(ImeError::UnsupportedPlatform)
        }
    }

    #[cfg(unix)]
    fn start_at(
        path: std::path::PathBuf,
        completion_sender: tokio::sync::mpsc::Sender<ClientLoopEvent>,
    ) -> Result<Self, ImeError> {
        local::validate_socket(&path)?;
        let shared = Arc::new(Shared::default());
        let worker_shared = Arc::clone(&shared);
        let (finished_tx, finished) = std::sync::mpsc::channel();
        let thread = thread::Builder::new()
            .name("herdr-ime-control".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    local::run(path, &worker_shared, &completion_sender);
                }));
                if result.is_err() {
                    worker_shared.stop();
                    let latest_key = lock(&worker_shared.mailbox).latest_key.clone();
                    if let Some(key) = latest_key {
                        local::send_final(&completion_sender, Completion {
                            authorization: key,
                            result: Err(ImeError::WorkerStopped),
                        }, &worker_shared);
                    }
                }
                worker_shared.stop();
                // Shutdown clones must not retain descriptors after the owning
                // sockets have gone away, including an unwinding worker.
                lock(&worker_shared.sockets).clear();
                let _ = finished_tx.send(());
            })
            .map_err(|error| ImeError::Transport(error.to_string()))?;
        Ok(Self { shared, thread: Some(thread), finished })
    }

    pub(super) fn submit(&self, mut plan: WorkerPlan) -> Result<(), ImeError> {
        let now = Instant::now();
        plan.deadline = plan.deadline.min(now + PLAN_LIMIT);
        if plan.deadline <= now {
            self.shared.stop();
            return Err(ImeError::Timeout);
        }
        if plan.desired.as_ref().is_some_and(|desired| {
            desired.identity != plan.authorization.identity
                || !plan.live_leases.contains(&desired.identity)
                || !valid_desired(desired)
        }) {
            self.shared.stop();
            return Err(ImeError::BadAck("invalid desired lease".into()));
        }
        let mut mailbox = lock(&self.shared.mailbox);
        if mailbox.stopped || mailbox.failure.is_some() {
            return Err(mailbox.failure.clone().unwrap_or(ImeError::WorkerStopped));
        }
        mailbox.revision = mailbox.revision.checked_add(1)
            .ok_or(ImeError::WorkerStopped)?;
        let revision = mailbox.revision;
        if let Some(previous) = &mailbox.pending {
            // Coalescing a classifier update in a still-pending real focus/key
            // episode must not erase the permission to begin that episode.
            if previous.plan.authorization.identity == plan.authorization.identity
                && previous.plan.authorization.focus_epoch == plan.authorization.focus_epoch
                && previous.plan.desired.is_some() && plan.desired.is_some()
            {
                plan.start_episode |= previous.plan.start_episode;
                plan.deadline = plan.deadline.min(previous.plan.deadline);
            }
        }
        mailbox.latest_key = Some(Arc::clone(&plan.authorization));
        if plan.desired.is_none() {
            // An intervening release must survive a subsequent acquire. The
            // latest acquire remains a separate coalesced replacement plan.
            mailbox.pending = None;
            mailbox.release = Some(QueuedPlan { revision, plan });
        } else {
            mailbox.pending = Some(QueuedPlan { revision, plan });
        }
        drop(mailbox);
        self.shared.wake.notify_one();
        Ok(())
    }

    pub(super) fn shutdown(&mut self) {
        lock(&self.shared.mailbox).shutdown_requested = true;
        self.shared.stop();
        if self.thread.is_some() && self.finished.recv_timeout(SHUTDOWN_LIMIT).is_ok() {
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
        // If an OS operation has not returned, detach rather than blocking the
        // event loop. All sockets have already been shutdown and are bounded.
        self.thread.take();
    }
}

impl Drop for ImeWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn valid_desired(desired: &DesiredLease) -> bool {
    matches!(
        (desired.policy, desired.state),
        (InputIntentPolicy::Mode, InputIntentState::Command | InputIntentState::Text)
            | (InputIntentPolicy::Entry, InputIntentState::Command)
    ) && match &desired.identity.terminal_target {
        LeaseTarget::Terminal(terminal) => !terminal.is_empty()
            && desired.identity.reporter_session.as_ref().is_some_and(|session| !session.is_empty()),
        LeaseTarget::LocalUi => desired.identity.reporter_session.is_none(),
    }
}

#[cfg(unix)]
mod local {
    use super::*;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::io::{self, IsTerminal, Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    use std::os::unix::net::UnixStream;
    use std::path::{Component, Path, PathBuf};

    pub(super) fn socket_path() -> Result<PathBuf, ImeError> {
        if let Some(path) = env::var_os("HERDR_IME_CONTROL_SOCKET") {
            if path.is_empty() {
                return Err(ImeError::UnsafeSocket("empty override".into()));
            }
            return Ok(PathBuf::from(path));
        }
        let home = env::var_os("HOME").filter(|value| !value.is_empty())
            .ok_or_else(|| ImeError::UnsafeSocket("HOME is unavailable".into()))?;
        Ok(PathBuf::from(home).join(".local/state/infra-as-code/ime-control/run/control.sock"))
    }

    pub(super) fn check_gui() -> Result<(), ImeError> {
        if !cfg!(any(target_os = "macos", target_os = "linux")) {
            return Err(ImeError::UnsupportedPlatform);
        }
        if !io::stdin().is_terminal()
            || ["SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"].iter()
                .any(|key| env::var_os(key).is_some_and(|value| !value.is_empty()))
        {
            return Err(ImeError::GuiUnavailable);
        }
        let uid = unsafe { libc::getuid() };
        #[cfg(target_os = "macos")]
        if fs::metadata("/dev/console").is_ok_and(|metadata| metadata.uid() == uid) {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            let runtime = format!("/run/user/{uid}");
            let bus = format!("unix:path={runtime}/bus");
            if env::var_os("XDG_RUNTIME_DIR").as_deref() == Some(std::ffi::OsStr::new(&runtime))
                && env::var_os("DBUS_SESSION_BUS_ADDRESS").as_deref() == Some(std::ffi::OsStr::new(&bus))
                && ["WAYLAND_DISPLAY", "DISPLAY"].iter()
                    .any(|key| env::var_os(key).is_some_and(|value| !value.is_empty()))
                && fs::symlink_metadata(&runtime).is_ok_and(|metadata| {
                    metadata.is_dir() && metadata.uid() == uid && metadata.mode() & 0o077 == 0
                })
            {
                // The daemon independently verifies the active local graphical
                // login. This check rejects headless/SSH callers before opening
                // a lease; it does not authorize a source operation.
                return Ok(());
            }
        }
        Err(ImeError::GuiUnavailable)
    }

    pub(super) fn validate_socket(path: &Path) -> Result<fs::Metadata, ImeError> {
        let fail = |reason: &str| ImeError::UnsafeSocket(reason.into());
        if !path.is_absolute() || path.as_os_str().as_bytes().contains(&0) {
            return Err(fail("socket must be a local absolute path"));
        }
        let uid = unsafe { libc::getuid() };
        let mut prefix = PathBuf::new();
        let mut private = false;
        let mut components = path.components().peekable();
        while let Some(component) = components.next() {
            match component {
                Component::RootDir | Component::Normal(_) => prefix.push(component.as_os_str()),
                _ => return Err(fail("non-normal socket path component")),
            }
            let metadata = fs::symlink_metadata(&prefix)
                .map_err(|error| ImeError::Transport(format!("local socket path is unavailable: {error}")))?;
            if metadata.file_type().is_symlink() {
                return Err(fail("socket path traverses a symlink"));
            }
            let mode = metadata.mode();
            if components.peek().is_none() {
                if !private || !metadata.file_type().is_socket()
                    || metadata.uid() != uid || mode & 0o077 != 0
                {
                    return Err(fail("socket is not current-UID private"));
                }
                return Ok(metadata);
            }
            if !metadata.is_dir() || (metadata.uid() != uid && metadata.uid() != 0) {
                return Err(fail("untrusted socket ancestor"));
            }
            // Root-owned sticky shared roots (e.g. /tmp) protect owned children
            // from unlinking. A private current-UID directory is still required
            // below them, and writable ancestors below that are never accepted.
            let sticky_root = !private && metadata.uid() == 0 && mode & 0o1000 != 0;
            if mode & 0o022 != 0 && !sticky_root {
                return Err(fail("externally writable socket ancestor"));
            }
            if private && (metadata.uid() != uid || mode & 0o077 != 0) {
                return Err(fail("socket ancestor escapes its private directory"));
            }
            private |= metadata.uid() == uid && mode & 0o077 == 0;
        }
        Err(fail("missing socket filename"))
    }

    fn io_error(error: io::Error) -> ImeError {
        if matches!(error.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock) {
            ImeError::Timeout
        } else {
            ImeError::Transport(error.to_string())
        }
    }

    fn wait_fd(fd: libc::c_int, events: libc::c_short, deadline: Instant) -> Result<(), ImeError> {
        loop {
            let remaining = deadline.checked_duration_since(Instant::now()).ok_or(ImeError::Timeout)?;
            let millis = remaining.as_millis().saturating_add(1).min(libc::c_int::MAX as u128);
            let mut poll = libc::pollfd { fd, events, revents: 0 };
            let ready = unsafe { libc::poll(&mut poll, 1, millis as libc::c_int) };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted { continue; }
                return Err(io_error(error));
            }
            if Instant::now() >= deadline { return Err(ImeError::Timeout); }
            if ready == 0 { return Err(ImeError::Timeout); }
            if poll.revents & libc::POLLNVAL != 0 {
                return Err(ImeError::Transport("invalid local socket descriptor".into()));
            }
            // HUP/ERR are passed to read/write for their precise EOF/error.
            return Ok(());
        }
    }

    fn connect(path: &Path, deadline: Instant) -> Result<UnixStream, ImeError> {
        let before = validate_socket(path)?;
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        let bytes = path.as_os_str().as_bytes();
        if bytes.len() >= address.sun_path.len() {
            return Err(ImeError::UnsafeSocket("socket path exceeds platform limit".into()));
        }
        address.sun_family = libc::AF_UNIX as _;
        for (target, byte) in address.sun_path.iter_mut().zip(bytes) { *target = *byte as _; }
        #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "openbsd", target_os = "netbsd", target_os = "dragonfly"))]
        { address.sun_len = std::mem::size_of::<libc::sockaddr_un>() as _; }
        let descriptor = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if descriptor < 0 { return Err(io_error(io::Error::last_os_error())); }
        let descriptor = unsafe { OwnedFd::from_raw_fd(descriptor) };
        if unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0
            || unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0
        {
            return Err(io_error(io::Error::last_os_error()));
        }
        #[cfg(target_os = "macos")]
        {
            let enabled: libc::c_int = 1;
            if unsafe { libc::setsockopt(descriptor.as_raw_fd(), libc::SOL_SOCKET, libc::SO_NOSIGPIPE,
                (&enabled as *const libc::c_int).cast(), std::mem::size_of_val(&enabled) as libc::socklen_t) } != 0
            {
                return Err(io_error(io::Error::last_os_error()));
            }
        }
        if Instant::now() >= deadline { return Err(ImeError::Timeout); }
        let result = unsafe { libc::connect(
            descriptor.as_raw_fd(),
            (&address as *const libc::sockaddr_un).cast(),
            std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
        ) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if !matches!(error.raw_os_error(), Some(libc::EINPROGRESS) | Some(libc::EAGAIN)) {
                return Err(io_error(error));
            }
            wait_fd(descriptor.as_raw_fd(), libc::POLLOUT, deadline)?;
            let mut socket_error: libc::c_int = 0;
            let mut length = std::mem::size_of_val(&socket_error) as libc::socklen_t;
            if unsafe { libc::getsockopt(descriptor.as_raw_fd(), libc::SOL_SOCKET, libc::SO_ERROR,
                (&mut socket_error as *mut libc::c_int).cast(), &mut length) } < 0
            {
                return Err(io_error(io::Error::last_os_error()));
            }
            if socket_error != 0 { return Err(io_error(io::Error::from_raw_os_error(socket_error))); }
        }
        if Instant::now() >= deadline { return Err(ImeError::Timeout); }
        let stream = UnixStream::from(descriptor);
        let after = validate_socket(path)?;
        if before.dev() != after.dev() || before.ino() != after.ino() {
            return Err(ImeError::UnsafeSocket("socket changed while connecting".into()));
        }
        verify_peer(&stream)?;
        Ok(stream)
    }

    fn verify_peer(stream: &UnixStream) -> Result<(), ImeError> {
        let expected = unsafe { libc::getuid() };
        #[cfg(target_os = "linux")]
        {
            let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
            let mut length = std::mem::size_of_val(&credentials) as libc::socklen_t;
            if unsafe { libc::getsockopt(stream.as_raw_fd(), libc::SOL_SOCKET, libc::SO_PEERCRED,
                (&mut credentials as *mut libc::ucred).cast(), &mut length) } != 0
                || length as usize != std::mem::size_of_val(&credentials)
                || credentials.uid != expected
            {
                return Err(ImeError::UnsafeSocket("daemon peer UID is unverified".into()));
            }
            return Ok(());
        }
        #[cfg(target_os = "macos")]
        {
            let mut uid = 0;
            let mut gid = 0;
            if unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) } != 0 || uid != expected {
                return Err(ImeError::UnsafeSocket("daemon peer UID is unverified".into()));
            }
            return Ok(());
        }
        #[allow(unreachable_code)]
        {
            let _ = (stream, expected);
            Err(ImeError::UnsupportedPlatform)
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SuccessAck<'a> {
        ok: bool,
        generation: u64,
        #[serde(borrow)]
        session: &'a str,
        #[serde(borrow)]
        scope: &'a str,
    }

    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct FailureAck<'a> {
        ok: bool,
        generation: u64,
        #[serde(borrow)]
        error: &'a str,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Status { Active, Suspended, Inactive }

    struct Connection {
        identity: Arc<LeaseIdentity>,
        shared: Arc<Shared>,
        stream: UnixStream,
        session: Option<Box<str>>,
        generation: u64,
        policy: InputIntentPolicy,
        status: Status,
    }

    impl Drop for Connection {
        fn drop(&mut self) {
            let _ = self.stream.shutdown(std::net::Shutdown::Both);
            lock(&self.shared.sockets).remove(&self.identity);
        }
    }

    impl Connection {
        fn open(path: &Path, identity: Arc<LeaseIdentity>, shared: &Arc<Shared>, deadline: Instant,
            policy: InputIntentPolicy) -> Result<Self, ImeError>
        {
            let stream = connect(path, deadline.min(Instant::now() + CONNECT_LIMIT))?;
            let shutdown_socket = stream.try_clone().map_err(io_error)?;
            let mailbox = lock(&shared.mailbox);
            if mailbox.stopped { return Err(ImeError::WorkerStopped); }
            lock(&shared.sockets).insert(Arc::clone(&identity), shutdown_socket);
            drop(mailbox);
            Ok(Self { identity, shared: Arc::clone(shared), stream, session: None,
                generation: 0, policy, status: Status::Inactive })
        }

        fn rpc(&mut self, request: &[u8], deadline: Instant) -> Result<AckScope, ImeError> {
            let deadline = deadline.min(Instant::now() + RPC_LIMIT);
            let mut written = 0;
            while written < request.len() {
                wait_fd(self.stream.as_raw_fd(), libc::POLLOUT, deadline)?;
                match self.stream.write(&request[written..]) {
                    Ok(0) => return Err(ImeError::Transport("daemon closed during request".into())),
                    Ok(count) => written += count,
                    Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => {},
                    Err(error) => return Err(io_error(error)),
                }
            }
            let mut frame = [0u8; MAX_FRAME];
            let mut length = 0;
            loop {
                if length == MAX_FRAME { return Err(ImeError::BadAck("ACK exceeds 4096 bytes".into())); }
                wait_fd(self.stream.as_raw_fd(), libc::POLLIN, deadline)?;
                let count = match self.stream.read(&mut frame[length..]) {
                    Ok(0) => return Err(ImeError::Transport("daemon disconnected before ACK".into())),
                    Ok(count) => count,
                    Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => continue,
                    Err(error) => return Err(io_error(error)),
                };
                let start = length;
                length += count;
                if let Some(end) = frame[start..length].iter().position(|byte| *byte == b'\n') {
                    if start + end + 1 != length {
                        return Err(ImeError::BadAck("unsolicited bytes after ACK".into()));
                    }
                    if Instant::now() >= deadline { return Err(ImeError::Timeout); }
                    return self.ack(&frame[..length]);
                }
            }
        }

        fn ack(&mut self, frame: &[u8]) -> Result<AckScope, ImeError> {
            if let Ok(response) = serde_json::from_slice::<SuccessAck<'_>>(frame) {
                if !response.ok || response.session.is_empty() || response.generation == 0
                    || response.generation < self.generation
                    || self.session.as_deref().is_some_and(|session| session != response.session)
                {
                    return Err(ImeError::BadAck("ACK changed lease identity or generation".into()));
                }
                let scope = match response.scope {
                    "applied" => AckScope::Applied,
                    "inactive" => AckScope::Inactive,
                    _ => return Err(ImeError::BadAck("unexpected ACK scope".into())),
                };
                self.generation = response.generation;
                if self.session.is_none() { self.session = Some(response.session.into()); }
                return Ok(scope);
            }
            if let Ok(response) = serde_json::from_slice::<FailureAck<'_>>(frame) {
                if response.ok || response.generation == 0 || response.generation < self.generation {
                    return Err(ImeError::BadAck("invalid failure generation".into()));
                }
                if !matches!(response.error, "INVALID_REQUEST" | "NO_LEASE" | "STALE_LEASE"
                    | "UNKNOWN" | "BACKEND_UNAVAILABLE" | "FOCUS_UNAVAILABLE" | "FOCUS_UNVERIFIED")
                {
                    return Err(ImeError::BadAck("invalid daemon error code".into()));
                }
                return Err(ImeError::Daemon(response.error.into()));
            }
            Err(ImeError::BadAck("malformed ACK".into()))
        }

        fn check_idle(&self) -> Result<(), ImeError> {
            let mut byte = 0u8;
            let result = unsafe { libc::recv(self.stream.as_raw_fd(),
                (&mut byte as *mut u8).cast(), 1, libc::MSG_PEEK | libc::MSG_DONTWAIT) };
            if result == 0 {
                return Err(ImeError::Transport("daemon disconnected while lease was idle".into()));
            }
            if result > 0 { return Err(ImeError::BadAck("unsolicited daemon frame".into())); }
            let error = io::Error::last_os_error();
            if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) { return Ok(()); }
            Err(io_error(error))
        }
    }

    struct Leases {
        path: PathBuf,
        shared: Arc<Shared>,
        connections: HashMap<Arc<LeaseIdentity>, Connection>,
        current: Option<Arc<LeaseIdentity>>,
    }

    enum Outcome { Complete(AckScope), Superseded }

    impl Leases {
        fn suspend(&mut self, identity: &Arc<LeaseIdentity>, deadline: Instant) -> Result<(), ImeError> {
            if let Some(connection) = self.connections.get_mut(identity) {
                if connection.status != Status::Suspended {
                    let scope = connection.rpc(b"{\"op\":\"suspend\"}\n", deadline)?;
                    if scope != AckScope::Applied {
                        return Err(ImeError::BadAck("suspend did not confirm release".into()));
                    }
                    connection.status = Status::Suspended;
                }
            }
            if self.current.as_ref() == Some(identity) { self.current = None; }
            Ok(())
        }

        fn release_all(&mut self, deadline: Instant) -> Result<(), ImeError> {
            if let Some(current) = self.current.clone() { self.suspend(&current, deadline)?; }
            // Inactive also requires explicit suspend: an in-flight foreground
            // change must not leave a declared mode poised to regain ownership.
            for connection in self.connections.values_mut() {
                if connection.status != Status::Suspended {
                    let scope = connection.rpc(b"{\"op\":\"suspend\"}\n", deadline)?;
                    if scope != AckScope::Applied {
                        return Err(ImeError::BadAck("suspend did not confirm release".into()));
                    }
                    connection.status = Status::Suspended;
                }
            }
            Ok(())
        }

        fn reconcile(&mut self, queued: &QueuedPlan) -> Result<Outcome, ImeError> {
            let plan = &queued.plan;
            if Instant::now() >= plan.deadline { return Err(ImeError::Timeout); }
            if plan.desired.is_none() {
                // Run even a superseded release. A later desired replacement
                // cannot erase the local focus-loss barrier.
                self.release_all(plan.deadline)?;
            } else if !self.shared.current(queued.revision) {
                return Ok(Outcome::Superseded);
            }
            if !self.shared.current(queued.revision) { return Ok(Outcome::Superseded); }
            // Release the owning target first, before closing inactive roster
            // removals or opening any new target connection.
            let desired_id = plan.desired.as_ref().map(|desired| &desired.identity);
            if self.current.as_ref().is_some_and(|current| Some(current) != desired_id) {
                let current = self.current.clone().expect("current target checked");
                self.suspend(&current, plan.deadline)?;
                if !self.shared.current(queued.revision) { return Ok(Outcome::Superseded); }
            }
            loop {
                let removed = self.connections.keys()
                    .find(|identity| !plan.live_leases.contains(identity)).cloned();
                let Some(identity) = removed else { break; };
                if let Some(mut connection) = self.connections.remove(&identity) {
                    let result = connection.rpc(b"{\"op\":\"close\"}\n", plan.deadline);
                    drop(connection); // EOF releases even an unsuccessful close.
                    if result? != AckScope::Applied {
                        return Err(ImeError::BadAck("close did not confirm release".into()));
                    }
                }
                if !self.shared.current(queued.revision) { return Ok(Outcome::Superseded); }
            }
            let Some(desired) = &plan.desired else {
                return Ok(Outcome::Complete(AckScope::Applied));
            };
            // Every retained reporter that is not this UI target is suspended;
            // connections are retained to preserve daemon-owned entry_armed.
            for connection in self.connections.values_mut() {
                if connection.identity != desired.identity && connection.status != Status::Suspended {
                    let scope = connection.rpc(b"{\"op\":\"suspend\"}\n", plan.deadline)?;
                    if scope != AckScope::Applied {
                        return Err(ImeError::BadAck("suspend did not confirm release".into()));
                    }
                    connection.status = Status::Suspended;
                    if !self.shared.current(queued.revision) { return Ok(Outcome::Superseded); }
                }
            }
            if self.connections.get(&desired.identity).is_some_and(|connection| connection.policy != desired.policy) {
                // A new policy is a new declaration, not a mode update. Release
                // its predecessor before declaring it on the same serial socket.
                self.suspend(&desired.identity, plan.deadline)?;
                if !self.shared.current(queued.revision) { return Ok(Outcome::Superseded); }
            }
            if let Some(connection) = self.connections.get(&desired.identity) {
                if connection.status != Status::Active && !plan.start_episode {
                    return Ok(Outcome::Complete(AckScope::Inactive));
                }
            } else {
                if !plan.start_episode { return Ok(Outcome::Complete(AckScope::Inactive)); }
                let connection = Connection::open(&self.path, Arc::clone(&desired.identity),
                    &self.shared, plan.deadline, desired.policy)?;
                self.connections.insert(Arc::clone(&desired.identity), connection);
                if !self.shared.current(queued.revision) {
                    // No operation was sent, hence there is no lease to suspend.
                    self.connections.remove(&desired.identity);
                    return Ok(Outcome::Superseded);
                }
            }
            if desired.policy == InputIntentPolicy::Entry && !plan.start_episode {
                // Entry has no state RPC. A background replacement cannot use
                // resume to rebuild the compositor observer or obtain a fresh
                // authorization. Keep the existing connection/entry_armed;
                // only a real input/focus episode may sample it again.
                return Ok(Outcome::Complete(AckScope::Inactive));
            }
            let connection = self.connections.get_mut(&desired.identity).expect("desired connection exists");
            let request: &[u8] = if connection.session.is_none() || connection.policy != desired.policy {
                match (desired.policy, desired.state) {
                    (InputIntentPolicy::Entry, _) => b"{\"op\":\"enter\"}\n",
                    (_, InputIntentState::Command) => b"{\"op\":\"activate\",\"policy\":\"mode\",\"state\":\"command\"}\n",
                    _ => b"{\"op\":\"activate\",\"policy\":\"mode\",\"state\":\"text\"}\n",
                }
            } else if plan.start_episode || connection.status != Status::Active {
                if desired.state == InputIntentState::Command {
                    b"{\"op\":\"resume\",\"state\":\"command\"}\n"
                } else { b"{\"op\":\"resume\",\"state\":\"text\"}\n" }
            } else if desired.state == InputIntentState::Command {
                b"{\"op\":\"state\",\"state\":\"command\"}\n"
            } else { b"{\"op\":\"state\",\"state\":\"text\"}\n" };
            if !self.shared.current(queued.revision) {
                if connection.session.is_none() { self.connections.remove(&desired.identity); }
                return Ok(Outcome::Superseded);
            }
            let scope = connection.rpc(request, plan.deadline)?;
            connection.policy = desired.policy;
            connection.status = if scope == AckScope::Applied { Status::Active } else { Status::Inactive };
            self.current = Some(Arc::clone(&desired.identity));
            if !self.shared.current(queued.revision) { return Ok(Outcome::Superseded); }
            Ok(Outcome::Complete(scope))
        }
    }

    fn latest_key(shared: &Shared) -> Option<Arc<AuthorizationKey>> {
        lock(&shared.mailbox).latest_key.clone()
    }

    pub(super) fn send_final(sender: &tokio::sync::mpsc::Sender<ClientLoopEvent>,
        completion: Completion, shared: &Shared)
    {
        let mut event = ClientLoopEvent::ImeControl(completion);
        loop {
            match sender.try_send(event) {
                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return,
                Err(tokio::sync::mpsc::error::TrySendError::Full(pending)) => event = pending,
            }
            if lock(&shared.mailbox).shutdown_requested { return; }
            thread::sleep(IDLE_POLL);
        }
    }

    pub(super) fn run(path: PathBuf, shared: &Arc<Shared>, sender: &tokio::sync::mpsc::Sender<ClientLoopEvent>) {
        let mut leases = Leases { path, shared: Arc::clone(shared), connections: HashMap::new(), current: None };
        let mut completion = None;
        let fatal = loop {
            if sender.is_closed() { break None; }
            let queued = {
                let mut mailbox = lock(&shared.mailbox);
                if mailbox.stopped { break None; }
                if completion.as_ref().is_some_and(|(revision, _)| *revision != mailbox.revision) {
                    completion = None;
                }
                // Sending never waits on the client event queue, so event-loop
                // congestion cannot starve a pending suspend.
                if let Some((revision, event)) = completion.take() {
                    match sender.try_send(event) {
                        Ok(()) => {},
                        Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => completion = Some((revision, event)),
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break None,
                    }
                }
                mailbox.release.take().or_else(|| mailbox.pending.take())
            };
            if let Some(queued) = queued {
                match leases.reconcile(&queued) {
                    Ok(Outcome::Complete(scope)) if shared.current(queued.revision) => {
                        completion = Some((queued.revision, ClientLoopEvent::ImeControl(Completion {
                            authorization: queued.plan.authorization,
                            result: Ok(scope),
                        })));
                    }
                    Ok(_) => {},
                    Err(error) => break Some(error),
                }
                continue;
            }
            if let Some(error) = leases.connections.values().find_map(|connection| connection.check_idle().err()) {
                break Some(error);
            }
            let mailbox = lock(&shared.mailbox);
            if mailbox.stopped { break None; }
            if mailbox.release.is_none() && mailbox.pending.is_none() {
                drop(shared.wake.wait_timeout(mailbox, IDLE_POLL));
            }
        };
        // Never reconnect/replay an old intent after EOF or an uncertain RPC.
        // Dropping every owned socket is the final release fallback.
        let notify_error = {
            let mut mailbox = lock(&shared.mailbox);
            if mailbox.stopped { false } else {
                mailbox.failure = fatal.clone();
                true
            }
        };
        drop(leases);
        shared.stop();
        if notify_error {
            if let (Some(error), Some(authorization)) = (fatal, latest_key(shared)) {
                // All sockets are already closed. Retain the precise error
                // under event-queue backpressure; shutdown cancels delivery.
                send_final(sender, Completion { authorization, result: Err(error) }, shared);
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc::{self, Receiver, Sender};

    static FIXTURE_ID: AtomicU64 = AtomicU64::new(1);
    const TEST_WAIT: Duration = Duration::from_secs(2);

    enum Event {
        Request(Request),
        Eof(u64),
    }

    struct Request {
        connection: u64,
        body: serde_json::Value,
        reply: Sender<Vec<u8>>,
    }

    impl Request {
        fn op(&self) -> &str { self.body["op"].as_str().unwrap() }
        fn ack(self, scope: &str, generation: u64) {
            let response = format!("{{\"ok\":true,\"session\":\"daemon:{}\",\"generation\":{generation},\"scope\":\"{scope}\"}}\n", self.connection);
            self.reply.send(response.into_bytes()).unwrap();
        }
    }

    struct Fixture {
        root: PathBuf,
        path: PathBuf,
        events: Receiver<Event>,
        stop: Arc<AtomicBool>,
        sockets: Arc<Mutex<Vec<UnixStream>>>,
        thread: Option<JoinHandle<()>>,
    }

    impl Fixture {
        fn new() -> Self {
            // Canonicalization is only for the test's shared temporary root;
            // production validation never resolves symlinks into acceptance.
            let base = fs::canonicalize(std::env::temp_dir()).unwrap();
            let root = base.join(format!("herdr-ime-{}-{}", std::process::id(), FIXTURE_ID.fetch_add(1, Ordering::Relaxed)));
            fs::create_dir(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let path = root.join("control.sock");
            let listener = UnixListener::bind(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            listener.set_nonblocking(true).unwrap();
            let (events_tx, events) = mpsc::channel();
            let stop = Arc::new(AtomicBool::new(false));
            let sockets = Arc::new(Mutex::new(Vec::new()));
            let server_stop = Arc::clone(&stop);
            let server_sockets = Arc::clone(&sockets);
            let thread = thread::spawn(move || {
                let mut handlers = Vec::new();
                let mut next_connection = 1;
                while !server_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            stream.set_nonblocking(false).unwrap();
                            let connection = next_connection;
                            next_connection += 1;
                            stream.set_read_timeout(Some(TEST_WAIT)).unwrap();
                            stream.set_write_timeout(Some(TEST_WAIT)).unwrap();
                            lock(&server_sockets).push(stream.try_clone().unwrap());
                            let tx = events_tx.clone();
                            handlers.push(thread::spawn(move || {
                                let mut reader = BufReader::new(stream);
                                loop {
                                    let mut frame = Vec::new();
                                    match reader.read_until(b'\n', &mut frame) {
                                        Ok(0) | Err(_) => { let _ = tx.send(Event::Eof(connection)); break; },
                                        Ok(_) => {},
                                    }
                                    let body = serde_json::from_slice(&frame).unwrap();
                                    let (reply, response) = mpsc::channel();
                                    if tx.send(Event::Request(Request { connection, body, reply })).is_err() { break; }
                                    let Ok(frame) = response.recv_timeout(TEST_WAIT) else { break; };
                                    if reader.get_mut().write_all(&frame).is_err() { break; }
                                }
                            }));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(1)),
                        Err(error) => panic!("fixture accept: {error}"),
                    }
                }
                for handler in handlers { handler.join().unwrap(); }
            });
            Self { root, path, events, stop, sockets, thread: Some(thread) }
        }

        fn worker(&self) -> (ImeWorker, tokio::sync::mpsc::Receiver<ClientLoopEvent>) {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            (ImeWorker::start_at(self.path.clone(), tx).unwrap(), rx)
        }

        fn request(&self) -> Request {
            match self.events.recv_timeout(TEST_WAIT).unwrap() {
                Event::Request(request) => request,
                Event::Eof(connection) => panic!("unexpected EOF on connection {connection}"),
            }
        }

        fn eof(&self, expected: u64) {
            match self.events.recv_timeout(TEST_WAIT).unwrap() {
                Event::Eof(connection) => assert_eq!(connection, expected),
                Event::Request(request) => panic!("unexpected request {}", request.op()),
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            for stream in lock(&self.sockets).iter() { let _ = stream.shutdown(std::net::Shutdown::Both); }
            if let Some(thread) = self.thread.take() { thread.join().unwrap(); }
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    fn identity(terminal: &str, session: &str) -> Arc<LeaseIdentity> {
        Arc::new(LeaseIdentity {
            endpoint_id: ClientEndpointId::Local,
            connection_generation: 1,
            boot_id: Arc::from("boot:1"),
            terminal_target: LeaseTarget::Terminal(Arc::from(terminal)),
            reporter_session: Some(Arc::from(session)),
        })
    }

    fn plan(identity: &Arc<LeaseIdentity>, state: Option<InputIntentState>, policy: InputIntentPolicy,
        live: &[Arc<LeaseIdentity>], epoch: u64) -> WorkerPlan
    {
        WorkerPlan {
            authorization: Arc::new(AuthorizationKey { identity: Arc::clone(identity), intent_generation: epoch,
                focus_epoch: epoch, arbitration_epoch: epoch }),
            desired: state.map(|state| DesiredLease { identity: Arc::clone(identity), policy, state }),
            live_leases: Arc::from(live),
            start_episode: true,
            deadline: Instant::now() + TEST_WAIT,
        }
    }

    fn completion(rx: &mut tokio::sync::mpsc::Receiver<ClientLoopEvent>) -> Completion {
        // Bound the test independently of the worker's delivery path.
        let deadline = Instant::now() + TEST_WAIT;
        loop {
            match rx.try_recv() {
                Ok(ClientLoopEvent::ImeControl(completion)) => return completion,
                Ok(_) => panic!("unexpected client event"),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => panic!("worker exited without completion"),
                Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(1)),
                Err(_) => panic!("completion deadline exceeded"),
            }
        }
    }

    #[test]
    fn release_survives_coalescing_and_precedes_new_target_after_started_rpc() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let a = identity("a", "reporter:a");
        let b = identity("b", "reporter:b");
        let live = [a.clone(), b.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 1)).unwrap();
        let started = fixture.request();
        assert_eq!(started.op(), "activate");
        let a_connection = started.connection;
        worker.submit(plan(&a, None, InputIntentPolicy::Mode, &live, 2)).unwrap();
        worker.submit(plan(&b, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 3)).unwrap();
        started.ack("applied", 1);
        let release = fixture.request();
        assert_eq!((release.connection, release.op()), (a_connection, "suspend"));
        release.ack("applied", 2);
        let acquire = fixture.request();
        assert_ne!(acquire.connection, a_connection);
        assert_eq!(acquire.op(), "activate");
        acquire.ack("applied", 3);
        let result = completion(&mut rx);
        assert_eq!(result.authorization.arbitration_epoch, 3);
        assert_eq!(result.result, Ok(AckScope::Applied));
        worker.shutdown();
    }

    #[test]
    fn latest_state_replaces_pending_states_without_replaying_started_activation() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let a = identity("a", "a");
        let live = [a.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 1)).unwrap();
        let started = fixture.request();
        let mut text = plan(&a, Some(InputIntentState::Text), InputIntentPolicy::Mode, &live, 2);
        text.start_episode = false;
        worker.submit(text).unwrap();
        let mut command = plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 3);
        command.start_episode = false;
        worker.submit(command).unwrap();
        started.ack("applied", 1);
        let latest = fixture.request();
        assert_eq!(latest.op(), "state");
        assert_eq!(latest.body["state"], "command");
        latest.ack("applied", 2);
        let result = completion(&mut rx);
        assert_eq!(result.authorization.intent_generation, 3);
        assert_eq!(result.result, Ok(AckScope::Applied));
        worker.shutdown();
    }

    #[test]
    fn entry_uses_same_socket_resume_after_suspend_and_removed_roster_closes_it() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let a = identity("a", "a");
        let live = [a.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Entry, &live, 1)).unwrap();
        let enter = fixture.request();
        assert_eq!(enter.op(), "enter");
        let connection = enter.connection;
        enter.ack("applied", 1);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.submit(plan(&a, None, InputIntentPolicy::Entry, &live, 2)).unwrap();
        let suspend = fixture.request();
        assert_eq!((suspend.connection, suspend.op()), (connection, "suspend"));
        suspend.ack("applied", 2);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Entry, &live, 3)).unwrap();
        let resume = fixture.request();
        assert_eq!((resume.connection, resume.op()), (connection, "resume"));
        assert_eq!(resume.body["state"], "command");
        resume.ack("applied", 3);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.submit(plan(&a, None, InputIntentPolicy::Entry, &[], 4)).unwrap();
        fixture.request().ack("applied", 4); // suspend before closing the owner
        let close = fixture.request();
        assert_eq!((close.connection, close.op()), (connection, "close"));
        close.ack("applied", 5);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        fixture.eof(connection);
        worker.shutdown();
    }

    #[test]
    fn inactive_requires_real_episode_and_never_counts_as_applied() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let a = identity("a", "a");
        let live = [a.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 1)).unwrap();
        let activate = fixture.request();
        let connection = activate.connection;
        activate.ack("inactive", 1);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Inactive));
        let mut background = plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 2);
        background.start_episode = false;
        worker.submit(background).unwrap();
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Inactive));
        worker.submit(plan(&a, Some(InputIntentState::Text), InputIntentPolicy::Mode, &live, 3)).unwrap();
        let resume = fixture.request();
        assert_eq!((resume.connection, resume.op()), (connection, "resume"));
        assert_eq!(resume.body["state"], "text");
        resume.ack("applied", 2);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.shutdown();
    }

    #[test]
    fn invalid_ack_identity_generation_scope_and_extra_fields_close_owned_socket() {
        for bad in [
            "{\"ok\":true,\"generation\":3,\"session\":\"different\",\"scope\":\"applied\"}\n",
            "{\"ok\":true,\"generation\":1,\"session\":\"daemon:1\",\"scope\":\"applied\"}\n",
            "{\"ok\":true,\"generation\":3,\"session\":\"daemon:1\",\"scope\":\"pending\"}\n",
            "{\"ok\":true,\"generation\":3,\"session\":\"daemon:1\",\"scope\":\"applied\",\"source\":\"private\"}\n",
        ] {
            let fixture = Fixture::new();
            let (mut worker, mut rx) = fixture.worker();
            let a = identity("a", "a");
            let live = [a.clone()];
            worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 1)).unwrap();
            fixture.request().ack("applied", 2);
            assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
            worker.submit(plan(&a, Some(InputIntentState::Text), InputIntentPolicy::Mode, &live, 2)).unwrap();
            let request = fixture.request();
            request.reply.send(bad.as_bytes().to_vec()).unwrap();
            assert!(matches!(completion(&mut rx).result, Err(ImeError::BadAck(_))));
            fixture.eof(1);
            worker.shutdown();
        }
    }

    #[test]
    fn incomplete_ack_obeys_original_plan_deadline_and_shutdown_interrupts_rpc() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let a = identity("a", "a");
        let mut short = plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &[a.clone()], 1);
        short.deadline = Instant::now() + Duration::from_millis(500);
        worker.submit(short).unwrap();
        let request = fixture.request();
        request.reply.send(b"{\"ok\":true".to_vec()).unwrap();
        assert_eq!(completion(&mut rx).result, Err(ImeError::Timeout));
        fixture.eof(1);
        worker.shutdown();

        let fixture = Fixture::new();
        let (mut worker, _rx) = fixture.worker();
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &[a.clone()], 2)).unwrap();
        let request = fixture.request();
        let start = Instant::now();
        worker.shutdown();
        assert!(start.elapsed() < SHUTDOWN_LIMIT);
        // Let the fixture's own handler finish; the worker did not require ACK.
        request.reply.send(Vec::new()).unwrap();
        fixture.eof(1);
    }

    #[test]
    fn changed_boot_closes_old_reporter_before_new_acquisition_and_idle_eof_is_fatal() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let old = identity("a", "a");
        worker.submit(plan(&old, Some(InputIntentState::Command), InputIntentPolicy::Mode, &[old.clone()], 1)).unwrap();
        fixture.request().ack("applied", 1);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        let mut new = (*old).clone();
        new.boot_id = Arc::from("boot:2");
        let new = Arc::new(new);
        worker.submit(plan(&new, Some(InputIntentState::Command), InputIntentPolicy::Mode, &[new.clone()], 2)).unwrap();
        let suspend = fixture.request();
        assert_eq!(suspend.op(), "suspend");
        suspend.ack("applied", 2);
        let close = fixture.request();
        assert_eq!(close.op(), "close");
        close.ack("applied", 3);
        let acquire = loop {
            match fixture.events.recv_timeout(TEST_WAIT).unwrap() {
                Event::Request(request) => break request,
                Event::Eof(_) => {},
            }
        };
        assert_eq!(acquire.op(), "activate");
        assert_eq!(acquire.connection, 2);
        acquire.ack("applied", 4);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        let _ = lock(&fixture.sockets)[1].shutdown(std::net::Shutdown::Both);
        assert!(matches!(completion(&mut rx).result, Err(ImeError::Transport(_))));
        worker.shutdown();
    }

    #[test]
    fn removing_suspended_parent_does_not_release_or_retire_current_child() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let parent = identity("a", "parent");
        let child = identity("a", "child");
        let live = [parent.clone(), child.clone()];
        worker.submit(plan(&parent, Some(InputIntentState::Command), InputIntentPolicy::Entry, &live, 1)).unwrap();
        fixture.request().ack("applied", 1);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.submit(plan(&child, Some(InputIntentState::Text), InputIntentPolicy::Mode, &live, 2)).unwrap();
        let suspend = fixture.request();
        assert_eq!((suspend.connection, suspend.op()), (1, "suspend"));
        suspend.ack("applied", 2);
        let activate = fixture.request();
        assert_eq!((activate.connection, activate.op()), (2, "activate"));
        activate.ack("applied", 3);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        let mut update = plan(&child, Some(InputIntentState::Text), InputIntentPolicy::Mode, &[child.clone()], 3);
        update.start_episode = false;
        worker.submit(update).unwrap();
        let close = fixture.request();
        assert_eq!((close.connection, close.op()), (1, "close"));
        close.ack("applied", 4);
        let state = loop {
            match fixture.events.recv_timeout(TEST_WAIT).unwrap() {
                Event::Request(request) => break request,
                Event::Eof(connection) => assert_eq!(connection, 1),
            }
        };
        assert_eq!((state.connection, state.op()), (2, "state"));
        assert_eq!(state.body["state"], "text");
        state.ack("applied", 5);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.shutdown();
    }

    #[test]
    fn policy_transition_releases_before_new_declaration_on_same_socket() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let a = identity("a", "a");
        let live = [a.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Entry, &live, 1)).unwrap();
        fixture.request().ack("applied", 1);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.submit(plan(&a, Some(InputIntentState::Text), InputIntentPolicy::Mode, &live, 2)).unwrap();
        let suspend = fixture.request();
        assert_eq!((suspend.connection, suspend.op()), (1, "suspend"));
        suspend.ack("applied", 2);
        let activate = fixture.request();
        assert_eq!((activate.connection, activate.op()), (1, "activate"));
        assert_eq!(activate.body["state"], "text");
        activate.ack("applied", 3);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.shutdown();
    }

    #[test]
    fn oversized_ack_and_inactive_release_fail_closed() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let a = identity("a", "a");
        let live = [a.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 1)).unwrap();
        fixture.request().reply.send(vec![b' '; MAX_FRAME + 1]).unwrap();
        assert!(matches!(completion(&mut rx).result, Err(ImeError::BadAck(_))));
        fixture.eof(1);
        worker.shutdown();

        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 1)).unwrap();
        fixture.request().ack("applied", 1);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.submit(plan(&a, None, InputIntentPolicy::Mode, &live, 2)).unwrap();
        let release = fixture.request();
        assert_eq!(release.op(), "suspend");
        release.ack("inactive", 2);
        assert!(matches!(completion(&mut rx).result, Err(ImeError::BadAck(_))));
        fixture.eof(1);
        worker.shutdown();
    }

    #[test]
    fn socket_validation_rejects_symlinks_public_socket_and_writable_ancestors() {
        let fixture = Fixture::new();
        assert!(local::validate_socket(&fixture.path).is_ok());
        fs::set_permissions(&fixture.path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(matches!(local::validate_socket(&fixture.path), Err(ImeError::UnsafeSocket(_))));
        fs::set_permissions(&fixture.path, fs::Permissions::from_mode(0o600)).unwrap();
        let link = fixture.root.join("linked.sock");
        symlink(&fixture.path, &link).unwrap();
        assert!(matches!(local::validate_socket(&link), Err(ImeError::UnsafeSocket(_))));
        let parent_link = fixture.root.join("linked-parent");
        symlink(&fixture.root, &parent_link).unwrap();
        assert!(matches!(local::validate_socket(&parent_link.join("control.sock")), Err(ImeError::UnsafeSocket(_))));
        fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o777)).unwrap();
        assert!(matches!(local::validate_socket(&fixture.path), Err(ImeError::UnsafeSocket(_))));
        fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn full_client_event_queue_never_blocks_serial_release_or_next_target() {
        let fixture = Fixture::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(ClientLoopEvent::Timer).unwrap_or_else(|_| panic!("fixture event queue"));
        let mut worker = ImeWorker::start_at(fixture.path.clone(), tx).unwrap();
        let a = identity("a", "a");
        let b = identity("b", "b");
        let live = [a.clone(), b.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 1)).unwrap();
        let started = fixture.request();
        worker.submit(plan(&a, None, InputIntentPolicy::Mode, &live, 2)).unwrap();
        started.ack("applied", 1);
        let suspend = fixture.request();
        assert_eq!((suspend.connection, suspend.op()), (1, "suspend"));
        suspend.ack("applied", 2);
        worker.submit(plan(&b, Some(InputIntentState::Text), InputIntentPolicy::Mode, &live, 3)).unwrap();
        let activate = fixture.request();
        assert_eq!((activate.connection, activate.op()), (2, "activate"));
        activate.ack("applied", 3);
        assert!(matches!(rx.try_recv(), Ok(ClientLoopEvent::Timer)));
        let result = completion(&mut rx);
        assert_eq!(result.authorization.identity, b);
        assert_eq!(result.result, Ok(AckScope::Applied));
        worker.shutdown();
    }

    #[test]
    fn fatal_ack_closes_sockets_before_waiting_to_deliver_precise_error() {
        let fixture = Fixture::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(ClientLoopEvent::Timer).unwrap_or_else(|_| panic!("fixture event queue"));
        let mut worker = ImeWorker::start_at(fixture.path.clone(), tx).unwrap();
        let a = identity("a", "a");
        let live = [a.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 1)).unwrap();
        fixture.request().reply.send(b"{\"ok\":true}\n".to_vec()).unwrap();
        fixture.eof(1); // Release does not wait for event-queue capacity.
        let retry = worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 2));
        assert!(matches!(retry, Err(ImeError::BadAck(_))));
        assert!(matches!(rx.try_recv(), Ok(ClientLoopEvent::Timer)));
        assert!(matches!(completion(&mut rx).result, Err(ImeError::BadAck(_))));
        worker.shutdown();
    }

    #[test]
    fn genuine_episode_resamples_active_mode_with_resume_and_background_never_reacquires() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let a = identity("a", "a");
        let live = [a.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 1)).unwrap();
        let activate = fixture.request();
        assert_eq!(activate.op(), "activate");
        activate.ack("applied", 1);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));

        let mut background = plan(&a, Some(InputIntentState::Text), InputIntentPolicy::Mode, &live, 2);
        background.start_episode = false;
        worker.submit(background).unwrap();
        let state = fixture.request();
        assert_eq!((state.connection, state.op()), (1, "state"));
        assert_eq!(state.body["state"], "text");
        state.ack("applied", 2);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));

        // The daemon may have compositor-paused this lease without the client
        // receiving FocusLost. A real input must freshly sample via resume even
        // though the worker's previous ACK and cached status were both active.
        worker.submit(plan(&a, Some(InputIntentState::Text), InputIntentPolicy::Mode, &live, 3)).unwrap();
        let resume = fixture.request();
        assert_eq!((resume.connection, resume.op()), (1, "resume"));
        assert_eq!(resume.body["state"], "text");
        resume.ack("inactive", 3);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Inactive));

        let mut background = plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 4);
        background.start_episode = false;
        worker.submit(background).unwrap();
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Inactive));
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Mode, &live, 5)).unwrap();
        let resume = fixture.request();
        assert_eq!((resume.connection, resume.op()), (1, "resume"));
        assert_eq!(resume.body["state"], "command");
        resume.ack("applied", 4);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.shutdown();
    }

    #[test]
    fn background_entry_cannot_restart_observer_or_reenter_armed_lease() {
        let fixture = Fixture::new();
        let (mut worker, mut rx) = fixture.worker();
        let a = identity("a", "a");
        let live = [a.clone()];
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Entry, &live, 1)).unwrap();
        let enter = fixture.request();
        assert_eq!((enter.connection, enter.op()), (1, "enter"));
        enter.ack("applied", 1);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        let mut background = plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Entry, &live, 2);
        background.start_episode = false;
        worker.submit(background).unwrap();
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Inactive));
        worker.submit(plan(&a, Some(InputIntentState::Command), InputIntentPolicy::Entry, &live, 3)).unwrap();
        let resume = fixture.request();
        assert_eq!((resume.connection, resume.op()), (1, "resume"));
        assert_eq!(resume.body["state"], "command");
        resume.ack("applied", 2);
        assert_eq!(completion(&mut rx).result, Ok(AckScope::Applied));
        worker.shutdown();
    }
}
