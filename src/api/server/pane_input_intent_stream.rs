use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::api::schema::{
    ErrorBody, ErrorResponse, InputIntentOperation, Method, PaneInputIntentStreamOperationParams,
    PaneInputIntentStreamParams, Request, ResponseResult, SuccessResponse,
};
use crate::api::{ApiRequestMessage, ApiRequestSender};
use crate::ipc::{
    poll_local_stream_read_count, set_local_stream_polling, LocalStream, LocalStreamReadCount,
};

use super::{write_json_line_allow_disconnect, write_text_line_allow_disconnect};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
pub(super) const MAX_FRAME_BYTES: usize = 4096;
const MAX_BUFFERED_BYTES: usize = MAX_FRAME_BYTES * 16;
const APP_POLL_INTERVAL: Duration = Duration::from_millis(5);
static NEXT_STREAM_OWNER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
enum AppWaitError {
    Timeout,
    PeerClosed,
    ServerStopped,
    BufferLimit,
    Dispatch,
    Io(io::Error),
}

struct BufferedChunk {
    bytes: VecDeque<u8>,
    received_at: Instant,
}

#[derive(Default)]
struct InputBuffer {
    chunks: VecDeque<BufferedChunk>,
    buffered_bytes: usize,
}

impl InputBuffer {
    fn first_line_length(&self) -> Option<usize> {
        let mut length = 0;
        for chunk in &self.chunks {
            for byte in &chunk.bytes {
                length += 1;
                if *byte == b'\n' {
                    return Some(length);
                }
            }
        }
        None
    }

    fn front_received_at(&self) -> Option<Instant> {
        self.chunks.front().map(|chunk| chunk.received_at)
    }

    fn take_line(&mut self, length: usize) -> (Vec<u8>, Instant) {
        let received_at = self.front_received_at().expect("line has bytes");
        let mut line = Vec::with_capacity(length);
        for _ in 0..length {
            let empty = {
                let chunk = self.chunks.front_mut().expect("line chunk exists");
                line.push(chunk.bytes.pop_front().expect("line byte exists"));
                chunk.bytes.is_empty()
            };
            self.buffered_bytes -= 1;
            if empty {
                self.chunks.pop_front();
            }
        }
        (line, received_at)
    }

    fn push(&mut self, bytes: &[u8], received_at: Instant) -> Result<(), AppWaitError> {
        if self.buffered_bytes.saturating_add(bytes.len()) > MAX_BUFFERED_BYTES {
            return Err(AppWaitError::BufferLimit);
        }
        if !bytes.is_empty() {
            self.chunks.push_back(BufferedChunk {
                bytes: bytes.iter().copied().collect(),
                received_at,
            });
            self.buffered_bytes += bytes.len();
        }
        Ok(())
    }
}

enum ReadFrame {
    Frame { line: String, started_at: Instant },
    Eof,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationAck {
    ok: bool,
    generation: u64,
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

pub(super) fn serve(
    stream: LocalStream,
    request_id: String,
    params: PaneInputIntentStreamParams,
    api_tx: &ApiRequestSender,
    running: &Arc<AtomicBool>,
    server_stop: Option<&Arc<AtomicBool>>,
) -> io::Result<()> {
    serve_with_timeout(
        stream,
        request_id,
        params,
        api_tx,
        running,
        server_stop,
        REQUEST_TIMEOUT,
    )
}

fn serve_with_timeout(
    mut stream: LocalStream,
    request_id: String,
    mut params: PaneInputIntentStreamParams,
    api_tx: &ApiRequestSender,
    running: &Arc<AtomicBool>,
    server_stop: Option<&Arc<AtomicBool>>,
    request_timeout: Duration,
) -> io::Result<()> {
    if !valid_target(&params) {
        return write_error_response(
            &mut stream,
            &request_id,
            "INVALID_REQUEST",
            "exactly one nonempty pane_id or popup_terminal_id is required",
        );
    }

    let owner = next_owner();
    params.owner.clone_from(&owner);
    let open_gate = Arc::new(AtomicBool::new(true));
    let mut input = InputBuffer::default();

    let result = (|| {
        let open_request = Request {
            id: request_id.clone(),
            method: Method::PaneInputIntentStreamOpen(params),
        };
        let open_response = dispatch_request(
            open_request,
            api_tx,
            &mut stream,
            running,
            server_stop,
            &mut input,
            Instant::now() + request_timeout,
            Arc::clone(&open_gate),
        );
        let response = match open_response {
            Ok(response) => response,
            Err(AppWaitError::Timeout) => {
                open_gate.store(false, Ordering::Release);
                write_error_response(
                    &mut stream,
                    &request_id,
                    "TIMEOUT",
                    "open request timed out",
                )?;
                return Ok(());
            }
            Err(AppWaitError::PeerClosed | AppWaitError::ServerStopped) => {
                open_gate.store(false, Ordering::Release);
                return Ok(());
            }
            Err(AppWaitError::BufferLimit) => {
                open_gate.store(false, Ordering::Release);
                write_error_response(
                    &mut stream,
                    &request_id,
                    "INVALID_REQUEST",
                    "too much pipelined input",
                )?;
                return Ok(());
            }
            Err(AppWaitError::Io(err)) => return Err(err),
            Err(AppWaitError::Dispatch) => {
                open_gate.store(false, Ordering::Release);
                write_error_response(
                    &mut stream,
                    &request_id,
                    "TIMEOUT",
                    "open request could not be dispatched",
                )?;
                return Ok(());
            }
        };

        let opened = match validate_open_response(&response, &request_id, &owner) {
            Ok(opened) => opened,
            Err(OpenResponseError::AppError) => {
                write_text_line_allow_disconnect(&mut stream, &response)?;
                return Ok(());
            }
            Err(OpenResponseError::Invalid) => {
                write_error_response(
                    &mut stream,
                    &request_id,
                    "INVALID_REQUEST",
                    "invalid stream open response",
                )?;
                return Ok(());
            }
        };

        if let Err(err) = write_text_line_allow_disconnect(&mut stream, &response) {
            return Err(err);
        }
        open_gate.store(false, Ordering::Release);
        serve_operations(
            &mut stream,
            &request_id,
            &owner,
            opened.generation,
            api_tx,
            running,
            server_stop,
            &mut input,
            request_timeout,
        )
    })();

    open_gate.store(false, Ordering::Release);
    close_session(&request_id, &owner, api_tx);
    result
}

fn valid_target(params: &PaneInputIntentStreamParams) -> bool {
    match (&params.pane_id, &params.popup_terminal_id) {
        (Some(pane_id), None) => !pane_id.trim().is_empty(),
        (None, Some(terminal_id)) => !terminal_id.trim().is_empty(),
        _ => false,
    }
}

fn next_owner() -> String {
    let id = NEXT_STREAM_OWNER.fetch_add(1, Ordering::Relaxed);
    format!("pane.input_intent.stream:{}:{id}", std::process::id())
}

struct Opened {
    generation: u64,
}

enum OpenResponseError {
    AppError,
    Invalid,
}

fn validate_open_response(
    response: &str,
    request_id: &str,
    owner: &str,
) -> Result<Opened, OpenResponseError> {
    if response.contains('\n') || response.contains('\r') {
        return Err(OpenResponseError::Invalid);
    }
    let value = serde_json::from_str::<serde_json::Value>(response)
        .map_err(|_| OpenResponseError::Invalid)?;
    let object = value.as_object().ok_or(OpenResponseError::Invalid)?;
    if object.get("id").and_then(serde_json::Value::as_str) != Some(request_id) {
        return Err(OpenResponseError::Invalid);
    }
    if let Some(result) = object.get("result") {
        if object.len() != 2 {
            return Err(OpenResponseError::Invalid);
        }
        let result = result.as_object().ok_or(OpenResponseError::Invalid)?;
        if result.len() != 3
            || result.get("type").and_then(serde_json::Value::as_str)
                != Some("pane_input_intent_stream_opened")
        {
            return Err(OpenResponseError::Invalid);
        }
        let success =
            serde_json::from_value::<SuccessResponse>(value).map_err(|_| OpenResponseError::Invalid)?;
        return match success.result {
            ResponseResult::PaneInputIntentStreamOpened { session, generation }
                if session == owner && generation > 0 => Ok(Opened { generation }),
            _ => Err(OpenResponseError::Invalid),
        };
    }
    if let Some(error) = object.get("error") {
        if object.len() != 2 {
            return Err(OpenResponseError::Invalid);
        }
        let error = error.as_object().ok_or(OpenResponseError::Invalid)?;
        if error.len() == 2 && error.contains_key("code") && error.contains_key("message") {
            serde_json::from_value::<ErrorResponse>(value)
                .map_err(|_| OpenResponseError::Invalid)?;
            return Err(OpenResponseError::AppError);
        }
    }
    Err(OpenResponseError::Invalid)
}

fn serve_operations(
    stream: &mut LocalStream,
    request_id: &str,
    owner: &str,
    mut generation: u64,
    api_tx: &ApiRequestSender,
    running: &Arc<AtomicBool>,
    server_stop: Option<&Arc<AtomicBool>>,
    input: &mut InputBuffer,
    request_timeout: Duration,
) -> io::Result<()> {
    let mut operation_number = 0_u64;
    loop {
        let frame = match read_frame(stream, input, running, server_stop, request_timeout) {
            Ok(ReadFrame::Frame { line, started_at }) => (line, started_at),
            Ok(ReadFrame::Eof) => return Ok(()),
            Err(ReadFrameError::Timeout) => {
                write_ack_error(stream, generation, "TIMEOUT")?;
                return Ok(());
            }
            Err(ReadFrameError::TooLarge | ReadFrameError::Invalid) => {
                write_ack_error(stream, generation, "INVALID_REQUEST")?;
                return Ok(());
            }
            Err(ReadFrameError::Stopped) => return Ok(()),
            Err(ReadFrameError::Io(err)) => return Err(err),
        };
        let (line, started_at) = frame;
        let operation: InputIntentOperation = match serde_json::from_str(&line) {
            Ok(operation) => operation,
            Err(_) => {
                write_ack_error(stream, generation, "INVALID_REQUEST")?;
                return Ok(());
            }
        };
        let operation_name = operation_name(&operation);
        operation_number = operation_number.saturating_add(1);
        let operation_request = Request {
            id: format!("{request_id}:intent:{operation_number}"),
            method: Method::PaneInputIntentStreamOperation(
                PaneInputIntentStreamOperationParams {
                    session: owner.to_owned(),
                    operation,
                },
            ),
        };
        let gate = Arc::new(AtomicBool::new(true));
        let deadline = started_at + request_timeout;
        let response = match dispatch_request(
            operation_request,
            api_tx,
            stream,
            running,
            server_stop,
            input,
            deadline,
            Arc::clone(&gate),
        ) {
            Ok(response) => response,
            Err(AppWaitError::Timeout) => {
                gate.store(false, Ordering::Release);
                tracing::debug!(
                    request_id,
                    operation = operation_name,
                    generation,
                    outcome = "TIMEOUT",
                    "input intent operation failed"
                );
                write_ack_error(stream, generation, "TIMEOUT")?;
                return Ok(());
            }
            Err(AppWaitError::PeerClosed | AppWaitError::ServerStopped) => {
                gate.store(false, Ordering::Release);
                return Ok(());
            }
            Err(AppWaitError::BufferLimit) => {
                gate.store(false, Ordering::Release);
                write_ack_error(stream, generation, "INVALID_REQUEST")?;
                return Ok(());
            }
            Err(AppWaitError::Io(err)) => {
                gate.store(false, Ordering::Release);
                return Err(err);
            }
            Err(AppWaitError::Dispatch) => {
                gate.store(false, Ordering::Release);
                write_ack_error(stream, generation, "TIMEOUT")?;
                return Ok(());
            }
        };

        let ack = match parse_operation_ack(&response, owner, generation) {
            Ok(ack) => ack,
            Err(()) => {
                tracing::debug!(
                    request_id,
                    operation = operation_name,
                    generation,
                    outcome = "INVALID_REQUEST",
                    "input intent operation failed"
                );
                write_ack_error(stream, generation, "INVALID_REQUEST")?;
                return Ok(());
            }
        };
        generation = ack.generation;
        let outcome = if ack.ok {
            "recorded"
        } else {
            ack.error.as_deref().unwrap_or("INVALID_REQUEST")
        };
        tracing::debug!(request_id, operation = operation_name, generation, outcome, "input intent operation completed");
        if let Err(err) = write_text_line_allow_disconnect(stream, &response) {
            return Err(err);
        }
        if matches!(operation_name, "close") || (!ack.ok && ack.error.as_deref() == Some("TIMEOUT")) {
            return Ok(());
        }
    }
}

fn operation_name(operation: &InputIntentOperation) -> &'static str {
    match operation {
        InputIntentOperation::Enter {} => "enter",
        InputIntentOperation::Activate { .. } => "activate",
        InputIntentOperation::State { .. } => "state",
        InputIntentOperation::Blur {} => "blur",
        InputIntentOperation::Suspend {} => "suspend",
        InputIntentOperation::Resume { .. } => "resume",
        InputIntentOperation::Close {} => "close",
    }
}

fn parse_operation_ack(
    response: &str,
    owner: &str,
    previous_generation: u64,
) -> Result<OperationAck, ()> {
    if response.contains('\n') || response.contains('\r') {
        return Err(());
    }
    let value = serde_json::from_str::<serde_json::Value>(response).map_err(|_| ())?;
    let object = value.as_object().ok_or(())?;
    let success = object.get("ok").and_then(serde_json::Value::as_bool).ok_or(())?;
    let expected_keys: &[&str] = if success {
        &["ok", "generation", "session", "scope"]
    } else {
        &["ok", "generation", "error"]
    };
    if object.len() != expected_keys.len()
        || expected_keys.iter().any(|key| !object.contains_key(*key))
    {
        return Err(());
    }
    let ack = serde_json::from_value::<OperationAck>(value).map_err(|_| ())?;
    if ack.generation < previous_generation {
        return Err(());
    }
    if ack.ok {
        if ack.session.as_deref() != Some(owner)
            || ack.scope.as_deref() != Some("recorded")
            || ack.error.is_some()
        {
            return Err(());
        }
    } else if !matches!(
        ack.error.as_deref(),
        Some("INVALID_REQUEST" | "NO_LEASE" | "STALE_LEASE" | "TARGET_NOT_FOUND" | "TIMEOUT")
    ) {
        return Err(());
    }
    Ok(ack)
}

#[derive(Debug)]
enum ReadFrameError {
    Timeout,
    TooLarge,
    Invalid,
    Stopped,
    Io(io::Error),
}

fn read_frame(
    stream: &mut LocalStream,
    input: &mut InputBuffer,
    running: &AtomicBool,
    server_stop: Option<&Arc<AtomicBool>>,
    timeout: Duration,
) -> Result<ReadFrame, ReadFrameError> {
    let mut scratch = [0_u8; 1024];
    loop {
        if server_is_stopping(running, server_stop) {
            return if input.buffered_bytes == 0 {
                Ok(ReadFrame::Eof)
            } else {
                Err(ReadFrameError::Stopped)
            };
        }
        let deadline = input.front_received_at().map(|started_at| started_at + timeout);
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(ReadFrameError::Timeout);
        }
        match poll_read_into(stream, &mut scratch).map_err(ReadFrameError::Io)? {
            LocalStreamReadCount::Data(count) => {
                input
                    .push(&scratch[..count], Instant::now())
                    .map_err(|_| ReadFrameError::TooLarge)?;
                continue;
            }
            LocalStreamReadCount::Pending => {}
            LocalStreamReadCount::Closed => {
                return if input.first_line_length().is_some() || input.buffered_bytes == 0 {
                    Ok(ReadFrame::Eof)
                } else {
                    Err(ReadFrameError::Invalid)
                };
            }
        }
        let deadline = input.front_received_at().map(|started_at| started_at + timeout);
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(ReadFrameError::Timeout);
        }
        if let Some(length) = input.first_line_length() {
            if length > MAX_FRAME_BYTES {
                return Err(ReadFrameError::TooLarge);
            }
            let (line, started_at) = input.take_line(length);
            let mut line = String::from_utf8(line).map_err(|_| ReadFrameError::Invalid)?;
            if line.pop() != Some('\n') {
                return Err(ReadFrameError::Invalid);
            }
            return Ok(ReadFrame::Frame {
                line,
                started_at,
            });
        }
        if input.buffered_bytes > MAX_FRAME_BYTES {
            return Err(ReadFrameError::TooLarge);
        }
        std::thread::sleep(super::CONNECTION_POLL_INTERVAL);
    }
}

fn dispatch_request(
    request: Request,
    api_tx: &ApiRequestSender,
    stream: &mut LocalStream,
    running: &AtomicBool,
    server_stop: Option<&Arc<AtomicBool>>,
    input: &mut InputBuffer,
    deadline: Instant,
    active: Arc<AtomicBool>,
) -> Result<String, AppWaitError> {
    let (respond_to, response_rx) = std::sync::mpsc::channel();
    if api_tx
        .send(ApiRequestMessage {
            request,
            respond_to,
            response_write_complete: None,
            stream_active: Some(Arc::clone(&active)),
        })
        .is_err()
    {
        active.store(false, Ordering::Release);
        return Err(AppWaitError::Dispatch);
    }

    let mut scratch = [0_u8; 1024];
    loop {
        if server_is_stopping(running, server_stop) {
            active.store(false, Ordering::Release);
            return Err(AppWaitError::ServerStopped);
        }
        if Instant::now() >= deadline {
            active.store(false, Ordering::Release);
            return Err(AppWaitError::Timeout);
        }
        match response_rx.try_recv() {
            Ok(response) => return Ok(response),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                active.store(false, Ordering::Release);
                return Err(AppWaitError::Dispatch);
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
        }
        let read = match poll_read_into(stream, &mut scratch) {
            Ok(read) => read,
            Err(err) => {
                active.store(false, Ordering::Release);
                return Err(AppWaitError::Io(err));
            }
        };
        match read {
            LocalStreamReadCount::Data(count) => {
                if input.push(&scratch[..count], Instant::now()).is_err() {
                    active.store(false, Ordering::Release);
                    return Err(AppWaitError::BufferLimit);
                }
            }
            LocalStreamReadCount::Pending => {}
            LocalStreamReadCount::Closed => {
                if Instant::now() < deadline {
                    match response_rx.try_recv() {
                        Ok(response) => return Ok(response),
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            active.store(false, Ordering::Release);
                            return Err(AppWaitError::Dispatch);
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {}
                    }
                }
                active.store(false, Ordering::Release);
                return Err(AppWaitError::PeerClosed);
            }
        }
        let now = Instant::now();
        if now >= deadline {
            active.store(false, Ordering::Release);
            return Err(AppWaitError::Timeout);
        }
        std::thread::sleep(APP_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
    }
}

fn poll_read_into(stream: &mut LocalStream, scratch: &mut [u8]) -> io::Result<LocalStreamReadCount> {
    set_local_stream_polling(stream, true)?;
    let result = poll_local_stream_read_count(stream, scratch);
    let restore = set_local_stream_polling(stream, false);
    match (result, restore) {
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(err),
        (Ok(result), Ok(())) => Ok(result),
    }
}

fn server_is_stopping(running: &AtomicBool, server_stop: Option<&Arc<AtomicBool>>) -> bool {
    !running.load(Ordering::Acquire)
        || server_stop.is_some_and(|stop| stop.load(Ordering::Acquire))
}

fn write_error_response(
    stream: &mut LocalStream,
    request_id: &str,
    code: &str,
    message: &str,
) -> io::Result<()> {
    write_json_line_allow_disconnect(
        stream,
        &ErrorResponse {
            id: request_id.to_owned(),
            error: ErrorBody {
                code: code.to_owned(),
                message: message.to_owned(),
            },
        },
    )
}

fn write_ack_error(stream: &mut LocalStream, generation: u64, code: &str) -> io::Result<()> {
    let response = serde_json::json!({
        "ok": false,
        "generation": generation,
        "error": code,
    });
    write_text_line_allow_disconnect(stream, &response.to_string())
}

fn close_session(request_id: &str, owner: &str, api_tx: &ApiRequestSender) {
    let request = Request {
        id: format!("{request_id}:close"),
        method: Method::PaneInputIntentStreamClose(PaneInputIntentStreamOperationParams {
            session: owner.to_owned(),
            operation: InputIntentOperation::Close {},
        }),
    };
    let _response = super::dispatch_to_app_with_timeout(
        request,
        api_tx,
        Some(REQUEST_TIMEOUT),
    );
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use interprocess::local_socket::traits::Listener as _;
    use std::io::{BufRead, BufReader, Write};
    use tokio::sync::mpsc;


    static NEXT_TEST_SOCKET: AtomicU64 = AtomicU64::new(1);

    fn params() -> PaneInputIntentStreamParams {
        PaneInputIntentStreamParams {
            pane_id: Some("pane_alias".into()),
            popup_terminal_id: None,
            owner: String::new(),
        }
    }

    fn local_pair(_name: &str) -> (LocalStream, LocalStream) {
        let id = NEXT_TEST_SOCKET.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("hi-{}-{id}.sock", std::process::id()));
        let listener = crate::ipc::bind_local_listener(&path).unwrap();
        let client = crate::ipc::connect_local_stream(&path).unwrap();
        let server = listener.accept().unwrap();
        #[cfg(target_os = "macos")]
        {
            use std::os::fd::{AsFd as _, AsRawFd as _};
            // CLI tests may select process-wide SIGPIPE_DFL. Disconnect must
            // reach the stream's error/cleanup path rather than kill the suite.
            for stream in [&client, &server] {
                let LocalStream::UdSocket(socket) = stream;
                let enabled: libc::c_int = 1;
                assert_eq!(unsafe {
                    libc::setsockopt(socket.as_fd().as_raw_fd(), libc::SOL_SOCKET,
                        libc::SO_NOSIGPIPE, (&enabled as *const libc::c_int).cast(),
                        std::mem::size_of_val(&enabled) as libc::socklen_t)
                }, 0);
            }
        }
        std::fs::remove_file(path).unwrap();
        (client, server)
    }

    fn start_direct(
        timeout: Duration,
    ) -> (
        LocalStream,
        mpsc::UnboundedReceiver<ApiRequestMessage>,
        Arc<AtomicBool>,
        std::thread::JoinHandle<io::Result<()>>,
    ) {
        let (client, server) = local_pair("input-intent-stream");
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let running = Arc::new(AtomicBool::new(true));
        let server_running = Arc::clone(&running);
        let server = std::thread::spawn(move || {
            serve_with_timeout(
                server,
                "intent_open".into(),
                params(),
                &api_tx,
                &server_running,
                None,
                timeout,
            )
        });
        (client, api_rx, running, server)
    }

    fn start_public() -> (
        LocalStream,
        mpsc::UnboundedReceiver<ApiRequestMessage>,
        std::thread::JoinHandle<io::Result<()>>,
    ) {
        let (mut client, server) = local_pair("input-intent-public");
        client
            .write_all(
                br#"{"id":"intent_public","method":"pane.input_intent.stream","params":{"pane_id":"pane_alias"}}"#,
            )
            .unwrap();
        client.write_all(b"\n").unwrap();
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let running = Arc::new(AtomicBool::new(true));
        let event_hub = crate::api::EventHub::default();
        let server = std::thread::spawn(move || {
            super::super::handle_connection(server, &api_tx, &event_hub, &running, None)
        });
        (client, api_rx, server)
    }

    fn read_line(stream: &mut LocalStream) -> String {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        line
    }

    fn write_frame(stream: &mut LocalStream, frame: &[u8]) {
        stream.write_all(frame).unwrap();
        stream.flush().unwrap();
    }

    fn take_open(receiver: &mut mpsc::UnboundedReceiver<ApiRequestMessage>) -> (ApiRequestMessage, String) {
        let message = receiver.blocking_recv().unwrap();
        let owner = match &message.request.method {
            Method::PaneInputIntentStreamOpen(params) => {
                assert_eq!(params.pane_id.as_deref(), Some("pane_alias"));
                assert!(params.popup_terminal_id.is_none());
                assert!(params.owner.starts_with("pane.input_intent.stream:"));
                assert_ne!(params.owner, message.request.id);
                params.owner.clone()
            }
            other => panic!("expected stream open, got {other:?}"),
        };
        (message, owner)
    }

    fn respond_open(message: ApiRequestMessage, owner: &str, generation: u64) {
        let response = serde_json::to_string(&SuccessResponse {
            id: message.request.id.clone(),
            result: ResponseResult::PaneInputIntentStreamOpened {
                session: owner.to_owned(),
                generation,
            },
        })
        .unwrap();
        message.respond_to.send(response).unwrap();
    }

    fn open_direct(
        stream: &mut LocalStream,
        receiver: &mut mpsc::UnboundedReceiver<ApiRequestMessage>,
        generation: u64,
    ) -> String {
        let (message, owner) = take_open(receiver);
        respond_open(message, &owner, generation);
        let response: serde_json::Value = serde_json::from_str(&read_line(stream)).unwrap();
        assert_eq!(response["result"]["type"], "pane_input_intent_stream_opened");
        assert_eq!(response["result"]["session"], owner);
        assert_eq!(response["result"]["generation"], generation);
        owner
    }

    fn respond_recorded(message: ApiRequestMessage, session: &str, generation: u64) {
        message
            .respond_to
            .send(
                serde_json::json!({
                    "ok": true,
                    "generation": generation,
                    "session": session,
                    "scope": "recorded",
                })
                .to_string(),
            )
            .unwrap();
    }

    fn respond_cleanup(receiver: &mut mpsc::UnboundedReceiver<ApiRequestMessage>, owner: &str) {
        let message = receiver.blocking_recv().unwrap();
        match &message.request.method {
            Method::PaneInputIntentStreamClose(params) => assert_eq!(params.session, owner),
            other => panic!("expected session cleanup, got {other:?}"),
        }
        message.respond_to.send("{}".into()).unwrap();
    }

    #[test]
    fn idle_stream_survives_request_deadline_and_records_serial_operations() {
        let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(100));
        let owner = open_direct(&mut client, &mut receiver, 7);
        std::thread::sleep(Duration::from_millis(300));

        write_frame(&mut client, b"{\"op\":\"enter\"}\n");
        let enter = receiver.blocking_recv().unwrap();
        match &enter.request.method {
            Method::PaneInputIntentStreamOperation(params) => {
                assert_eq!(params.session, owner);
                assert!(matches!(&params.operation, InputIntentOperation::Enter {}));
            }
            other => panic!("expected enter operation, got {other:?}"),
        }
        respond_recorded(enter, &owner, 8);
        let enter_ack: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(enter_ack["ok"], true);
        assert_eq!(enter_ack["generation"], 8);
        assert_eq!(enter_ack["session"], owner);
        assert_eq!(enter_ack["scope"], "recorded");

        write_frame(&mut client, b"{\"op\":\"close\"}\n");
        let close = receiver.blocking_recv().unwrap();
        match &close.request.method {
            Method::PaneInputIntentStreamOperation(params) => {
                assert_eq!(params.session, owner);
                assert!(matches!(&params.operation, InputIntentOperation::Close {}));
            }
            other => panic!("expected close operation, got {other:?}"),
        }
        respond_recorded(close, &owner, 9);
        let close_ack: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(close_ack["generation"], 9);
        respond_cleanup(&mut receiver, &owner);
        assert!(server.join().unwrap().is_ok());
    }

    #[test]
    fn eof_cleans_only_the_server_allocated_session() {
        let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(100));
        let owner = open_direct(&mut client, &mut receiver, 2);
        drop(client);
        respond_cleanup(&mut receiver, &owner);
        assert!(server.join().unwrap().is_ok());
    }

    #[test]
    fn malformed_unknown_and_oversized_frames_are_rejected_before_dispatch() {
        let cases: Vec<Vec<u8>> = vec![
            b"{not-json}\n".to_vec(),
            {
                let mut frame = br#"{"op":"enter","unexpected":true}"#.to_vec();
                frame.push(b'\n');
                frame
            },
            {
                let mut frame = br#"{"op":"activate","policy":"mode"}"#.to_vec();
                frame.push(b'\n');
                frame
            },
            vec![b'x'; MAX_FRAME_BYTES + 1],
        ];
        for frame in cases {
            let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(100));
            let owner = open_direct(&mut client, &mut receiver, 11);
            write_frame(&mut client, &frame);
            if frame.last() != Some(&b'\n') {
                write_frame(&mut client, b"\n");
            }
            let error: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
            assert_eq!(error["ok"], false);
            assert_eq!(error["generation"], 11);
            assert_eq!(error["error"], "INVALID_REQUEST");
            respond_cleanup(&mut receiver, &owner);
            assert!(server.join().unwrap().is_ok());
        }
    }

    #[test]
    fn partial_frame_deadline_is_bounded_and_cleans_up() {
        let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(100));
        let owner = open_direct(&mut client, &mut receiver, 3);
        write_frame(&mut client, br#"{"op":"activate""#);
        let timeout: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(timeout["error"], "TIMEOUT");
        respond_cleanup(&mut receiver, &owner);
        assert!(server.join().unwrap().is_ok());
    }

    #[test]
    fn timed_out_open_gate_is_inactive_before_queued_open_can_mutate() {
        let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(100));
        let (message, owner) = take_open(&mut receiver);
        let gate = Arc::clone(message.stream_active.as_ref().unwrap());
        let timeout: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(timeout["error"]["code"], "TIMEOUT");
        assert!(!gate.load(Ordering::Acquire));

        let mutation_happened = gate.load(Ordering::Acquire);
        assert!(!mutation_happened);
        let late = serde_json::to_string(&SuccessResponse {
            id: message.request.id.clone(),
            result: ResponseResult::PaneInputIntentStreamOpened {
                session: owner.clone(),
                generation: 1,
            },
        })
        .unwrap();
        let _ = message.respond_to.send(late);
        respond_cleanup(&mut receiver, &owner);
        assert!(server.join().unwrap().is_ok());
    }

    #[test]
    fn timed_out_operation_gate_is_inactive_before_queued_operation_can_mutate() {
        let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(100));
        let owner = open_direct(&mut client, &mut receiver, 5);
        write_frame(
            &mut client,
            b"{\"op\":\"activate\",\"state\":\"command\",\"policy\":\"mode\"}\n",
        );
        let message = receiver.blocking_recv().unwrap();
        let gate = Arc::clone(message.stream_active.as_ref().unwrap());
        let timeout: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(timeout["error"], "TIMEOUT");
        assert!(!gate.load(Ordering::Acquire));

        let mutation_happened = gate.load(Ordering::Acquire);
        assert!(!mutation_happened);
        let _ = message.respond_to.send(
            serde_json::json!({
                "ok": true,
                "generation": 6,
                "session": owner,
                "scope": "recorded",
            })
            .to_string(),
        );
        respond_cleanup(&mut receiver, &owner);
        assert!(server.join().unwrap().is_ok());
    }

    #[test]
    fn eof_cancels_pending_operation_before_cleanup() {
        let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(200));
        let owner = open_direct(&mut client, &mut receiver, 4);
        write_frame(&mut client, b"{\"op\":\"enter\"}\n");
        let operation = receiver.blocking_recv().unwrap();
        let gate = Arc::clone(operation.stream_active.as_ref().unwrap());
        drop(client);
        respond_cleanup(&mut receiver, &owner);
        assert!(!gate.load(Ordering::Acquire));
        assert!(server.join().unwrap().is_ok());
    }
    #[test]
    fn pipelined_frame_followed_by_eof_cancels_pending_work() {
        let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(500));
        let owner = open_direct(&mut client, &mut receiver, 6);
        write_frame(&mut client, b"{\"op\":\"enter\"}\n");
        let pending = receiver.blocking_recv().unwrap();
        let gate = Arc::clone(pending.stream_active.as_ref().unwrap());

        write_frame(
            &mut client,
            b"{\"op\":\"activate\",\"state\":\"command\",\"policy\":\"mode\"}\n",
        );
        drop(client);
        respond_cleanup(&mut receiver, &owner);

        assert!(!gate.load(Ordering::Acquire));
        assert!(receiver.try_recv().is_err());
        assert!(server.join().unwrap().is_ok());
    }

    #[test]
    fn public_method_dispatch_opens_the_persistent_transport() {
        let (mut client, mut receiver, server) = start_public();
        let (open, owner) = take_open(&mut receiver);
        assert_eq!(open.request.id, "intent_public");
        respond_open(open, &owner, 1);
        let response: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(response["result"]["type"], "pane_input_intent_stream_opened");
        drop(client);
        respond_cleanup(&mut receiver, &owner);
        assert!(server.join().unwrap().is_ok());
    }
    #[test]
    fn server_stop_releases_idle_stream_without_hanging() {
        let (mut client, server_stream) = local_pair("stop");
        let (api_tx, mut api_rx) = mpsc::unbounded_channel();
        let running = Arc::new(AtomicBool::new(true));
        let server_running = Arc::clone(&running);
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = Arc::clone(&stop);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let result = serve_with_timeout(
                server_stream,
                "intent_stop".into(),
                params(),
                &api_tx,
                &server_running,
                Some(&server_stop),
                Duration::from_secs(1),
            );
            let _ = done_tx.send(result);
        });
        let owner = open_direct(&mut client, &mut api_rx, 1);
        stop.store(true, Ordering::Release);
        respond_cleanup(&mut api_rx, &owner);
        assert!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap().is_ok());
        server.join().unwrap();
    }

    #[test]
    fn server_stop_cancels_pending_operation_before_cleanup() {
        let (mut client, server_stream) = local_pair("stop-operation");
        let (api_tx, mut api_rx) = mpsc::unbounded_channel();
        let running = Arc::new(AtomicBool::new(true));
        let server_running = Arc::clone(&running);
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = Arc::clone(&stop);
        let server = std::thread::spawn(move || {
            serve_with_timeout(
                server_stream,
                "intent_stop_operation".into(),
                params(),
                &api_tx,
                &server_running,
                Some(&server_stop),
                Duration::from_secs(1),
            )
        });
        let owner = open_direct(&mut client, &mut api_rx, 1);
        write_frame(&mut client, b"{\"op\":\"enter\"}\n");
        let operation = api_rx.blocking_recv().unwrap();
        let gate = Arc::clone(operation.stream_active.as_ref().unwrap());
        stop.store(true, Ordering::Release);
        respond_cleanup(&mut api_rx, &owner);
        assert!(!gate.load(Ordering::Acquire));
        assert!(server.join().unwrap().is_ok());
    }

    #[test]
    fn invalid_open_target_is_rejected_without_app_dispatch() {
        let (mut client, server_stream) = local_pair("invalid-target");
        let (api_tx, mut api_rx) = mpsc::unbounded_channel();
        let running = Arc::new(AtomicBool::new(true));
        let server_running = Arc::clone(&running);
        let mut params = params();
        params.popup_terminal_id = Some("popup_terminal".into());
        let server = std::thread::spawn(move || {
            serve_with_timeout(
                server_stream,
                "invalid_target".into(),
                params,
                &api_tx,
                &server_running,
                None,
                Duration::from_millis(100),
            )
        });
        let error: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(error["error"]["code"], "INVALID_REQUEST");
        assert!(server.join().unwrap().is_ok());
        assert!(api_rx.try_recv().is_err());
    }
    #[test]
    fn unknown_state_reaches_app_validation() {
        let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(200));
        let owner = open_direct(&mut client, &mut receiver, 4);
        write_frame(
            &mut client,
            b"{\"op\":\"activate\",\"state\":\"future\",\"policy\":\"mode\"}\n",
        );
        let operation = receiver.blocking_recv().unwrap();
        match &operation.request.method {
            Method::PaneInputIntentStreamOperation(params) => match &params.operation {
                InputIntentOperation::Activate { state, policy } => {
                    assert_eq!(*state, crate::api::schema::InputIntentState::Unknown);
                    assert_eq!(*policy, crate::api::schema::InputIntentPolicy::Mode);
                }
                other => panic!("expected activate, got {other:?}"),
            },
            other => panic!("expected operation request, got {other:?}"),
        }
        operation
            .respond_to
            .send(
                serde_json::json!({
                    "ok": false,
                    "generation": 4,
                    "error": "INVALID_REQUEST",
                })
                .to_string(),
            )
            .unwrap();
        let ack: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(ack["error"], "INVALID_REQUEST");
        drop(client);
        respond_cleanup(&mut receiver, &owner);
        assert!(server.join().unwrap().is_ok());
    }

    #[test]
    fn disconnect_during_ack_write_still_cleans_owned_session() {
        let (mut client, mut receiver, _, server) = start_direct(Duration::from_secs(1));
        let owner = open_direct(&mut client, &mut receiver, 1);
        write_frame(&mut client, b"{\"op\":\"enter\"}\n");
        let operation = receiver.blocking_recv().unwrap();
        respond_recorded(operation, &owner, 2);
        drop(client);
        respond_cleanup(&mut receiver, &owner);
        assert!(server.join().unwrap().is_ok());
    }
    #[test]
    fn oversized_public_open_frame_is_rejected_before_app_dispatch() {
        let (mut client, server_stream) = local_pair("oversized-open");
        let request = serde_json::json!({
            "id": "x".repeat(MAX_FRAME_BYTES),
            "method": "pane.input_intent.stream",
            "params": { "pane_id": "pane_alias" },
        });
        client
            .write_all(&serde_json::to_vec(&request).unwrap())
            .unwrap();
        client.write_all(b"\n").unwrap();
        let (api_tx, mut api_rx) = mpsc::unbounded_channel();
        let running = Arc::new(AtomicBool::new(true));
        let event_hub = crate::api::EventHub::default();
        let server = std::thread::spawn(move || {
            super::super::handle_connection(
                server_stream,
                &api_tx,
                &event_hub,
                &running,
                None,
            )
        });
        let error: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(error["error"]["code"], "INVALID_REQUEST");
        assert!(server.join().unwrap().is_ok());
        assert!(api_rx.try_recv().is_err());
    }
    #[test]
    fn mismatched_session_or_decreasing_generation_never_reaches_client_as_success() {
        for (name, generation) in [("session", 6), ("generation", 4)] {
            let (mut client, mut receiver, _, server) = start_direct(Duration::from_millis(300));
            let owner = open_direct(&mut client, &mut receiver, 5);
            write_frame(&mut client, b"{\"op\":\"enter\"}\n");
            let operation = receiver.blocking_recv().unwrap();
            let session = if name == "session" {
                "different-session"
            } else {
                owner.as_str()
            };
            operation
                .respond_to
                .send(
                    serde_json::json!({
                        "ok": true,
                        "generation": generation,
                        "session": session,
                        "scope": "recorded",
                    })
                    .to_string(),
                )
                .unwrap();
            let error: serde_json::Value =
                serde_json::from_str(&read_line(&mut client)).unwrap();
            assert_eq!(error["ok"], false);
            assert_eq!(error["generation"], 5);
            assert_eq!(error["error"], "INVALID_REQUEST");
            drop(client);
            respond_cleanup(&mut receiver, &owner);
            assert!(server.join().unwrap().is_ok());
        }
    }
    #[test]
    fn public_open_rejects_unknown_parameter_fields_without_dispatch() {
        let (mut client, server_stream) = local_pair("extra-open-field");
        let request = serde_json::json!({
            "id": "intent_extra",
            "method": "pane.input_intent.stream",
            "params": { "pane_id": "pane_alias", "unexpected": true },
        });
        client
            .write_all(&serde_json::to_vec(&request).unwrap())
            .unwrap();
        client.write_all(b"\n").unwrap();
        let (api_tx, mut api_rx) = mpsc::unbounded_channel();
        let running = Arc::new(AtomicBool::new(true));
        let event_hub = crate::api::EventHub::default();
        let server = std::thread::spawn(move || {
            super::super::handle_connection(
                server_stream,
                &api_tx,
                &event_hub,
                &running,
                None,
            )
        });
        let error: serde_json::Value = serde_json::from_str(&read_line(&mut client)).unwrap();
        assert_eq!(error["error"]["code"], "INVALID_REQUEST");
        assert!(server.join().unwrap().is_ok());
        assert!(api_rx.try_recv().is_err());
    }
}
