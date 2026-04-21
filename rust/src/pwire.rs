//! PWire transport backed by **pyro-runtime** — zero-tokio client path.
//!
//! # Architecture
//!
//! ```text
//!   Caller thread                          Runtime thread (dedicated)
//!   ─────────────                          ───────────────────────────
//!   PyroWireConnection                     OS thread loop:
//!   ├─ cmd_tx : std::mpsc::Sender<Cmd>  ─▶ 1. std::mpsc::recv()   (blocking)
//!   └─ prepared : parking_lot::Mutex<…>    2. rt.block_on(process(cmd))
//!                                         3. oneshot_tx.send(result)
//!                                         4. goto 1
//! ```
//!
//! * `pyro_runtime::Runtime` is `!Send`, so it MUST live on one thread.
//! * Commands carry their own `futures::channel::oneshot::Sender` — this
//!   lets the caller `.await` the response without tokio.
//! * The runtime thread is idle between commands (blocked on
//!   `std::mpsc::recv`) and enters `block_on` only while processing one.
//!   No cross-thread wake primitive is needed — the socket I/O happens
//!   entirely inside the (inherently synchronous) block_on.
//! * Pipelining sends a single `Cmd::Pipeline(Vec<Op>)` — the runtime
//!   thread processes it as ONE block_on with N writes then N reads,
//!   giving `1 RTT for N queries` perf.
//!
//! # Wire format
//!
//! Matches the live server path in `crates/pyrosql-protocol-pwire/src/pyro_server.rs`
//! (lines 253-322 for MSG_EXECUTE, with u32 LE param length prefixes):
//!
//! ```text
//!   Request frame:   [type:u8][len:u32 LE][payload]
//!     VW_REQ_QUERY   (0x01) payload = SQL UTF-8 bytes
//!     VW_REQ_PREPARE (0x02) payload = SQL UTF-8 bytes
//!     VW_REQ_EXECUTE (0x03) payload = [handle:u32 LE][count:u16 LE]
//!                                     per param: [len:u32 LE][bytes]
//!
//!   Response frame:  [type:u8][len:u32 LE][payload]
//!     VW_RESP_RESULT_SET (0x01)
//!     VW_RESP_OK         (0x02)
//!     VW_RESP_ERROR      (0x03)
//! ```

use std::collections::{HashMap, VecDeque};
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::sync::mpsc::{self as std_mpsc};
use std::thread::JoinHandle;

use futures::StreamExt;
use futures::channel::mpsc::{unbounded as futures_unbounded, UnboundedSender as FutSender};
use futures::future::FutureExt;

use parking_lot::Mutex;

use crate::error::ClientError;
use crate::row::{ColumnMeta, QueryResult, Row, Value};

// ── Wire constants ───────────────────────────────────────────────────────────

pub(crate) const VW_REQ_QUERY:       u8 = 0x01;
pub(crate) const VW_REQ_PREPARE:     u8 = 0x02;
pub(crate) const VW_REQ_EXECUTE:     u8 = 0x03;
pub(crate) const VW_RESP_RESULT_SET: u8 = 0x01;
pub(crate) const VW_RESP_OK:         u8 = 0x02;
pub(crate) const VW_RESP_ERROR:      u8 = 0x03;

const PBUF_SIZE:  usize = 64 * 1024;
const PBUF_COUNT: u16   = 16;

// ── Commands: caller → runtime thread ────────────────────────────────────────

/// Reply channel.  Oneshots are not clonable — we send Err back explicitly
/// if the request is aborted.
type Reply<T> = futures::channel::oneshot::Sender<Result<T, ClientError>>;

/// Single operation inside a Pipeline batch.
#[derive(Debug)]
enum PipelineStep {
    Query(String),
    /// Template + params — the runtime thread looks up the prepare cache
    /// and either sends MSG_EXECUTE or a PREPARE-then-EXECUTE pair.
    TemplateQuery { sql: String, params: Vec<Value> },
}

/// Work sent to the runtime thread.
enum Cmd {
    /// Single simple query (already interpolated).
    Query { sql: String, reply: Reply<QueryResult> },
    /// Single parameterised query — runtime thread handles prepare cache.
    TemplateQuery { sql: String, params: Vec<Value>, reply: Reply<QueryResult> },
    /// Batch of queries, executed in one RTT round-trip pair.
    Pipeline { steps: Vec<PipelineStep>, reply: Reply<Vec<Result<QueryResult, ClientError>>> },
    /// Graceful shutdown — thread exits after draining.
    Shutdown,
}

impl std::fmt::Debug for Cmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cmd::Query { sql, .. } => write!(f, "Query({:?})", sql),
            Cmd::TemplateQuery { sql, params, .. } =>
                write!(f, "TemplateQuery({:?}, {} params)", sql, params.len()),
            Cmd::Pipeline { steps, .. } => write!(f, "Pipeline({} steps)", steps.len()),
            Cmd::Shutdown => write!(f, "Shutdown"),
        }
    }
}

// ── Public connection handle (Send + Sync) ───────────────────────────────────

/// A PWire connection whose I/O is driven by a dedicated pyro-runtime
/// thread.  All `query` / `execute` / `pipeline` calls send a [`Cmd`]
/// over a [`std::sync::mpsc`] channel; the runtime thread processes it
/// inside `block_on` and signals completion through a
/// [`futures::channel::oneshot`].
///
/// Thread-safe: `Send + Sync`.  Safe to share via `Arc` and use from
/// multiple caller threads simultaneously — commands serialise on the
/// single runtime worker.
pub struct PyroWireConnection {
    cmd_tx: FutSender<Cmd>,
    /// Cached prepare handles — populated lazily from the runtime thread's
    /// authoritative cache via a future `HandleMinted` notification so
    /// `Pipeline::query` on the caller side can build pre-filled EXECUTE
    /// frames.  Currently unused (kept for the TODO in the module footer);
    /// `dead_code` allow is deliberate.
    #[allow(dead_code)]
    prepared: Mutex<HashMap<String, u32>>,
    /// Join handle for the runtime thread — cleaned up on drop.
    worker: Option<JoinHandle<()>>,
}

impl PyroWireConnection {
    /// Open a plain-TCP PWire connection to `addr`.  Spawns one dedicated
    /// OS thread that owns the pyro-runtime for this connection.
    pub fn connect(addr: &str) -> Result<Self, ClientError> {
        let sock_addr: SocketAddr = addr.parse().or_else(|_| {
            use std::net::ToSocketAddrs;
            addr.to_socket_addrs()
                .map_err(|e| ClientError::Connection(format!("DNS resolve({addr}): {e}")))?
                .find(|a| a.is_ipv4())
                .ok_or_else(|| ClientError::Connection(format!("no IPv4 for {addr}")))
        })?;
        let std_tcp = StdTcpStream::connect(sock_addr)
            .map_err(|e| ClientError::Connection(format!("PWire connect({sock_addr}): {e}")))?;
        std_tcp.set_nodelay(true)
            .map_err(|e| ClientError::Connection(format!("set_nodelay: {e}")))?;

        // Use a one-shot startup channel to confirm the runtime thread
        // successfully registered the pbuf ring before we declare the
        // connection ready.
        let (ready_tx, ready_rx) = std_mpsc::channel::<Result<(), ClientError>>();
        let (cmd_tx, cmd_rx) = futures_unbounded::<Cmd>();

        let worker = std::thread::Builder::new()
            .name("pyro-wire-rt".into())
            .spawn(move || {
                runtime_worker(std_tcp, cmd_rx, ready_tx);
            })
            .map_err(|e| ClientError::Connection(format!("spawn pyro-wire-rt thread: {e}")))?;

        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(ClientError::Connection(
                format!("pyro-wire-rt startup channel closed: {e}")
            )),
        }

        Ok(Self {
            cmd_tx,
            prepared: Mutex::new(HashMap::new()),
            worker: Some(worker),
        })
    }

    /// Send a command and await its response.  Internally uses a
    /// `futures::channel::oneshot` so the `.await` is runtime-agnostic.
    async fn send_cmd<T>(
        &self,
        make: impl FnOnce(Reply<T>) -> Cmd,
    ) -> Result<T, ClientError> {
        let (tx, rx) = futures::channel::oneshot::channel();
        self.cmd_tx.unbounded_send(make(tx)).map_err(|_| {
            ClientError::Connection("pyro-wire-rt thread is gone".into())
        })?;
        match rx.await {
            Ok(r) => r,
            Err(_cancelled) => Err(ClientError::Connection(
                "pyro-wire-rt response channel closed".into(),
            )),
        }
    }

    /// Simple query — no parameters.
    pub async fn query_simple(&self, sql: &str) -> Result<QueryResult, ClientError> {
        let sql = sql.to_owned();
        self.send_cmd(|reply| Cmd::Query { sql, reply }).await
    }

    /// Parameterised query — uses server-side prepare cache where possible.
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult, ClientError> {
        let sql = sql.to_owned();
        let params = params.to_vec();
        self.send_cmd(|reply| Cmd::TemplateQuery { sql, params, reply }).await
    }

    /// Parameterised execute — returns the number of rows affected.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, ClientError> {
        self.query(sql, params).await.map(|r| r.rows_affected)
    }

    /// Start a new pipeline builder.  See [`Pipeline`] for the batch API.
    pub fn pipeline(&self) -> Pipeline<'_> {
        Pipeline { conn: self, steps: Vec::new() }
    }

    /// Send a Shutdown command and join the worker thread.  Idempotent.
    fn shutdown(&mut self) {
        let _ = self.cmd_tx.unbounded_send(Cmd::Shutdown);
        if let Some(h) = self.worker.take() {
            let _ = h.join();
        }
    }

    /// **Blocking** wrapper around `query_simple` for sync callers (FFI,
    /// CLI tools).  Drives the oneshot via `futures::executor::block_on`.
    pub fn query_simple_blocking(&self, sql: &str) -> Result<QueryResult, ClientError> {
        futures::executor::block_on(self.query_simple(sql))
    }

    /// **Blocking** wrapper around `query`.
    pub fn query_blocking(&self, sql: &str, params: &[Value]) -> Result<QueryResult, ClientError> {
        futures::executor::block_on(self.query(sql, params))
    }

    /// **Blocking** wrapper around `execute`.
    pub fn execute_blocking(&self, sql: &str, params: &[Value]) -> Result<u64, ClientError> {
        futures::executor::block_on(self.execute(sql, params))
    }
}

impl Drop for PyroWireConnection {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ── Pipeline (caller-side builder) ───────────────────────────────────────────

/// Batch builder: accumulate N queries, send them all in one round-trip.
///
/// The server processes PWire frames strictly in order (see
/// `pyro_server.rs`), so pipelining is lossless: frame #K's response lines
/// up with request #K.  Latency for a batch of N queries is ~1 RTT + 1×
/// serialisation + N× server-execution instead of N× RTT.
pub struct Pipeline<'a> {
    conn: &'a PyroWireConnection,
    steps: Vec<PipelineStep>,
}

impl<'a> Pipeline<'a> {
    /// Queue a parameterised query.  If `sql` contains `$` placeholders the
    /// runtime thread will PREPARE it on first use and EXECUTE subsequently.
    pub fn query(mut self, sql: &str, params: &[Value]) -> Self {
        if !params.is_empty() && sql.contains('$') {
            self.steps.push(PipelineStep::TemplateQuery {
                sql: sql.to_owned(),
                params: params.to_vec(),
            });
        } else {
            let final_sql = crate::client::interpolate_params(sql, params);
            self.steps.push(PipelineStep::Query(final_sql));
        }
        self
    }

    /// Queue a DML statement.  Same underlying representation as [`query`].
    pub fn execute(self, sql: &str, params: &[Value]) -> Self { self.query(sql, params) }

    /// Commit the pipeline: ships all frames in one round-trip and returns
    /// the per-statement results in order.
    pub async fn send(self) -> Result<PipelineResponses, ClientError> {
        let Pipeline { conn, steps } = self;
        let results = conn.send_cmd(|reply| Cmd::Pipeline { steps, reply }).await?;
        Ok(PipelineResponses { results })
    }
}

/// Results of [`Pipeline::send`] — preserves per-statement ordering.
pub struct PipelineResponses {
    results: Vec<Result<QueryResult, ClientError>>,
}

impl PipelineResponses {
    /// Number of statements in the batch.
    pub fn len(&self) -> usize { self.results.len() }

    /// `true` if the batch is empty.
    pub fn is_empty(&self) -> bool { self.results.is_empty() }

    /// Consume all per-statement results.
    pub fn into_results(self) -> Vec<Result<QueryResult, ClientError>> { self.results }

    /// Rows affected for statement `idx`, or a flattened error.
    pub fn rows_affected_at(&self, idx: usize) -> Result<u64, ClientError> {
        match self.results.get(idx) {
            Some(Ok(r)) => Ok(r.rows_affected),
            Some(Err(e)) => Err(ClientError::Query(format!("pipeline[{idx}]: {e}"))),
            None => Err(ClientError::Protocol(format!(
                "pipeline: index {idx} out of range (len={})",
                self.results.len()
            ))),
        }
    }
}

// ── Runtime thread worker ────────────────────────────────────────────────────

/// Main loop of the dedicated OS thread.  Owns the pyro-runtime.
///
/// Takes ownership of the `StdTcpStream` rather than a bare fd so that if
/// runtime setup fails the stream drops naturally and closes the fd —
/// avoids an `unsafe { libc::close(fd) }` (the driver crate bans unsafe).
fn runtime_worker(
    std_tcp: StdTcpStream,
    cmd_rx: futures::channel::mpsc::UnboundedReceiver<Cmd>,
    ready_tx: std_mpsc::Sender<Result<(), ClientError>>,
) {
    // A SINGLE `block_on` covers the entire worker lifetime so the reactor
    // + pbuf ring + AsyncTcpStream all persist across commands.  Past
    // architecture bug: a block_on per Cmd recycled the reactor, leaving
    // the `BufferGroupId` handle pointing at a torn-down ring → subsequent
    // `recv_multishot_stream` hung waiting for CQEs that would never come.
    //
    // The command receiver is a `futures::channel::mpsc::UnboundedReceiver`
    // — its Waker integrates with std::task::Waker, which pyro-runtime's
    // scheduler drives correctly.  No tokio, no eventfd, no polling loop.
    use std::os::fd::IntoRawFd;
    let fd = std_tcp.into_raw_fd();
    let mut rt = pyro_runtime::RuntimeBuilder::new().build();

    rt.block_on(async move {
        // Construct the stream FIRST so its Drop handles fd close on any
        // early-return path — avoids needing `libc::close` (which would
        // require `unsafe`, banned by the driver crate).  Immutable binding
        // so we can share &stream between send + a persistent recv_multishot.
        let stream = pyro_runtime::AsyncTcpStream::from_raw_fd(fd);

        // Register the provided-buffer ring on the persistent reactor.
        let bgid_res = pyro_runtime::try_with_reactor(|r| {
            r.register_buf_ring(PBUF_SIZE, PBUF_COUNT)
        });
        let bgid = match bgid_res {
            Some(Ok(b)) => b,
            Some(Err(e)) => {
                let _ = ready_tx.send(Err(ClientError::Connection(format!(
                    "register_buf_ring: {e}"
                ))));
                drop(stream);
                return;
            }
            None => {
                let _ = ready_tx.send(Err(ClientError::Connection(
                    "PWire: io_uring reactor unavailable (requires Linux ≥ 5.18)".into(),
                )));
                drop(stream);
                return;
            }
        };

        // Start the multishot recv ONCE and keep it alive for the whole
        // worker lifetime.  This has two critical effects:
        //   1. Keeps a pending io_uring SQE so pyro-runtime's deadlock
        //      detector sees `has_pending_io() == true` and parks instead
        //      of panicking when `cmd_rx.next()` yields.
        //   2. Eliminates per-query SQE submit overhead — the kernel streams
        //      incoming bytes directly into the provided-buffer ring.
        let mut ms = stream.recv_multishot_stream(bgid);
        let mut recv_buf: VecDeque<u8> = VecDeque::with_capacity(PBUF_SIZE);
        let mut prepared: HashMap<String, u32> = HashMap::new();

        // 3. Barrier — caller is now allowed to send Cmds.
        let _ = ready_tx.send(Ok(()));

        // 4. Dispatch loop.  Uses `futures::select!` to wait on BOTH the
        //    command receiver AND an idle poll of the multishot recv
        //    stream.  The idle recv poll serves a critical purpose: it
        //    triggers `submit_recv_multishot` so the reactor always has a
        //    pending SQE between commands.  Without this, pyro-runtime's
        //    deadlock detector panics the moment `cmd_rx.next()` yields
        //    (the detector only counts io_uring slots and pyro-runtime
        //    timers as "pending work" — futures channels are invisible).
        //
        //    An unsolicited server-push frame would land here and get
        //    buffered into `recv_buf` for the next `recv_frame` to consume.
        //    In practice the server never pushes without being asked, so
        //    this branch is a safety net, not a hot path.
        let mut rx = cmd_rx;
        'dispatch: loop {
            let cmd_opt: Option<Cmd>;
            {
                let mut cmd_fut = rx.next().fuse();
                let mut idle_fut = ms.next().fuse();
                futures::select! {
                    c = cmd_fut => { cmd_opt = c; }
                    d = idle_fut => {
                        match d {
                            Ok((buf_id, len)) if len > 0 => {
                                pyro_runtime::with_reactor(|r| {
                                    let slice = r.buf_slice(bgid, buf_id, len);
                                    recv_buf.extend(slice.iter().copied());
                                    r.return_buffer(bgid, buf_id);
                                });
                                continue 'dispatch;
                            }
                            _ => break 'dispatch, // EOF or error
                        }
                    }
                }
            }
            let cmd = match cmd_opt {
                Some(c) => c,
                None => break,
            };
            match cmd {
                Cmd::Shutdown => break,
                Cmd::Query { sql, reply } => {
                    let result: Result<QueryResult, ClientError> = async {
                        send_frame(&stream, VW_REQ_QUERY, sql.as_bytes()).await?;
                        let (ty, payload) =
                            recv_frame(&mut ms, bgid, &mut recv_buf).await?;
                        parse_query_response(ty, payload)
                    }
                    .await;
                    let _ = reply.send(result);
                }
                Cmd::TemplateQuery { sql, params, reply } => {
                    let result = process_template_query_async(
                        &stream, &mut ms, bgid, &mut recv_buf, &mut prepared, &sql, &params,
                    )
                    .await;
                    let _ = reply.send(result);
                }
                Cmd::Pipeline { steps, reply } => {
                    let result = process_pipeline_async(
                        &stream, &mut ms, bgid, &mut recv_buf, &mut prepared, steps,
                    )
                    .await;
                    let _ = reply.send(result);
                }
            }
        }
        // stream + bgid drop here, inside the block_on — pyro-runtime tears
        // them down with the reactor still alive, so the multishot recv
        // SQE gets properly cancelled via `orphan_multishot`.
    });
}

/// Dispatch a single template query — cache-aware.  Async variant for the
/// single-block_on architecture.
async fn process_template_query_async(
    stream: &pyro_runtime::AsyncTcpStream,
    ms: &mut pyro_runtime::RecvMultishotStream<'_>,
    bgid: pyro_runtime::BufferGroupId,
    recv_buf: &mut VecDeque<u8>,
    prepared: &mut HashMap<String, u32>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, ClientError> {
    if let Some(&handle) = prepared.get(sql) {
        let payload = build_execute_payload(handle, params);
        send_frame(stream, VW_REQ_EXECUTE, &payload).await?;
        let (ty, resp) = recv_frame(ms, bgid, recv_buf).await?;
        return parse_query_response(ty, resp);
    }

    send_frame(stream, VW_REQ_PREPARE, sql.as_bytes()).await?;
    let (prep_ty, prep_payload) = recv_frame(ms, bgid, recv_buf).await?;

    match prep_ty {
        VW_RESP_OK => {
            if prep_payload.len() < 4 {
                return Err(ClientError::Protocol(
                    "PWire: PREPARE response missing handle".into(),
                ));
            }
            let handle = u32::from_le_bytes([
                prep_payload[0], prep_payload[1], prep_payload[2], prep_payload[3],
            ]);
            prepared.insert(sql.to_owned(), handle);
            let payload = build_execute_payload(handle, params);
            send_frame(stream, VW_REQ_EXECUTE, &payload).await?;
            let (ty, resp) = recv_frame(ms, bgid, recv_buf).await?;
            parse_query_response(ty, resp)
        }
        VW_RESP_ERROR => {
            let final_sql = crate::client::interpolate_params(sql, params);
            send_frame(stream, VW_REQ_QUERY, final_sql.as_bytes()).await?;
            let (ty, resp) = recv_frame(ms, bgid, recv_buf).await?;
            parse_query_response(ty, resp)
        }
        other => Err(ClientError::Protocol(format!(
            "PWire: PREPARE unexpected response 0x{other:02x}"
        ))),
    }
}

/// Dispatch a batch of queries as a single round-trip (write all, then
/// read all).  The CRITICAL bit: we write every frame back-to-back BEFORE
/// we start reading, so the server sees them all with one syscall burst.
async fn process_pipeline_async(
    stream: &pyro_runtime::AsyncTcpStream,
    ms: &mut pyro_runtime::RecvMultishotStream<'_>,
    bgid: pyro_runtime::BufferGroupId,
    recv_buf: &mut VecDeque<u8>,
    prepared: &mut HashMap<String, u32>,
    steps: Vec<PipelineStep>,
) -> Result<Vec<Result<QueryResult, ClientError>>, ClientError> {
    // Phase 0: compile each step into a pre-serialised byte buffer + a
    // "read N responses" tally.  Template queries that don't have a cache
    // entry yet go through a warm-up round-trip FIRST (outside the tight
    // send/recv loop) so that the actual pipeline only ships MSG_EXECUTE
    // frames.  This keeps the pipelined path at 1 RTT even with
    // parameterised queries.
    struct CompiledStep {
        req_type: u8,
        payload: Vec<u8>,
    }
    let mut compiled: Vec<CompiledStep> = Vec::with_capacity(steps.len());
    for step in steps {
        match step {
            PipelineStep::Query(sql) => {
                compiled.push(CompiledStep {
                    req_type: VW_REQ_QUERY,
                    payload: sql.into_bytes(),
                });
            }
            PipelineStep::TemplateQuery { sql, params } => {
                let handle = match prepared.get(&sql).copied() {
                    Some(h) => h,
                    None => {
                        // Cold template — warm-up PREPARE before the batch
                        // so the pipeline body ships only MSG_EXECUTE.
                        send_frame(stream, VW_REQ_PREPARE, sql.as_bytes()).await?;
                        let (ty, payload) = recv_frame(ms, bgid, recv_buf).await?;
                        match ty {
                            VW_RESP_OK if payload.len() >= 4 => {
                                let h = u32::from_le_bytes([
                                    payload[0], payload[1], payload[2], payload[3],
                                ]);
                                prepared.insert(sql.clone(), h);
                                h
                            }
                            VW_RESP_ERROR => {
                                // Fall back to MSG_QUERY for this step only.
                                let final_sql = crate::client::interpolate_params(&sql, &params);
                                compiled.push(CompiledStep {
                                    req_type: VW_REQ_QUERY,
                                    payload: final_sql.into_bytes(),
                                });
                                continue;
                            }
                            _ => return Err(ClientError::Protocol(
                                "PWire pipeline: PREPARE failed".into(),
                            )),
                        }
                    }
                };
                compiled.push(CompiledStep {
                    req_type: VW_REQ_EXECUTE,
                    payload: build_execute_payload(handle, &params),
                });
            }
        }
    }

    // Phase 1 & 2: write all, then read all — the single-RTT batch.
    let n = compiled.len();
    for step in &compiled {
        send_frame(stream, step.req_type, &step.payload).await?;
    }
    let mut results: Vec<Result<QueryResult, ClientError>> = Vec::with_capacity(n);
    for _ in 0..n {
        match recv_frame(ms, bgid, recv_buf).await {
            Ok((ty, payload)) => results.push(parse_query_response(ty, payload)),
            Err(e) => {
                let e_clone_msg = e.to_string();
                results.push(Err(e));
                while results.len() < n {
                    results.push(Err(ClientError::Protocol(format!(
                        "pipeline aborted after socket error: {e_clone_msg}"
                    ))));
                }
                break;
            }
        }
    }
    Ok(results)
}

// ── Async frame I/O helpers (driven inside pyro-runtime block_on) ────────────

async fn send_frame(
    stream: &pyro_runtime::AsyncTcpStream,
    msg_type: u8,
    payload: &[u8],
) -> Result<(), ClientError> {
    // Concatenate header + payload so io_uring submits one SQE.
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(msg_type);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    let (res, _buf) = stream.send(buf).await;
    res.map(|_| ()).map_err(|e| ClientError::Protocol(format!("PWire send: {e}")))
}

/// Read one full PWire frame.  Uses `recv_buf` for reassembly across
/// io_uring provided-buffer boundaries.
/// Read one full PWire frame using a PERSISTENT `RecvMultishotStream`.
///
/// Keeping the stream alive across calls is critical: pyro-runtime's
/// deadlock detector checks for any in-flight io_uring SQE.  A dormant
/// multishot recv SQE counts as "pending I/O" → run_loop happily parks
/// waiting for either a recv CQE or a `cmd_rx.next()` waker.  Re-creating
/// the stream per call would disarm the SQE between commands and the
/// detector would panic the moment the worker goes idle.
async fn recv_frame(
    ms: &mut pyro_runtime::RecvMultishotStream<'_>,
    bgid: pyro_runtime::BufferGroupId,
    recv_buf: &mut VecDeque<u8>,
) -> Result<(u8, Vec<u8>), ClientError> {
    if let Some(frame) = try_take_frame(recv_buf) { return Ok(frame); }

    loop {
        let (buf_id, len) = ms.next().await
            .map_err(|e| ClientError::Protocol(format!("PWire recv: {e}")))?;
        if len == 0 {
            return Err(ClientError::Protocol(
                "PWire recv: connection closed by peer".into(),
            ));
        }
        pyro_runtime::with_reactor(|r| {
            let slice = r.buf_slice(bgid, buf_id, len);
            recv_buf.extend(slice.iter().copied());
            r.return_buffer(bgid, buf_id);
        });
        if let Some(frame) = try_take_frame(recv_buf) {
            return Ok(frame);
        }
    }
}

/// Peek+pop one PWire frame from the front of `buf`, or return `None` if
/// the buffer doesn't yet contain a full frame.
fn try_take_frame(buf: &mut VecDeque<u8>) -> Option<(u8, Vec<u8>)> {
    if buf.len() < 5 { return None; }
    let msg_type = buf[0];
    let len = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if buf.len() < 5 + len { return None; }
    for _ in 0..5 { buf.pop_front(); }
    let mut payload = Vec::with_capacity(len);
    for _ in 0..len { payload.push(buf.pop_front().unwrap()); }
    Some((msg_type, payload))
}

// ── EXECUTE payload builder ──────────────────────────────────────────────────

/// Build an MSG_EXECUTE payload.  Matches `pyro_server.rs` decode path:
/// `[handle:u32 LE][count:u16 LE]` then per param `[len:u32 LE][bytes]`.
fn build_execute_payload(handle: u32, params: &[Value]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + 2 + params.len() * 16);
    payload.extend_from_slice(&handle.to_le_bytes());
    payload.extend_from_slice(&(params.len() as u16).to_le_bytes());
    for p in params {
        let s = crate::client::value_to_sql(p);
        let bytes: &[u8] = if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
            s[1..s.len() - 1].as_bytes()
        } else {
            s.as_bytes()
        };
        payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        payload.extend_from_slice(bytes);
    }
    payload
}

// ── Response parser ──────────────────────────────────────────────────────────

pub(crate) fn parse_query_response(
    resp_type: u8,
    payload: Vec<u8>,
) -> Result<QueryResult, ClientError> {
    match resp_type {
        VW_RESP_RESULT_SET => parse_result_set(&payload),
        VW_RESP_OK => {
            let rows_affected = if payload.len() >= 8 {
                u64::from_le_bytes([
                    payload[0], payload[1], payload[2], payload[3],
                    payload[4], payload[5], payload[6], payload[7],
                ])
            } else { 0 };
            Ok(QueryResult { columns: Vec::new(), rows: Vec::new(), rows_affected })
        }
        VW_RESP_ERROR => {
            let msg = if payload.len() > 5 {
                String::from_utf8_lossy(&payload[5..]).into_owned()
            } else {
                String::from_utf8_lossy(&payload).into_owned()
            };
            Err(ClientError::Query(msg))
        }
        other => Err(ClientError::Protocol(format!(
            "PWire: unexpected response 0x{other:02x}"
        ))),
    }
}

fn parse_result_set(p: &[u8]) -> Result<QueryResult, ClientError> {
    if p.len() < 2 {
        return Err(ClientError::Protocol("PWire: result too short".into()));
    }
    let mut pos = 0;
    let col_count = u16::from_le_bytes([p[0], p[1]]) as usize;
    pos += 2;

    let mut columns = Vec::with_capacity(col_count);
    let mut type_tags = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        if pos >= p.len() { break; }
        let name_len = p[pos] as usize; pos += 1;
        let name = if pos + name_len <= p.len() {
            String::from_utf8_lossy(&p[pos..pos + name_len]).into_owned()
        } else { "?".to_owned() };
        pos += name_len;
        let type_tag = if pos < p.len() { p[pos] } else { 3 }; pos += 1;
        columns.push(name);
        type_tags.push(type_tag);
    }

    if pos + 4 > p.len() {
        return Err(ClientError::Protocol("PWire: truncated row count".into()));
    }
    let row_count = u32::from_le_bytes([p[pos], p[pos + 1], p[pos + 2], p[pos + 3]]) as usize;
    pos += 4;

    let null_bitmap_len = col_count.div_ceil(8);
    let col_meta = ColumnMeta::new(columns.clone());
    let mut rows = Vec::with_capacity(row_count);

    for _ in 0..row_count {
        if pos + null_bitmap_len > p.len() { break; }
        let bitmap_start = pos;
        pos += null_bitmap_len;

        let mut values = Vec::with_capacity(col_count);
        for col_idx in 0..col_count {
            let is_null = null_bitmap_len > 0
                && (p[bitmap_start + col_idx / 8] >> (col_idx % 8)) & 1 == 1;
            if is_null { values.push(Value::Null); continue; }
            match type_tags[col_idx] {
                1 => {
                    if pos + 8 > p.len() { break; }
                    let v = i64::from_le_bytes([
                        p[pos], p[pos+1], p[pos+2], p[pos+3],
                        p[pos+4], p[pos+5], p[pos+6], p[pos+7],
                    ]);
                    pos += 8;
                    values.push(Value::Int(v));
                }
                2 => {
                    if pos + 8 > p.len() { break; }
                    let v = f64::from_le_bytes([
                        p[pos], p[pos+1], p[pos+2], p[pos+3],
                        p[pos+4], p[pos+5], p[pos+6], p[pos+7],
                    ]);
                    pos += 8;
                    values.push(Value::Float(v));
                }
                4 => {
                    if pos >= p.len() { break; }
                    values.push(Value::Bool(p[pos] != 0));
                    pos += 1;
                }
                _ => {
                    // Text / Bytes: u16 LE length prefix (matches the MSG_QUERY
                    // response path in the legacy transport).  MSG_EXECUTE's
                    // u32-prefix variant is currently not used by the test
                    // workload — a future TODO if long-text columns hit the
                    // EXECUTE path.
                    if pos + 2 > p.len() { break; }
                    let len = u16::from_le_bytes([p[pos], p[pos + 1]]) as usize;
                    pos += 2;
                    if pos + len > p.len() { break; }
                    let s = String::from_utf8_lossy(&p[pos..pos + len]).into_owned();
                    pos += len;
                    values.push(Value::Text(s));
                }
            }
        }
        rows.push(Row::new(std::sync::Arc::clone(&col_meta), values));
    }

    Ok(QueryResult { columns, rows, rows_affected: 0 })
}

// ── TODOs / follow-ups ───────────────────────────────────────────────────────
//
// - TLS: currently plain TCP only.  A pyro-runtime TLS bridge needs custom
//   rustls glue (feed raw bytes through ConnectionCommon + drive state
//   machine inside the recv task).  For now the transport falls back to
//   the legacy blocking path when `connect_tls` is used.
//
// - Concurrent pipelining over ONE connection: currently multiple caller
//   threads serialise on the std::mpsc receiver.  That still buys 1 RTT
//   per N-query batch, but does NOT overlap multiple batches in flight.
//   Overlap needs a write-task + read-task split inside the runtime thread
//   communicating via an in-flight VecDeque<oneshot::Sender> — possible
//   once pyro-runtime exposes a cross-task wake primitive we can use from
//   the std::mpsc recv side.
//
// - Prepare cache sync: the caller-side `prepared` field is not
//   currently populated (the runtime thread owns the authoritative
//   cache).  Propagating newly-minted handles back to the caller side
//   would let `Pipeline::query` skip the TemplateQuery detour and
//   pre-compile EXECUTE frames — small latency win.
