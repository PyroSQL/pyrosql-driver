//! Transparent auto-pipelining multiplexer for PWire connections.
//!
//! # Model
//!
//! The caller keeps looking like plain request/response — each
//! `submit` returns a future that resolves when THIS query's response
//! arrives.  Behind the scenes a single TCP socket is owned by two
//! OS threads (writer + reader) plus a per-connection FIFO waiter
//! queue.  When N callers submit concurrently, the writer drains
//! everything queued into a single `write_all` syscall → N queries
//! pipeline automatically on the shared socket.
//!
//! # Implementation notes
//!
//! All synchronisation is plain std primitives (std::sync::mpsc,
//! std::sync::Mutex, parking_lot::Mutex, std::thread).  No runtime
//! dependency — the returned future works under any executor that
//! can poll a `Pin<Box<dyn Future>>`.  Waker notifications go through
//! `futures::channel::oneshot` which is the only async-specific piece.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use futures::channel::oneshot;
use parking_lot::Mutex;

/// Encoded request ready to write.  Caller already serialised the
/// pwire frame (type byte + length + payload).
pub type Frame = Vec<u8>;

/// Decoded response: `(type_byte, payload_bytes)`.
pub type FrameReply = (u8, Vec<u8>);

/// Handle to a multiplexed PWire connection.  Cloneable — all clones
/// share the same underlying TCP socket and I/O threads.
#[derive(Clone)]
pub struct MuxConnection {
    /// Submission channel to the writer thread.
    tx: std::sync::mpsc::Sender<PendingRequest>,
    /// Kept so the connection stays alive as long as at least one
    /// clone exists.  When all clones drop, the Sender drops, the
    /// writer thread exits cleanly, the Drop on TcpStream closes the
    /// socket, and the reader thread sees EOF and exits.
    _keep_alive: Arc<()>,
}

struct PendingRequest {
    frame: Frame,
    reply: oneshot::Sender<io::Result<FrameReply>>,
}

impl MuxConnection {
    /// Open a multiplexed PWire connection.
    pub fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();
        let (tx, rx) = std::sync::mpsc::channel::<PendingRequest>();
        // Shared FIFO: writer appends before writing, reader pops on response.
        let waiters: Arc<Mutex<VecDeque<oneshot::Sender<io::Result<FrameReply>>>>> =
            Arc::new(Mutex::new(VecDeque::new()));
        // Spawn reader + writer.
        Self::spawn_reader(stream.try_clone()?, Arc::clone(&waiters));
        Self::spawn_writer(stream, rx, waiters);
        Ok(Self { tx, _keep_alive: Arc::new(()) })
    }

    /// Submit a pre-encoded frame.  The returned future resolves with
    /// the decoded response (or an I/O error if the connection died).
    pub fn submit(
        &self,
        frame: Frame,
    ) -> impl std::future::Future<Output = io::Result<FrameReply>> + Send {
        let (reply_tx, reply_rx) = oneshot::channel();
        let send_res = self.tx.send(PendingRequest { frame, reply: reply_tx });
        async move {
            if send_res.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "mux connection closed (writer gone)",
                ));
            }
            reply_rx.await.unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "mux I/O thread dropped reply channel",
                ))
            })
        }
    }

    fn spawn_writer(
        mut stream: TcpStream,
        rx: std::sync::mpsc::Receiver<PendingRequest>,
        waiters: Arc<Mutex<VecDeque<oneshot::Sender<io::Result<FrameReply>>>>>,
    ) {
        std::thread::Builder::new()
            .name("pyrosql-mux-tx".into())
            .spawn(move || {
                let mut batched: Vec<u8> = Vec::with_capacity(64 * 1024);
                let mut drained_waiters: Vec<oneshot::Sender<io::Result<FrameReply>>> = Vec::new();
                loop {
                    // Block until at least one request arrives.
                    let first = match rx.recv() {
                        Ok(req) => req,
                        Err(_) => break, // all senders dropped
                    };
                    batched.extend_from_slice(&first.frame);
                    drained_waiters.push(first.reply);
                    // Drain everything else queued at this instant.
                    while let Ok(req) = rx.try_recv() {
                        batched.extend_from_slice(&req.frame);
                        drained_waiters.push(req.reply);
                    }
                    // Register waiters BEFORE writing: the reader must
                    // never pop an empty queue for a response already
                    // on the wire.
                    {
                        let mut q = waiters.lock();
                        q.extend(drained_waiters.drain(..));
                    }
                    // One syscall per batch.
                    if stream.write_all(&batched).is_err() {
                        // Fail everything in the queue; reader will see EOF.
                        let mut q = waiters.lock();
                        while let Some(w) = q.pop_front() {
                            let _ = w.send(Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "mux writer: write failed",
                            )));
                        }
                        break;
                    }
                    batched.clear();
                }
                // Exit: drop remaining waiters as errors.
                let mut q = waiters.lock();
                while let Some(w) = q.pop_front() {
                    let _ = w.send(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "mux writer: shutting down",
                    )));
                }
            })
            .expect("spawn mux writer");
    }

    fn spawn_reader(
        mut stream: TcpStream,
        waiters: Arc<Mutex<VecDeque<oneshot::Sender<io::Result<FrameReply>>>>>,
    ) {
        std::thread::Builder::new()
            .name("pyrosql-mux-rx".into())
            .spawn(move || {
                let mut hdr = [0u8; 5];
                loop {
                    if stream.read_exact(&mut hdr).is_err() {
                        // EOF or error — signal everyone still waiting.
                        let mut q = waiters.lock();
                        while let Some(w) = q.pop_front() {
                            let _ = w.send(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "mux reader: socket closed",
                            )));
                        }
                        break;
                    }
                    let ty = hdr[0];
                    let len = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
                    let mut payload = vec![0u8; len];
                    if len > 0 && stream.read_exact(&mut payload).is_err() {
                        let mut q = waiters.lock();
                        while let Some(w) = q.pop_front() {
                            let _ = w.send(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "mux reader: payload truncated",
                            )));
                        }
                        break;
                    }
                    // FIFO dispatch — pwire responses are strictly ordered.
                    let maybe_waiter = {
                        let mut q = waiters.lock();
                        q.pop_front()
                    };
                    if let Some(w) = maybe_waiter {
                        let _ = w.send(Ok((ty, payload)));
                    }
                    // Else: unsolicited push; drop silently.
                }
            })
            .expect("spawn mux reader");
    }
}
