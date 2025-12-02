use std::collections::VecDeque;
use std::pin::pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures::future::try_join;
use futures::{Stream, TryStreamExt};
use rand::Fill;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, split},
    select,
    sync::{mpsc, oneshot},
};
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, trace, warn};

/// Handles communication with the SNES core over UART.
///
/// Supports both request-response and streaming modes: a standard request generates a single
/// response, while a streaming request generates a respoonse once per frame.
#[derive(Debug)]
pub struct Snes {
    /// Channel for sending requests to the SNES task.
    tx: mpsc::Sender<Request>,

    /// Channel for sending streaming requests to the SNES task.
    stream_tx: mpsc::Sender<StreamRequest>,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("The SNES core is not connected.")]
    Disconnected,

    #[error("Serial communication error")]
    Serial(#[from] tokio_serial::Error),

    #[error("I/O error")]
    Io(#[from] std::io::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

/// A oneshot request.
#[derive(Debug)]
struct Request {
    /// The command bytes to be sent to the core.
    request: Vec<u8>,

    /// Once the core replies, the data recieved in response to the command.
    response: oneshot::Sender<Result<Vec<u8>>>,

    /// A semaphore permit for the RX buffer capacity used by this command.
    buffer_space: Option<OwnedSemaphorePermit>,
}

/// A streaming request.
struct StreamRequest {
    /// The command bytes to be sent to the core, once per frame.
    requests: Vec<Vec<u8>>,

    /// A channel over which we'll send the replies to the commands.
    response: mpsc::Sender<Result<Vec<Vec<u8>>>>,
}

#[repr(u8)]
enum Command {
    Ping,
    Read,
    Write,
    WaitNMI,
}

type Serial = BufReader<SerialStream>;

impl Snes {
    const MAX_REQUEST_LEN: usize = 255;
    const RX_BUFFER_LEN: usize = 512;
    const REQUEST_TIMEOUT_MS: u64 = 1000;
    const RESPONSE_TIMEOUT_MS: u64 = 1000;

    /// Starts the SNES interface.
    ///
    /// The interface will be stopped once all outstanding references are dropped.
    pub fn run() -> (Arc<Snes>, impl Future<Output = Result<()>>) {
        let (tx, rx) = mpsc::channel(4096);
        let (stream_tx, stream_rx) = mpsc::channel(128);
        let snes = Arc::new(Snes { tx, stream_tx });
        (snes.clone(), snes._run(rx, stream_rx))
    }

    /// Reads memory from the attached SNES core.
    #[tracing::instrument]
    pub async fn read(&self, address: u32, len: usize) -> Result<Vec<u8>> {
        // Each read request can read a maximum of 255 bytes,
        // so split into multiple requests if needed.
        // Fire off the requests all at once, then wait for them all at once.
        let mut chunks = Vec::with_capacity(len.div_ceil(Self::MAX_REQUEST_LEN));
        for offset in (0..len).step_by(255) {
            let address = (address.wrapping_add(offset as u32)).to_le_bytes();
            chunks.push(
                self.send([
                    Command::Read as u8,
                    address[0],
                    address[1],
                    address[2],
                    (len - offset).min(Self::MAX_REQUEST_LEN) as u8,
                ])
                .await,
            );
        }

        let mut result = Vec::with_capacity(len);
        for chunk in chunks {
            let Ok(buf) = chunk.await else {
                return Err(Error::Disconnected);
            };
            result.append(&mut buf?);
        }

        Ok(result)
    }

    /// Writes memory to the attached SNES core.
    #[tracing::instrument]
    pub async fn write(&self, address: u32, data: Vec<u8>) -> Result<()> {
        // Each read request can write a maximum of 256 bytes,
        // so split into multiple requests if needed.
        // Fire off the requests all at once, then wait for them all at once.
        let mut chunks = Vec::with_capacity(data.len().div_ceil(Self::MAX_REQUEST_LEN));
        for (i, chunk) in data.chunks(Self::MAX_REQUEST_LEN).enumerate() {
            let address = (address
                .wrapping_add((i as u32).wrapping_mul(Self::MAX_REQUEST_LEN as u32)))
            .to_le_bytes();
            chunks.push(
                self.send(
                    [
                        Command::Write as u8,
                        address[0],
                        address[1],
                        address[2],
                        chunk.len() as u8,
                    ]
                    .into_iter()
                    .chain(chunk.iter().copied()),
                )
                .await,
            );
        }

        for chunk in chunks {
            chunk.await.unwrap_or_else(|_| Err(Error::Disconnected))?;
        }
        Ok(())
    }

    /// Streams the contents of a memory location every frame.
    #[allow(unused)]
    pub async fn stream_reads<R: IntoIterator<Item = (u32, usize)>>(
        &self,
        requests: R,
        // TODO: `use<R>` is not needed, but we need
        // https://github.com/rust-lang/rust/issues/130043 to drop it
    ) -> impl Stream<Item = Result<Vec<Vec<u8>>>> + use<R> {
        let mut chunks: Vec<Vec<u8>> = vec![];
        let mut num_chunks: Vec<usize> = vec![];
        for (address, len) in requests {
            num_chunks.push(len.div_ceil(256));
            for offset in (0..len).step_by(256) {
                let address = (address.wrapping_add(offset as u32)).to_le_bytes();
                chunks.push(vec![
                    Command::Read as u8,
                    address[0],
                    address[1],
                    address[2],
                    (len - offset).min(255) as u8,
                ]);
            }
        }
        let rx = self.stream(chunks).await;

        ReceiverStream::new(rx).map_ok(move |chunks| {
            let mut chunks = VecDeque::from(chunks);

            num_chunks
                .iter()
                .map(|&n| {
                    (0..n)
                        .flat_map(|_| chunks.pop_front().unwrap())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<Vec<u8>>>()
        })
    }

    async fn _run(
        self: Arc<Self>,
        mut reqs: mpsc::Receiver<Request>,
        mut stream_reqs: mpsc::Receiver<StreamRequest>,
    ) -> Result<()> {
        // Don't hold a strong reference to self.
        let snes = Arc::downgrade(&self);
        drop(self);

        // Open the serial port.
        let serial = tokio_serial::new("/dev/ttyS1", 115200).open_native_async()?;

        // Apply read buffering.
        let mut serial = BufReader::new(serial);

        // Active stream requests
        let mut streams: Vec<StreamRequest> = vec![];

        'connect: while !reqs.is_closed() {
            {
                // Let any streaming clients know that we don't have a connection.
                streams.retain(|req| {
                    !matches!(
                        req.response.try_send(Err(Error::Disconnected)),
                        Err(TrySendError::Closed(_))
                    )
                });

                // Start the handshake task with a timeout.
                let mut connecting = pin!(timeout(
                    Duration::from_millis(200),
                    Self::establish_connection(&mut serial)
                ));
                loop {
                    select! {
                        r = &mut connecting => match r {
                            // If we established a connection, proceed.
                            Ok(r) => break r?,

                            // If we timed out, retry..
                            Err(_) => continue 'connect,
                        },

                        // Handle pending stream requests.
                        Some(req) = stream_reqs.recv() => streams.push(req),

                        // Drop pending requests so clients know we're disconnected.
                        req = reqs.recv() => match req {
                            Some(r) => drop(r),
                            None => break 'connect,
                        },
                    }
                }
            }

            // Split into subtasks to handle reading & writing from the serial port, so that we can
            // pipeline requests and responses.
            //
            // The request flow looks like:
            //           Client: snes.tx.send(request)
            //
            //  handle_requests: request reqs.recv()
            //  handle_requests: serial.write(request)
            //  handle_requests: responses_tx.send(request)
            //
            // handle_responses: request = responses_rx.recv()
            // handle_responses: data = serial.read()
            // handle_responses: request.response.send(data)
            //
            //           Client: return response.recv()
            let (read, write) = split(&mut serial);
            let (responses_tx, responses_rx) = mpsc::channel(4096);

            let mut tasks = pin!(try_join(
                Self::handle_requests(&mut reqs, responses_tx, write),
                Self::handle_responses(responses_rx, read),
            ));
            loop {
                // Handle any outstanding streaming requests.
                while let Ok(req) = stream_reqs.try_recv() {
                    streams.push(req);
                }
                select! {
                    // If one of our tasks exited, that means we lost connection. Try to reconnect,
                    // or propogate the error if the task failed.
                    result = &mut tasks => match result {
                        Ok(_) => continue 'connect,
                        Err(e) => return Err(e)
                    },

                    // Handle streaming connections.
                    () = Self::handle_streams(&mut streams, &snes), if !streams.is_empty() => {},

                    // Handle streaming requests.
                    Some(req) = stream_reqs.recv(), if streams.is_empty() => {
                        streams.push(req);
                    }
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(serial))]
    async fn establish_connection(serial: &mut Serial) -> Result<()> {
        info!("Trying to establish serial connection");
        let (mut rx, mut tx) = split(serial);

        'retry: loop {
            trace!("sync 0/3");

            // Write a stream of zeroes until we get a 0 echoed back.
            const PING: u8 = Command::Ping as u8;
            tx.write_all(&[PING, PING]).await?;
            loop {
                match timeout(Duration::from_millis(1), rx.read_u8()).await {
                    Ok(Ok(PING)) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => continue 'retry,
                };
            }
            trace!("sync 1/3");

            // Write [0, 0xFF, 0, 0xFE].
            // 0 is the ping command, and 0xFF is an invalid command. This sequence will ensure we're
            // not desynchronized with the stream.
            tx.write_all(&[PING, 0xFF, PING, 0xFE]).await?;
            while rx.read_u8().await? != 0xFE {}
            trace!("sync 2/3");

            // Echo a sequence of 8 random bytes (with the top bits set so we don't accidentally send a
            // valid command.)
            let mut random = [0u8; 8];
            random.fill(&mut rand::rng());
            random.iter_mut().for_each(|b| *b |= 0x80);

            for v in random {
                tx.write_all(&[PING, v]).await?;
                if rx.read_u8().await? != 1 || rx.read_u8().await? != v {
                    continue 'retry;
                }
            }
            trace!("sync 3/3");

            break;
        }

        info!("Connection established");
        Ok(())
    }

    async fn send(
        &self,
        request: impl IntoIterator<Item = u8> + std::fmt::Debug,
    ) -> oneshot::Receiver<Result<Vec<u8>>> {
        let request: Vec<u8> = request.into_iter().collect();
        let (tx, rx) = oneshot::channel();
        _ = self
            .tx
            .send(Request {
                request,
                response: tx,
                buffer_space: None,
            })
            .await;
        rx
    }

    async fn stream(
        &self,
        request: impl IntoIterator<Item = Vec<u8>>,
    ) -> mpsc::Receiver<Result<Vec<Vec<u8>>>> {
        let (tx, rx) = mpsc::channel(128);
        _ = self
            .stream_tx
            .send(StreamRequest {
                requests: request.into_iter().collect(),
                response: tx,
            })
            .await;
        rx
    }

    #[tracing::instrument(skip(rx, tx, serial))]
    async fn handle_requests(
        rx: &mut mpsc::Receiver<Request>,
        tx: mpsc::Sender<Request>,
        serial: impl AsyncWrite,
    ) -> Result<()> {
        let mut serial = pin!(serial);
        let rx_buffer_space = Arc::new(Semaphore::new(Self::RX_BUFFER_LEN));
        loop {
            select! {
                req = rx.recv() => match req {
                    // The client has disconnected, exit
                    None => return Ok(()),

                    // We have a request, handle it
                    Some(mut req) => {
                        trace!("Sending request: {:?}", req.request);
                        req.buffer_space = Some(
                            rx_buffer_space
                            .clone()
                            .acquire_many_owned(req.request.len() as u32)
                            .await
                            .unwrap()
                        );
                        match timeout(
                            Duration::from_millis(Self::REQUEST_TIMEOUT_MS),
                            serial.write_all(&req.request)
                            ).await {
                                // Success, pass it along to the responder
                                Ok(Ok(())) => _ = tx.send(req).await,

                                // Failed, bail out
                                Ok(Err(e)) => return Err(Error::Io(e)),

                                // Timed out, end the task so we can try to reconnect
                                Err(_) => {
                                    warn!("Timed out writing request to SNES core");
                                    return Ok(())
                                },
                        }
                        trace!("Request written");
                    }
                },
                // The responder has exited, exit
                _ = tx.closed() => return Ok(())
            }
        }
    }

    #[tracing::instrument(skip(rx, serial))]
    async fn handle_responses(
        mut rx: mpsc::Receiver<Request>,
        serial: impl AsyncRead,
    ) -> Result<()> {
        let mut serial = pin!(serial);
        while let Some(req) = rx.recv().await {
            match timeout(
                Duration::from_millis(Self::RESPONSE_TIMEOUT_MS),
                Self::read_response(&mut serial),
            )
            .await
            {
                Ok(resp) => _ = req.response.send(resp),
                Err(_) => {
                    warn!("Timed out reading response from SNES core");
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn read_response(serial: impl AsyncRead) -> Result<Vec<u8>> {
        let mut serial = pin!(serial);

        let len = serial.read_u8().await?;
        trace!("Response length: {len}");
        let mut buf = vec![0; len as usize];
        serial.read_exact(&mut buf).await?;
        trace!("Resonse data: {buf:?}");

        Ok(buf)
    }

    async fn handle_streams(streams: &mut Vec<StreamRequest>, snes: &Weak<Snes>) {
        let Some(snes) = snes.upgrade() else {
            streams.clear();
            return;
        };
        // Send a WaitNMI followed by all our requests
        _ = snes.send([Command::WaitNMI as u8]).await;
        let mut commands = Vec::with_capacity(streams.len());
        for stream in &*streams {
            for request in &stream.requests {
                commands.push(snes.send(request.clone()).await);
            }
        }

        // Now wait for the command responses
        let mut responses = VecDeque::with_capacity(commands.len());
        for command in commands {
            responses.push_back(command.await.unwrap_or_else(|_| Err(Error::Disconnected)));
        }

        streams.retain_mut(|stream| {
            let response = stream
                .requests
                .iter()
                .map(|_| responses.pop_front().unwrap())
                .collect::<Result<Vec<Vec<u8>>>>();
            !matches!(
                stream.response.try_send(response),
                Err(TrySendError::Closed(_))
            )
        });
    }
}
