// QUsb2snes-compatible server implementation

use std::sync::Arc;
use std::{fmt::Display, net::SocketAddr};

use anyhow::{Result, anyhow, bail};
use futures::future::join_all;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt, channel::oneshot};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::snes::Snes;

pub async fn run(addr: SocketAddr, snes: Arc<Snes>, cancel: CancellationToken) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    info!("QUsb2snes server starting on {addr}");
    while let Some(client) = cancel.run_until_cancelled(listener.accept()).await {
        match client {
            Ok((stream, addr)) => {
                let snes = snes.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    let cancel = cancel.child_token();
                    match cancel
                        .run_until_cancelled(serve(stream, addr, snes, cancel.clone()))
                        .await
                    {
                        None => {}
                        Some(Ok(())) => {}
                        Some(Err(e)) => error!("Failed serving QUsb2Snes client: {e}"),
                    }
                });
            }
            Err(e) => error!("accept() failed: {e}"),
        }
    }

    Ok(())
}

async fn serve(
    stream: TcpStream,
    addr: SocketAddr,
    snes: Arc<Snes>,
    cancel: CancellationToken,
) -> Result<()> {
    info!("Accepted a QUsb2snes connection from {addr}.");
    let (write, mut read) = tokio_tungstenite::accept_async(stream).await?.split();
    let mut inflight_commands = None::<oneshot::Receiver<()>>;

    let write = Arc::new(Mutex::new(write));
    while let Some(cmd) = read.next().await {
        let cmd = cmd?.into_data();
        if cmd.is_empty() {
            // emotracker seems to  send empty packets for some reason?
            continue;
        }
        let cmd: Command = match serde_json::from_slice(&cmd) {
            Ok(c) => c,
            Err(e) => {
                if let Some(inflight) = inflight_commands.take() {
                    _ = inflight.await;
                }
                write_error(e, cancel.clone()).await;
                continue;
            }
        };

        if cmd.opcode == Opcode::Close {
            return Ok(());
        } else if cmd.opcode == Opcode::Fence {
            if let Some(inflight) = inflight_commands.take() {
                _ = inflight.await;
            }
        } else {
            // If this is a PutAddress command, get the associated payload.
            let payload = if cmd.opcode == Opcode::PutAddress {
                let payload = read
                    .next()
                    .await
                    .map(|r| r.map_err(|e| e.into()))
                    .unwrap_or_else(|| Err(anyhow!("Expected a message")));

                let payload = payload.and_then(|p| match p {
                    Message::Binary(b) => Ok(b),
                    _ => Err(anyhow!("PutAddress operand must be binary")),
                });

                if let Err(e) = payload {
                    if let Some(inflight) = inflight_commands.take() {
                        _ = inflight.await;
                    }
                    write_error(e, cancel.clone()).await;
                    continue;
                }

                Some(payload.unwrap())
            } else {
                None
            };

            let (tx, rx) = oneshot::channel();
            let prev_inflight = inflight_commands.replace(rx);
            let write = write.clone();
            let snes = snes.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let response = execute(cmd, snes, payload.as_deref()).await;
                if let Some(inflight) = prev_inflight {
                    _ = inflight.await;
                }
                if let Some(response) = response.transpose() {
                    write_message_or_error(&mut *write.lock().await, response, cancel).await;
                }
                _ = tx.send(());
            });
        }
    }

    Ok(())
}

#[allow(clippy::get_first)]
async fn execute(cmd: Command, snes: Arc<Snes>, payload: Option<&[u8]>) -> Result<Option<Message>> {
    match cmd.opcode {
        Opcode::PutAddress | Opcode::GetAddress => {
            if cmd.space != Space::Snes {
                bail!("CMD space not supported");
            }
            execute_memory_command(&cmd.operands, payload, snes).await
        }

        Opcode::DeviceList => reply(Response::Results(vec!["MiSTer".to_string()])),
        Opcode::Attach => Ok(None),

        Opcode::Info => reply(Response::Results(
            [
                env!("CARGO_PKG_VERSION"),
                "MiSTer",
                "No Info",
                "NO_FILE_CMD",
                "NO_CONTROL_CMD",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        )),

        Opcode::AppVersion => reply(Response::Results(vec![
            concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION")).to_string(),
        ])),

        Opcode::Name => Ok(None),

        Opcode::QUsb2SnesRegisterApplication => Ok(None),

        Opcode::Fence | Opcode::Close => unreachable!(), // already handled
        _ => bail!("Unimplemented opcode"),
    }
}

async fn execute_memory_command(
    operands: &[String],
    mut write_payload: Option<&[u8]>,
    snes: Arc<Snes>,
) -> Result<Option<Message>> {
    if !operands.len().is_multiple_of(2) {
        bail!("Memory command operands are not a multiple of 2");
    }

    let mut requests = Vec::new();
    for op in operands.chunks_exact(2) {
        let Ok(addr) = u32::from_str_radix(&op[0], 16) else {
            bail!("Address is invalid");
        };
        let Ok(len) = u32::from_str_radix(&op[1], 16) else {
            bail!("Length is invalid");
        };

        let payload = if let Some(payload) = write_payload.as_mut() {
            if payload.len() < len as usize {
                bail!("Payload length mismatch");
            }
            let (p, remainder) = payload.split_at(len as usize);
            *payload = remainder;
            Some(p.to_vec())
        } else {
            None
        };
        let snes = snes.clone();
        requests.push(async move {
            if let Some(payload) = payload {
                snes.write(addr, payload).await?;
                Ok(vec![])
            } else {
                snes.read(addr, len as usize).await
            }
        });
    }

    let mut result = vec![];
    for req in join_all(requests).await {
        result.append(&mut req?);
    }

    if write_payload.is_some() {
        Ok(None)
    } else {
        Ok(Some(Message::binary(result)))
    }
}

fn reply(response: Response) -> Result<Option<Message>> {
    Ok(Some(Message::text(
        serde_json::to_string(&response).unwrap(),
    )))
}

type MessageSink = SplitSink<WebSocketStream<TcpStream>, Message>;
async fn write_message(write: &mut MessageSink, message: Message, cancel: CancellationToken) {
    if let Err(e) = write.send(message).await {
        error!("Failed writing websocket response: {e}");
        // If we failed writing the response, close the connection.
        cancel.cancel();
    }
}

async fn write_error(error: impl Display, cancel: CancellationToken) {
    let text = error.to_string();
    error!("{}", &text);
    // QUsb2snes has no error-reporting mechanism; the server just closes the connection :/
    cancel.cancel();
}

async fn write_message_or_error(
    write: &mut MessageSink,
    message: Result<Message>,
    cancel: CancellationToken,
) {
    match message {
        Ok(m) => write_message(write, m, cancel).await,
        Err(e) => write_error(e, cancel).await,
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct Command {
    opcode: Opcode,

    #[serde(default)] // archipelago is bad-behaved and doesn't send this sometimes
    space: Space,

    #[allow(unused)]
    #[serde(default)]
    flags: Option<Vec<String>>, // idk what these are, the docs don't say and i don't feel like digging

    #[serde(default)]
    operands: Vec<String>,
}

// https://github.com/Skarsnik/QUsb2snes/blob/3dd88ab0549307c8e5038fe301f36e282a863699/core/usb2snes.h#L104
#[derive(Deserialize, PartialEq, Eq, Clone, Copy, Debug)]
pub enum Opcode {
    // Connection
    DeviceList,
    Attach,
    AppVersion,
    Name,
    Close,

    // Special
    Info,
    Boot,
    Menu,
    Reset,
    Binary,
    Stream,
    Fence,

    GetAddress,
    PutAddress,
    PutIPS,

    GetFile,
    PutFile,
    List,
    Remove,
    Rename,
    MakeDir,

    QUsb2SnesRegisterApplication,
}

#[derive(Deserialize, PartialEq, Eq, Clone, Copy, Debug, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum Space {
    #[default]
    Snes,
    Cmd,
}

#[derive(Serialize, Clone, Debug)]
pub enum Response {
    Results(Vec<String>),
}
