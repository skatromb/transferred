//! Observes prepared INSERT commands and injects faults against a real server.

#![expect(
    clippy::arithmetic_side_effects,
    clippy::little_endian_bytes,
    reason = "MySQL packets use little endian lengths and bounded byte offsets"
)]

use std::future::pending;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use url::Url;

use crate::server::Server;
use crate::wire_fault::WireFault;
use crate::wire_state::WireState;

pub(crate) struct WireProxy {
    dsn: String,
    state: Arc<WireState>,
    task: JoinHandle<()>,
}

impl WireProxy {
    pub(crate) async fn start(server: &Server, fault: WireFault) -> Self {
        let mut url = Url::parse(&server.dsn()).unwrap();
        let upstream = url.port().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        url.set_port(Some(listener.local_addr().unwrap().port()))
            .unwrap();
        let state = Arc::new(WireState::default());
        let observation = state.clone();
        let task = tokio::spawn(async move {
            let (client, _) = listener.accept().await.unwrap();
            let backend = TcpStream::connect(("127.0.0.1", upstream)).await.unwrap();
            drop(forward(client, backend, fault, &observation).await);
        });
        Self {
            dsn: url.to_string(),
            state,
            task,
        }
    }

    pub(crate) fn dsn(&self) -> &str {
        &self.dsn
    }

    pub(crate) fn insert_commands(&self) -> usize {
        self.state.insert_commands()
    }

    pub(crate) fn maximum_parameters(&self) -> usize {
        self.state.maximum_parameters()
    }
}

impl Drop for WireProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn forward(
    client: TcpStream,
    backend: TcpStream,
    fault: WireFault,
    state: &WireState,
) -> io::Result<()> {
    let (incoming, outgoing) = client.into_split();
    let (responses, commands) = backend.into_split();
    tokio::try_join!(
        send_commands(incoming, commands, fault, state),
        send_responses(responses, outgoing, fault, state)
    )?;
    Ok(())
}

async fn send_commands(
    mut incoming: OwnedReadHalf,
    mut outgoing: OwnedWriteHalf,
    fault: WireFault,
    state: &WireState,
) -> io::Result<()> {
    loop {
        let mut packet = read_packet(&mut incoming).await?;
        if state.command(&packet) && fault == WireFault::InsertWarning {
            state.intercept().store(true, Ordering::SeqCst);
        }
        inspect_query(&mut packet, fault, state.intercept())?;
        outgoing.write_all(&packet).await?;
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "lists the fault applied to each observed SQL command"
)]
fn inspect_query(packet: &mut Vec<u8>, fault: WireFault, intercept: &AtomicBool) -> io::Result<()> {
    if packet[3] != 0 || packet[4] != 3 {
        return Ok(());
    }
    let query = String::from_utf8_lossy(&packet[5..]);
    if query.starts_with("RENAME TABLE ") {
        match fault {
            WireFault::BeforeRename => return Err(io::Error::other("injected request loss")),
            WireFault::AfterRename | WireFault::TimeoutRename => {
                intercept.store(true, Ordering::SeqCst);
            }
            WireFault::RejectRename => replace_query(
                packet,
                "RENAME TABLE `_transferred_missing` TO `_transferred_unused`",
            ),
            WireFault::Observe | WireFault::RejectBackup | WireFault::InsertWarning => {}
        }
        return Ok(());
    }
    if fault == WireFault::RejectBackup
        && query.starts_with("DROP TABLE ")
        && query.ends_with("_b`")
    {
        replace_query(packet, "DROP TABLE `_transferred_missing`");
    }
    Ok(())
}

async fn send_responses(
    mut incoming: OwnedReadHalf,
    mut outgoing: OwnedWriteHalf,
    fault: WireFault,
    state: &WireState,
) -> io::Result<()> {
    loop {
        let mut packet = read_packet(&mut incoming).await?;
        state.response(&packet);
        if state.intercept().swap(false, Ordering::SeqCst) {
            if fault == WireFault::TimeoutRename {
                pending::<()>().await;
            }
            if fault == WireFault::AfterRename {
                return Err(io::Error::other("injected response loss"));
            }
            warning(&mut packet);
        }
        outgoing.write_all(&packet).await?;
    }
}

async fn read_packet(incoming: &mut OwnedReadHalf) -> io::Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    _ = incoming.read_exact(&mut header).await?;
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut packet = vec![0; usize::try_from(length).unwrap() + 4];
    packet[..4].copy_from_slice(&header);
    _ = incoming.read_exact(&mut packet[4..]).await?;
    Ok(packet)
}

fn replace_query(packet: &mut Vec<u8>, query: &str) {
    let length = u32::try_from(query.len() + 1).unwrap().to_le_bytes();
    packet.clear();
    packet.extend_from_slice(&length[..3]);
    packet.extend_from_slice(&[0, 3]);
    packet.extend_from_slice(query.as_bytes());
}

fn warning(packet: &mut [u8]) {
    assert_eq!(packet[4], 0, "INSERT must return an OK packet");
    let affected_end = 5 + encoded_length(packet[5]);
    let status = affected_end + encoded_length(packet[affected_end]);
    packet[status + 2] = 1;
    packet[status + 3] = 0;
}

const fn encoded_length(prefix: u8) -> usize {
    match prefix {
        252 => 3,
        253 => 4,
        254 => 9,
        _ => 1,
    }
}
