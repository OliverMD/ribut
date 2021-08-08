use crate::raft::client::{Request, Response};
use anyhow::Result;
use futures::SinkExt;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio_serde::formats::Bincode;
use tokio_serde::Framed as SerdeFramed;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

enum LogEntry {
    Config,
    Other,
}

struct LeaderState {
    next_index: Vec<u32>,
    match_index: Vec<u32>,
}

#[derive(Default)]
struct ServerState {
    // TODO: Persist to disk
    current_term: u32,
    voted_for: Option<u32>,
    log: Vec<LogEntry>,

    commit_index: u32,
    last_applied: u32,

    leader_state: Option<LeaderState>,
}

#[derive(Default)]
pub struct RaftNode {
    state: ServerState,
}

impl RaftNode {}

pub async fn run_node(mut node: RaftNode) -> Result<()> {
    // Need to listen on both client & raft sockets here

    let our_addr = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 5000);

    let client_listener = TcpListener::bind(our_addr).await?;

    loop {
        tokio::select! {
            Ok((socket, addr)) = client_listener.accept() => {
                let conn = SerdeFramed::new(Framed::new(socket, LengthDelimitedCodec::new()), Bincode::default());
                tokio::spawn(run_client_connection(conn, our_addr));
            }
            else => {
                break Ok(())
            }
        }
    }
}

async fn run_client_connection(
    mut conn: SerdeFramed<
        Framed<TcpStream, LengthDelimitedCodec>,
        Request,
        Response,
        Bincode<Request, Response>,
    >,
    our_addr: SocketAddr,
) {
    while let Some(Ok(msg)) = conn.next().await {
        match msg {
            Request::Leader => {
                conn.send(Response::Leader(our_addr)).await;
            }
        }
    }
}
