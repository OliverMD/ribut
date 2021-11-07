use crate::raft::node::NodeState::{Candidate, Follower};
use crate::raft::{ClientRPC, NodeId};
use anyhow::Result;
use futures::future::{self, Ready};
use futures::{SinkExt, Stream, StreamExt};
use itertools::Itertools;
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::cmp::min;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::thread::sleep;
use tarpc::server::{Channel, Serve};
use tarpc::{client, context, server};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Duration;
use tokio_serde::formats::Bincode;
use tokio_serde::Framed as SerdeFramed;
use tokio_stream::{self as stream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LogEntry {
    Config,
    Other(u32),
}

#[derive(Clone)]
enum NodeState {
    Follower,
    Candidate {
        response_count: u32,
        vote_count: u32,
    },
    Leader(LeaderState),
}

impl Default for NodeState {
    fn default() -> Self {
        Follower
    }
}

#[derive(Default, Clone)]
struct LeaderState {
    next_index: Vec<u32>,
    match_index: Vec<u32>,
}

#[derive(Default)]
struct GeneralState {
    // TODO: Persist to disk
    current_term: u32,
    voted_for: Option<NodeId>,
    log: Vec<(u32, LogEntry)>, // TODO: This likely needs to be indexed from 1

    commit_index: u32,
    last_applied: u32,

    leader_id: Option<NodeId>,
}

pub struct RaftNode {
    state: RwLock<GeneralState>,
    node_state: RwLock<NodeState>,
    conn_infos: Vec<(IpAddr, u16)>,
    conns: RwLock<HashMap<NodeId, NodeRPCClient>>, // TODO: Can we do better?
    node_id: NodeId,
}

impl RaftNode {
    fn new(node_id: NodeId, conn_infos: Vec<(IpAddr, u16)>) -> Self {
        Self {
            state: RwLock::new(Default::default()),
            node_state: RwLock::new(Default::default()),
            conn_infos,
            conns: RwLock::new(HashMap::new()),
            node_id,
        }
    }

    async fn connect(&self) {
        for (ip, port) in &self.conn_infos {
            let transport = tarpc::serde_transport::tcp::connect((*ip, *port), Bincode::default);
            let client =
                NodeRPCClient::new(client::Config::default(), transport.await.unwrap()).spawn();
            self.conns
                .write()
                .insert(NodeId::from((*ip, *port)), client);
        }
    }

    async fn handle_election_timout(&self) {
        let current_node_state = self.node_state.read().clone();
        match current_node_state {
            Follower => {
                {
                    let mut state = self.state.write();
                    state.current_term += 1;
                    state.voted_for = Some(self.node_id);

                    *self.node_state.write() = Candidate {
                        response_count: 0,
                        vote_count: 0,
                    };
                }

                let mut response_count = 0;
                let mut vote_count = 0;

                // Send out the requests for votes
                let mut resp_stream = {
                    let state = self.state.read();
                    let current_term = state.current_term;
                    let last_log_idx = (state.log.len() - 1) as u32;
                    let last_log_term = state.log.last().map(|t| t.0).unwrap_or(0);

                    let keys: Vec<_> = self.conns.read().keys().cloned().collect();

                    // Working around https://github.com/rust-lang/rust/issues/70263
                    stream::iter(keys)
                        .map(move |node| {
                            // let client = self.conns.clone().read().get(&node).unwrap();
                            let client = self.conns.read().get(&node).unwrap().clone();

                            async move {
                                client
                                    .request_vote(
                                        context::current(),
                                        self.node_id,
                                        current_term,
                                        self.node_id,
                                        last_log_idx,
                                        last_log_term,
                                    )
                                    .await
                            }
                        })
                        .buffer_unordered(self.conns.read().len())
                };

                while let Some(res) = resp_stream.next().await {
                    match res {
                        Ok(RequestVoteResult { term, vote_granted }) => {
                            if vote_granted {
                                response_count += 1;
                                vote_count += 1;
                            } else {
                                response_count += 1;
                                if term > self.state.read().current_term {
                                    self.state.write().current_term = term;
                                    // Become follower line 404 of spec
                                    *self.node_state.write() = Follower;
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            // TODO: Retry? Or Logging? Does tarpc retry?
                            println!("Received error send request vote: {}", e)
                        }
                    }
                }
            }
            NodeState::Candidate {
                response_count,
                vote_count,
            } => {}
            NodeState::Leader(_) => {
                // Should not happen
                panic!("Leader got election timeout!");
            }
        };
    }
}

// Created for each inbound connection with another node
#[derive(Clone)]
pub struct ConnectionHandler {
    state: Arc<RaftNode>,
    election_timeout_handler: Sender<()>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AppendEntriesResult {
    term: u32,
    success: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestVoteResult {
    term: u32,
    vote_granted: bool,
}

#[tarpc::service]
trait NodeRPC {
    async fn append_entries(
        from: NodeId, // TODO: This feels a little brittle, how can this be improved?
        term: u32,
        leader_id: NodeId,
        prev_log_index: u32,
        prev_log_term: u32,
        entries: Vec<(u32, u32, LogEntry)>,
        leader_commit: u32,
    ) -> AppendEntriesResult;

    async fn request_vote(
        from: NodeId, // TODO: Is this the same as candidate_id?
        term: u32,
        candidate_id: NodeId,
        last_log_index: u32,
        last_log_term: u32,
    ) -> RequestVoteResult;
}

impl ConnectionHandler {
    async fn append_entries_impl(
        self,
        from: NodeId,
        term: u32,
        leader_id: NodeId,
        prev_log_index: u32,
        prev_log_term: u32,
        entries: Vec<(u32, u32, LogEntry)>, // idx, term, entry
        leader_commit: u32,
    ) -> AppendEntriesResult {
        let is_from_leader = {
            let state = self.state.state.read();
            state.leader_id.map(|l| l == from).unwrap_or(false)
        };

        if is_from_leader {
            self.election_timeout_handler.send(()).await;
        }

        let mut state = self.state.state.write();

        if term < state.current_term {
            state.current_term = term;
            {
                let mut node_state = self.state.node_state.write();
                *node_state = Follower;
            }

            return AppendEntriesResult {
                term: state.current_term,
                success: false,
            };
        }

        if state
            .log
            .get(prev_log_index as usize)
            .map(|v| v.0 != prev_log_term)
            .unwrap_or(true)
        {
            return AppendEntriesResult {
                term: state.current_term,
                success: false,
            };
        }

        // TODO: Should this be here or earlier?
        state.leader_id = Some(leader_id);

        // Go through the new entries and check if there are 2 entries with the same index but mismatched
        // terms.
        if let Some((idx, _)) = entries.iter().find_position(|(idx, et, _)| {
            state
                .log
                .get(*idx as usize)
                .map(|(t, _)| *t != *et)
                .unwrap_or(false)
        }) {
            state.log.truncate(idx as usize);
        }

        let last_idx = entries.last().unwrap().0;

        for (idx, t, e) in entries {
            if state.log.get(idx as usize).is_none() {
                state.log.push((t, e));
            }
        }

        if leader_commit > state.commit_index {
            state.commit_index = min(leader_commit, last_idx);
            state.last_applied = state.commit_index;
        }

        AppendEntriesResult {
            term: state.current_term,
            success: true,
        }
    }

    async fn request_vote_impl(
        self,
        from: NodeId,
        term: u32,
        candidate_id: NodeId,
        last_log_index: u32,
        last_log_term: u32, // TODO: Need to figure out why this is here?
    ) -> RequestVoteResult {
        let mut state = self.state.state.write();

        if term > state.current_term {
            state.current_term = term;
            state.voted_for = Option::None;

            let mut node_state = self.state.node_state.write();
            *node_state = Follower;
        }

        let vote_granted = term >= state.current_term
            && (state.voted_for.is_none()
                || state.voted_for.map(|c| c == candidate_id).unwrap_or(false))
            && last_log_index > (state.log.len() as u32 - 1);

        if vote_granted {
            state.voted_for = Some(candidate_id);
        }

        RequestVoteResult {
            term: state.current_term,
            vote_granted,
        }
    }
}

#[tarpc::server]
impl NodeRPC for ConnectionHandler {
    async fn append_entries(
        self,
        _: context::Context,
        from: NodeId,
        term: u32,
        leader_id: NodeId,
        prev_log_index: u32,
        prev_log_term: u32,
        entries: Vec<(u32, u32, LogEntry)>,
        leader_commit: u32,
    ) -> AppendEntriesResult {
        self.append_entries_impl(
            from,
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        )
        .await
    }

    async fn request_vote(
        self,
        _: context::Context,
        from: NodeId,
        term: u32,
        candidate_id: NodeId,
        last_log_index: u32,
        last_log_term: u32,
    ) -> RequestVoteResult {
        self.request_vote_impl(from, term, candidate_id, last_log_index, last_log_term)
            .await
    }
}

#[tarpc::server]
impl ClientRPC for ConnectionHandler {
    type ReadLogFut = Ready<Vec<u32>>;

    fn read_log(self, _: context::Context) -> Self::ReadLogFut {
        future::ready(
            self.state
                .state
                .read()
                .log
                .iter()
                .filter_map(|(_, e)| match e {
                    LogEntry::Config => None,
                    LogEntry::Other(x) => Some(*x),
                })
                .collect(),
        )
    }

    async fn add_entry(self, _: context::Context, entry: u32) {
        // TODO: Sort this out
        // self.state.write().log.push((selfLogEntry::Other(entry)));
    }
}

pub async fn start_raft_node(
    bind_addr: IpAddr,
    bind_port: u16,
    others: Vec<(IpAddr, u16)>,
) -> JoinHandle<()> {
    let mut listener =
        tarpc::serde_transport::tcp::listen(&(bind_addr, bind_port), Bincode::default)
            .await
            .unwrap();
    listener.config_mut().max_frame_length(usize::MAX);

    let state = Arc::new(RaftNode::new((bind_addr, bind_port).into(), others));

    let mut rng = rand::thread_rng();

    let election_timeout = Duration::from_millis(500 + rng.gen_range(0..100));

    let (heartbeat_tx, mut election_rx) = mpsc::channel(10);

    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                match time::timeout(election_timeout, election_rx.recv()).await {
                    Ok(Some(())) => {
                        // Do nothing we got the heartbeat in time
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(_) => {
                        // Election timeout
                        // TODO: Do we want to await here or continue the timeout tracking, should
                        // we launch another task here?
                        let state = state.clone();
                        tokio::spawn(async move { state.handle_election_timout().await });
                    }
                }
            }
        });
    }

    let our_state = state.clone();
    let server_fut = listener.
        // Ignore accept errors.
        filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(move |channel| {
            // Created for every new connection
            let server = ConnectionHandler {
                state: state.clone(),
                election_timeout_handler: heartbeat_tx.clone(),
            };
            channel.execute(ClientRPC::serve(server))
        })
        .buffer_unordered(10000)
        .for_each(|_| async {});

    println!("Spawned");
    our_state.connect();
    tokio::spawn(server_fut)
}
