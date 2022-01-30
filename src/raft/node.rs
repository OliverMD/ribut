use crate::raft::client::Response;
use crate::raft::{
    node::NodeState::{Candidate, Follower},
    AppendEntriesResult, ClientRPC, LogEntry, NodeId, NodeRPC, NodeRPCClient, RequestVoteResult,
};
use futures::{
    future::{self},
    StreamExt,
};
use itertools::Itertools;
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use rand::Rng;
use std::net::SocketAddr;
use std::{
    cmp::{max, min},
    collections::HashMap,
    net::IpAddr,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tarpc::context::Context;
use tarpc::{client, context, server, server::Channel};
use tokio::{
    sync::{
        mpsc,
        mpsc::{Receiver, Sender},
    },
    time,
    time::Duration,
};
use tokio_serde::formats::Bincode;
use tokio_stream::{self as stream};

#[derive(Clone, Debug)]
enum NodeState {
    Follower,
    Candidate,
    Leader {
        next_index: HashMap<NodeId, u32>,
        match_index: HashMap<NodeId, u32>,
    },
}

impl Default for NodeState {
    fn default() -> Self {
        Follower
    }
}

#[derive(Default)]
struct GeneralState {
    // TODO: Persist to disk
    current_term: u32,
    voted_for: Option<NodeId>,

    // u32 is the term
    log: Vec<(u32, LogEntry)>, // TODO: This likely needs to be indexed from 1

    commit_index: u32,
    last_applied: u32,

    leader_id: Option<NodeId>,
}

pub struct RaftNode {
    state: RwLock<GeneralState>,
    node_state: RwLock<NodeState>,
    conn_infos: HashMap<NodeId, SocketAddr>,
    conns: RwLock<HashMap<NodeId, NodeRPCClient>>, // TODO: Can we do better?
    node_id: NodeId,
    election_timeout_handler: Sender<()>,
}

impl RaftNode {
    fn new(
        node_id: NodeId,
        conn_infos: Vec<SocketAddr>,
        election_timeout_handler: Sender<()>,
    ) -> Self {
        Self {
            state: RwLock::new(Default::default()),
            node_state: RwLock::new(Default::default()),
            conn_infos: conn_infos
                .iter()
                .map(|ip_port| (NodeId::from(*ip_port), *ip_port))
                .collect(),
            conns: RwLock::new(HashMap::new()),
            node_id,
            election_timeout_handler,
        }
    }

    fn entries_to_send(&self, node_id: NodeId) -> Vec<(u32, u32, LogEntry)> {
        if let NodeState::Leader {
            next_index,
            match_index: _,
        } = self.node_state.read().deref()
        {
            let next_idx = next_index.get(&node_id).cloned().unwrap_or(0);

            debug!(
                "{} - Next index for {} is {}",
                self.node_id, node_id, next_idx
            );

            let send: Vec<(u32, u32, LogEntry)> = self
                .state
                .read()
                .log
                .iter()
                .enumerate()
                .skip(next_idx.saturating_sub(1) as usize)
                .map(|(i, (t, e))| (i as u32, *t, e.clone()))
                .collect();

            if !send.is_empty() {
                debug!("{} - Sending {:?} to {}", self.node_id, send, node_id);
            }

            send
        } else {
            vec![]
        }
    }

    async fn connect(&self) {
        for (node_id, ip_port) in &self.conn_infos {
            let transport = {
                loop {
                    let transport = tarpc::serde_transport::tcp::connect(ip_port, Bincode::default);
                    match transport.await {
                        Ok(res) => break res,
                        Err(e) => error!(
                            "{} - Error connection to {:?}: {}",
                            self.node_id, ip_port, e
                        ),
                    }

                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            };

            let client = NodeRPCClient::new(client::Config::default(), transport).spawn();
            self.conns.write().insert(*node_id, client);
        }
    }

    async fn handle_election_timout(&self) {
        let current_node_state = self.node_state.read().clone();
        self.state.write().leader_id = None;
        match current_node_state {
            Follower => {
                {
                    let mut state = self.state.write();
                    state.current_term += 1;
                    state.voted_for = Some(self.node_id);

                    info!("{} - State change to Candidate", self.node_id);

                    *self.node_state.write() = Candidate;
                }

                let mut vote_count = 0;

                // Send out the requests for votes
                let mut resp_stream = {
                    let state = self.state.read();
                    let current_term = state.current_term;
                    let last_log_idx = state.log.len() as u32;
                    let last_log_term = state.log.last().map(|t| t.0).unwrap_or(0);

                    let keys: Vec<_> = self.conns.read().keys().cloned().collect();

                    // Working around https://github.com/rust-lang/rust/issues/70263
                    stream::iter(keys)
                        .map(move |node| {
                            let client = self.conns.read().get(&node).unwrap().clone();

                            async move {
                                client
                                    .request_vote(
                                        context::current(),
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
                    // TODO: Check the state we're on each loop
                    match res {
                        Ok(RequestVoteResult { term, vote_granted }) => {
                            if term >= self.state.read().current_term {
                                if vote_granted {
                                    vote_count += 1;

                                    // Check if we can become leader
                                    if matches!(
                                        *self.node_state.read(),
                                        NodeState::Candidate { .. }
                                    ) && vote_count >= self.conn_infos.len() / 2
                                    {
                                        let set_idx: u32 = self.state.read().log.len() as u32 + 1;

                                        debug!("{} - Set idx: {}", self.node_id, set_idx);

                                        *self.node_state.write() = NodeState::Leader {
                                            next_index: self
                                                .conns
                                                .read()
                                                .keys()
                                                .map(|n| (*n, set_idx))
                                                .collect(),
                                            match_index: self
                                                .conns
                                                .read()
                                                .keys()
                                                .map(|n| (*n, 0))
                                                .collect(),
                                        };

                                        self.state.write().leader_id = Some(self.node_id);

                                        info!("{} - State change to leader Leader", self.node_id);
                                        break; // We are now leader
                                    }
                                } else if term > self.state.read().current_term {
                                    self.state.write().current_term = term;
                                    // Become follower line 404 of spec
                                    *self.node_state.write() = Follower;
                                    info!("{} - State change to Follower", self.node_id);
                                    break;
                                }
                            } else {
                                warn!(
                                    "{} - Ignoring request vote response due to outdated term {}",
                                    self.node_id, term
                                )
                            }
                        }
                        Err(e) => {
                            error!(
                                "{} - Received error from send request vote: {}",
                                self.node_id, e
                            )
                        }
                    }
                }
            }
            NodeState::Candidate { .. } => {
                // Ignore
            }
            NodeState::Leader { .. } => {
                // Ignore
            }
        };
    }

    fn start_election_timeout(
        self: Arc<Self>,
        election_timeout: Duration,
        mut election_rx: Receiver<()>,
    ) {
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
                        if !matches!(*self.node_state.read(), NodeState::Leader { .. }) {
                            info!("{} - Election timeout hit", self.node_id);
                        }
                        // Election timeout
                        // TODO: Do we want to await here or continue the timeout tracking, should
                        // we launch another task here?
                        let state = self.clone();
                        tokio::spawn(async move { state.handle_election_timout().await });
                    }
                }
            }
        });
    }

    // TODO: This doesn't need to be running all the time
    fn start_heartbeats(self: Arc<Self>) {
        let mut interval = time::interval(Duration::from_millis(200));
        let node_id = self.node_id;
        let clients: HashMap<NodeId, NodeRPCClient> = {
            self.conns
                .read()
                .iter()
                .map(|(a, b)| (*a, b.clone()))
                .collect()
        };
        let state = self.clone();

        tokio::spawn(async move {
            loop {
                if matches!(*state.node_state.read(), NodeState::Leader { .. }) {
                    for (other_id, client) in &clients {
                        let (current_term, last_log_idx, last_log_term) = {
                            let idx_for_node = {
                                match state.node_state.read().deref() {
                                    NodeState::Leader {
                                        next_index,
                                        match_index: _,
                                    } => Some(*next_index.get(other_id).unwrap()),
                                    _ => None,
                                }
                            }
                                .unwrap() // TODO: Handle None case
                                - 1;

                            debug!(
                                "{} - idx for node {}: {}",
                                state.node_id, other_id, idx_for_node
                            );

                            let state = state.state.read();
                            (
                                state.current_term,
                                idx_for_node, // TODO: This needs to be fixed with indexing...
                                if idx_for_node > 0 {
                                    state
                                        .log
                                        .get(idx_for_node as usize - 1) // TODO: This is an instance where the indexing could get messed up
                                        .map(|t| t.0)
                                        .unwrap_or(0)
                                } else {
                                    0
                                },
                            )
                        };

                        let result = client
                            .append_entries(
                                context::current(),
                                node_id,
                                current_term,
                                node_id,
                                last_log_idx,
                                last_log_term,
                                state.entries_to_send(*other_id),
                                last_log_idx, // TODO: This is clearly wrong
                            )
                            .await
                            .unwrap();

                        // TODO: Fan this out, like with the election requests

                        debug!("{} - Heartbeat response: {:?}", node_id, result);
                        if result.term == state.state.read().current_term {
                            if result.success {
                                if let NodeState::Leader {
                                    next_index,
                                    match_index,
                                } = state.node_state.write().deref_mut()
                                {
                                    *next_index.get_mut(other_id).unwrap() = result.match_index + 1;
                                    *match_index.get_mut(other_id).unwrap() = result.match_index;
                                }
                            } else if let NodeState::Leader {
                                next_index,
                                match_index: _,
                            } = state.node_state.write().deref_mut()
                            {
                                let ni = next_index.get_mut(other_id).unwrap();
                                *ni = max(ni.saturating_sub(1), 1);
                            }
                        }
                    }

                    // AdvanceCommitIndex
                    // We need to find the maximum matchIndex that is on at least 50% of nodes and that index
                    // needs to have the same term as the current term

                    let new_commit_idx = {
                        let commit_idx = state.state.read().commit_index;

                        let mut new_commit_idx = commit_idx;
                        if let NodeState::Leader {
                            next_index: _,
                            match_index,
                        } = state.node_state.read().deref()
                        {
                            for idx in commit_idx..state.state.read().log.len() as u32 {
                                let a = match_index.values().filter(|&&i| i >= idx as u32).count();
                                let b = (match_index.len() + 1) / 2;

                                if a > b
                                // TODO: This almost certainly wrong. Come up with better quorum solution
                                {
                                    new_commit_idx = idx;
                                }
                            }

                            if new_commit_idx != commit_idx {
                                info!(
                                    "{} - commit_index changed from {} to {}",
                                    state.node_id, commit_idx, new_commit_idx
                                )
                            }

                            new_commit_idx
                        } else {
                            panic!("Not in leader state") // TODO: I think this can sometimes happen
                        }
                    };

                    state.state.write().commit_index = new_commit_idx;
                }
                interval.tick().await;
            }
        });
    }

    fn leader_conn_info(&self) -> Option<SocketAddr> {
        // TODO: This is wrong, it returns the port used for node comms rather than client comms

        self.state
            .read()
            .leader_id
            .map(|node_id| SocketAddr::from(*self.conn_infos.get(&node_id).unwrap()))
    }
}

#[tarpc::server]
impl NodeRPC for Arc<RaftNode> {
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
        // if entries.len() > 0 {
        debug!(
            "{} - Received append entries request: mterm: {}, ourterm: {}, pre_log_index: {}, pre_log_term: {}, entries: {:?}, leader_commit: {:?}",
            self.node_id,
            term,
            self.state.read().current_term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit
        );
        // }

        let is_from_leader = {
            let state = self.state.read();
            state.leader_id.map(|l| l == from).unwrap_or(false)
                || (term >= state.current_term && leader_id == from)
        };

        if is_from_leader {
            self.election_timeout_handler.send(()).await.unwrap();
        }

        let mut state = self.state.write();

        if term < state.current_term {
            state.current_term = term;
            {
                let mut node_state = self.node_state.write();
                info!("{} - State change to Follower", self.node_id);
                *node_state = Follower;
            }

            return AppendEntriesResult {
                term: state.current_term,
                success: false,
                match_index: 0,
            };
        }

        // LogOk
        let log_ok = {
            if prev_log_index == 0 {
                true
            } else {
                prev_log_index <= state.log.len() as u32
                    && state
                        .log
                        .get(prev_log_index as usize - 1) // TODO: Another indexing snafu
                        .map(|v| v.0 == prev_log_term)
                        .unwrap_or(false)
            }
        };

        if !log_ok {
            warn!(
                "{} - Rejecting append entries. {:?}",
                self.node_id, state.log
            );

            return AppendEntriesResult {
                term: state.current_term,
                success: false,
                match_index: 0,
            };
        }

        // TODO: Should this be here or earlier?
        state.leader_id = Some(leader_id);

        // Go through the new entries and check if there are 2 entries with the same index but mismatched
        // terms.
        if !entries.is_empty() {
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

            for (idx, t, e) in &entries {
                if state.log.get(*idx as usize).is_none() {
                    debug!("{} - Adding entry {:?}", self.node_id, e.clone());
                    state.log.push((*t, e.clone()));
                }
            }

            if leader_commit > state.commit_index {
                state.commit_index = min(leader_commit, last_idx);
                state.last_applied = state.commit_index;
            }
        }

        AppendEntriesResult {
            term: state.current_term,
            success: true,
            match_index: prev_log_index + entries.len() as u32,
        }
    }

    async fn request_vote(
        self,
        _: context::Context,
        term: u32,
        candidate_id: NodeId,
        last_log_index: u32,
        _last_log_term: u32,
    ) -> RequestVoteResult {
        let mut state = self.state.write();

        if term > state.current_term {
            state.current_term = term;
            state.voted_for = Option::None;

            let mut node_state = self.node_state.write();

            info!("{} - State change to Follower", self.node_id);
            *node_state = Follower;
        }

        let vote_granted = term >= state.current_term
            && (state.voted_for.is_none()
                || state.voted_for.map(|c| c == candidate_id).unwrap_or(false))
            && last_log_index >= (state.log.len() as u32);

        if vote_granted {
            state.voted_for = Some(candidate_id);
        }

        let res = RequestVoteResult {
            term: state.current_term,
            vote_granted,
        };

        debug!(
            "{} - Sending Request vote response to {} - {:?}",
            self.node_id, candidate_id, res
        );

        res
    }
}

#[tarpc::server]
impl ClientRPC for Arc<RaftNode> {
    async fn read_log(self, _: context::Context) -> Response<Vec<u32>> {
        if matches!(*self.node_state.read(), NodeState::Leader { .. }) {
            let ret: Vec<u32> = self.state.read().log[0..=self.state.read().commit_index as usize]
                .iter()
                .filter_map(|(_, e)| match e {
                    LogEntry::Config => None,
                    LogEntry::Other(x) => Some(*x),
                })
                .collect();

            Response::Ok(ret)
        } else {
            Response::NotLeader(self.leader_conn_info())
        }
    }

    async fn add_entry(self, _: context::Context, entry: u32) -> Response<()> {
        if matches!(self.node_state.read().deref(), NodeState::Leader { .. }) {
            let current_term = self.state.read().current_term;

            self.state
                .write()
                .log
                .push((current_term, LogEntry::Other(entry)));

            Response::Ok(())
        } else {
            Response::NotLeader(self.leader_conn_info())
        }
    }

    async fn leader(self, _context: Context) -> Response<()> {
        if matches!(*self.node_state.read(), NodeState::Leader { .. }) {
            debug!("{} - leader - we are leader", self.node_id);
            Response::Ok(())
        } else {
            debug!("{} - leader - not leader", self.node_id);
            Response::NotLeader(self.leader_conn_info())
        }
    }
}

pub async fn start_raft_node(
    bind_addr: IpAddr,
    client_bind_port: u16,
    node_bind_port: u16,
    others: Vec<SocketAddr>,
) {
    let node_id = (bind_addr, node_bind_port).into();
    let election_timeout = Duration::from_millis(1000 + rand::thread_rng().gen_range(0..250));
    let (heartbeat_tx, election_rx) = mpsc::channel(10);
    let state = Arc::new(RaftNode::new(node_id, others, heartbeat_tx));

    info!("{} - Starting node server", node_id);

    start_node_server(state.clone(), bind_addr, node_bind_port).await;

    info!("{} - Connecting to other nodes", node_id);

    state.connect().await;

    info!("{} - Starting heartbeats", node_id);
    state.clone().start_heartbeats();
    info!("{} - Starting election timeout", node_id);
    state
        .clone()
        .start_election_timeout(election_timeout, election_rx);
    info!("{} - Starting client server", node_id);
    start_client_server(state.clone(), bind_addr, client_bind_port).await;

    info!("{} - Node startup complete", node_id);
}

async fn start_node_server(state: Arc<RaftNode>, bind_addr: IpAddr, bind_port: u16) {
    let node_listener =
        tarpc::serde_transport::tcp::listen(&(bind_addr, bind_port), Bincode::default)
            .await
            .unwrap();

    let server_for_node = node_listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(move |channel| {
            // Created for every new connection
            channel.execute(NodeRPC::serve(state.clone()))
        })
        .buffer_unordered(10000)
        .for_each(|_| async {});

    tokio::spawn(server_for_node);
}

async fn start_client_server(state: Arc<RaftNode>, bind_addr: IpAddr, bind_port: u16) {
    let mut client_listener =
        tarpc::serde_transport::tcp::listen(&(bind_addr, bind_port), Bincode::default)
            .await
            .unwrap();
    client_listener.config_mut().max_frame_length(usize::MAX);

    let client_server_state = state.clone();

    let server_for_client_fut = client_listener.
        // Ignore accept errors.
        filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(move |channel| {
            channel.execute(ClientRPC::serve(client_server_state.clone()))
        })
        .buffer_unordered(10000)
        .for_each(|_| async {});

    tokio::spawn(server_for_client_fut);
}
