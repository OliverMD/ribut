pub mod client;
pub mod node;

use crate::raft::client::Response;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::{
    collections::hash_map::DefaultHasher,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    net::IpAddr,
};

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
struct NodeId(u64);

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{}>", self.0)
    }
}

impl From<(IpAddr, u16)> for NodeId {
    fn from((addr, port): (IpAddr, u16)) -> Self {
        SocketAddr::new(addr, port).into()
    }
}

impl From<SocketAddr> for NodeId {
    fn from(s: SocketAddr) -> Self {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);

        NodeId(hasher.finish())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct AppendEntriesResult {
    term: u32,
    success: bool,
    match_index: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestVoteResult {
    term: u32,
    vote_granted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LogEntry<T> {
    Config,
    Other(T),
}

#[tarpc::service]
pub trait ClientRPC {
    async fn read_log() -> Response<Vec<u8>>;
    async fn add_entry(entry: Vec<u8>) -> Response<()>;

    /// Returns Ok(()) iff it is the leader. NotLeader(leader) otherwise
    async fn leader() -> Response<()>;
}

#[tarpc::service]
trait NodeRPC {
    async fn append_entries(
        from: NodeId,
        term: u32,
        leader_id: NodeId,
        prev_log_index: u32,
        prev_log_term: u32,
        entries: Vec<(u32, u32, Vec<u8>)>, // (index, term, entry)
        leader_commit: u32,
    ) -> AppendEntriesResult;

    async fn request_vote(
        term: u32,
        candidate_id: NodeId,
        last_log_index: u32,
        last_log_term: u32,
    ) -> RequestVoteResult;
}
