pub mod client;
pub mod node;

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;

// Plan:
// 1. Write a simple client
// 2. Allow client to communicate with a node
// 3. Get nodes talking to eachother
// 4. Add logic for heartbeats
// 5. Add logic for elections
// 6. Add logic for configuration changes
// 7. Add logic for snapshotting and log compaction

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
struct NodeId(u64);

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{}>", self.0)
    }
}

impl From<(IpAddr, u16)> for NodeId {
    fn from((addr, port): (IpAddr, u16)) -> Self {
        let mut hasher = DefaultHasher::new();
        addr.hash(&mut hasher);
        port.hash(&mut hasher);

        NodeId(hasher.finish())
    }
}

struct AppendEntriesArgs {
    term: u32,
    leader_id: NodeId,
    prev_log_index: u32,
    prev_log_term: u32,
    // entries: Vec<LogEntry>,
    leader_commit: u32,
}

struct AppendEntriesResult {
    term: u32,
    success: bool,
}

struct RequestVoteArgs {
    term: u32,
    candidate_id: NodeId,
    last_log_index: u32,
    last_log_term: u32,
}

struct RequestVoteResult {
    term: u32,
    vote_granted: bool,
}

#[tarpc::service]
pub trait ClientRPC {
    async fn read_log() -> Vec<u32>;
    async fn add_entry(entry: u32);
}
