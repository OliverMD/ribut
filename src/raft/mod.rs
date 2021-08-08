pub mod client;
pub mod node;

use std::collections::HashMap;

// Plan:
// 1. Write a simple client
// 2. Allow client to communicate with a node
// 3. Get nodes talking to eachother
// 4. Add logic for heartbeats
// 5. Add logic for elections
// 6. Add logic for configuration changes
// 7. Add logic for snapshotting and log compaction

struct NodeId(u32);

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
