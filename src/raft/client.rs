use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize, Deserialize)]
pub(crate) enum Response {
    Leader(SocketAddr),
    Log(Vec<u32>),
}
