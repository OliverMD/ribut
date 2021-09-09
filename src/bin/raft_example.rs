use crossterm::event::{Event, EventStream, KeyCode};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use futures::future::{self, Ready};
use futures::StreamExt;
use parking_lot::lock_api::RwLock;
use ribut::raft::node::{start_raft_node, RaftNode};
use ribut::raft::{node::ConnectionHandle, ClientRPC, ClientRPCClient};
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use tarpc::server::{Channel, Incoming, Serve};
use tarpc::{client, context, server, tokio_serde::formats::Bincode};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub async fn main() {
    let local = IpAddr::V6(Ipv6Addr::LOCALHOST);
    start_raft_node(local, 6000, vec![(local, 6001)]);
    start_raft_node(local, 6001, vec![(local, 6000)]);

    let mut joins = Vec::new();

    for i in 0u32..2000 {
        println!("Creating task {}", i);
        joins.push(tokio::spawn(async move {
            let transport =
                tarpc::serde_transport::tcp::connect("localhost:6000", Bincode::default);
            let client =
                ClientRPCClient::new(client::Config::default(), transport.await.unwrap()).spawn();
            let resp = client.read_log(context::current()).await;
            // println!("{} - {:?}", i, resp);
            client.add_entry(context::current(), i.pow(2)).await;
            let resp = client.read_log(context::current()).await;
            // println!("{} - {:?}", i, resp);
        }));
    }

    let a = future::join_all(joins).await;
}
