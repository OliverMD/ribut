use crossterm::event::{Event, EventStream, KeyCode};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use futures::future::{self, Ready};
use futures::StreamExt;
use parking_lot::lock_api::RwLock;
use ribut::raft::node::{start_raft_node, RaftNode};
use ribut::raft::{node::ConnectionHandler, ClientRPC, ClientRPCClient};
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use tarpc::server::{incoming, Channel, Serve};
use tarpc::{client, context, server, tokio_serde::formats::Bincode};
use tokio::time::Duration;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub async fn main() {
    const LOCAL: IpAddr = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let node_a =
        tokio::spawn(async { start_raft_node(LOCAL, 6000, 7000, vec![(LOCAL, 7001)]).await });
    let node_b =
        tokio::spawn(async { start_raft_node(LOCAL, 6001, 7001, vec![(LOCAL, 7000)]).await });

    future::join_all(vec![node_a, node_b]).await;

    let mut joins = Vec::new();

    for i in 0u32..2 {
        println!("Creating task {}", i);
        joins.push(tokio::spawn(async move {
            let transport =
                tarpc::serde_transport::tcp::connect("localhost:6000", Bincode::default);
            let client =
                ClientRPCClient::new(client::Config::default(), transport.await.unwrap()).spawn();
            let resp = client.read_log(context::current()).await;
            println!("{} - {:?}", i, resp);
            client.add_entry(context::current(), i.pow(2)).await;
            let resp = client.read_log(context::current()).await;
            println!("{} - {:?}", i, resp);
        }));
    }

    let a = future::join_all(joins).await;

    tokio::time::sleep(Duration::from_secs(2)).await;
}
