use futures::future::{self};
use futures::StreamExt;
use ribut::raft::{node::start_raft_node, ClientRPCClient};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use tarpc::{client, context, tokio_serde::formats::Bincode};
use tokio::time::Duration;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
pub async fn main() {
    const LOCAL: IpAddr = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let node_a = tokio::spawn(async {
        start_raft_node(LOCAL, 6000, 7000, vec![(LOCAL, 7001), (LOCAL, 7002)]).await
    });
    let node_b = tokio::spawn(async {
        start_raft_node(LOCAL, 6001, 7001, vec![(LOCAL, 7000), (LOCAL, 7002)]).await
    });
    let node_c = tokio::spawn(async {
        start_raft_node(LOCAL, 6002, 7002, vec![(LOCAL, 7000), (LOCAL, 7001)]).await
    });

    future::join_all(vec![node_a, node_b, node_c]).await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let clients: Vec<ClientRPCClient> = tokio_stream::iter(0u32..3)
        .map(|i| async move {
            let transport = tarpc::serde_transport::tcp::connect(
                SocketAddr::new(LOCAL, 6000 + i as u16),
                Bincode::default,
            );
            let client =
                ClientRPCClient::new(client::Config::default(), transport.await.unwrap()).spawn();

            client
        })
        .buffer_unordered(3)
        .collect::<Vec<ClientRPCClient>>()
        .await;

    let mut joins = Vec::new();

    for i in 0usize..3 {
        let client = clients.get(i).unwrap().clone();

        joins.push(tokio::spawn(async move {
            let resp = client.read_log(context::current()).await;

            for j in 0u32..4 {
                println!("{} - {:?}", j, resp);
                client
                    .add_entry(context::current(), j.pow(2))
                    .await
                    .unwrap();
                let resp = client.read_log(context::current()).await;
                println!("{} - {:?}", j, resp);
            }
        }));
    }

    future::join_all(joins).await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("START PHASE 3");

    let mut joins = Vec::new();

    for i in 0usize..3 {
        let client = clients.get(i).unwrap().clone();

        joins.push(tokio::spawn(async move {
            let resp = client.read_log(context::current()).await;

            for j in 0u32..4 {
                println!("{} - {:?}", j, resp);
                client
                    .add_entry(context::current(), j.pow(2))
                    .await
                    .unwrap();
                let resp = client.read_log(context::current()).await;
                println!("{} - {:?}", j, resp);
            }
        }));
    }

    future::join_all(joins).await;

    tokio::time::sleep(Duration::from_secs(5)).await;
}
