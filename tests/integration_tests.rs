use env_logger::Builder;
use futures::future::{self};
use log::LevelFilter;
use ribut::raft::client::Client;
use ribut::raft::node::start_raft_node;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use tokio::time::Duration;

// #[tokio::main(flavor = "multi_thread", worker_threads = 4)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_test() {
    Builder::new()
        .is_test(true)
        .filter_level(LevelFilter::Info)
        .filter_module("tarpc", LevelFilter::Error)
        .filter_module("ribut::raft::node", LevelFilter::Info)
        .filter_module("ribut::raft::client", LevelFilter::Info)
        .init();

    const LOCAL: IpAddr = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let node_a = tokio::spawn(async {
        start_raft_node::<u32>(
            LOCAL,
            6000,
            7000,
            vec![(LOCAL, 7001).into(), (LOCAL, 7002).into()],
        )
        .await
    });
    let node_b = tokio::spawn(async {
        start_raft_node::<u32>(
            LOCAL,
            6001,
            7001,
            vec![(LOCAL, 7000).into(), (LOCAL, 7002).into()],
        )
        .await
    });
    let node_c = tokio::spawn(async {
        start_raft_node::<u32>(
            LOCAL,
            6002,
            7002,
            vec![(LOCAL, 7000).into(), (LOCAL, 7001).into()],
        )
        .await
    });

    future::join_all(vec![node_a, node_b, node_c]).await;

    let seeds = vec![
        SocketAddr::new(LOCAL, 6000),
        SocketAddr::new(LOCAL, 6001),
        SocketAddr::new(LOCAL, 6002),
    ];

    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut client = Client::new(&seeds);
    let mut joins = Vec::new();

    {
        let mut client = client.clone();
        joins.push(tokio::spawn(async move {
            for j in 0u32..4 {
                let resp = client.write_val(j.pow(2)).await;
                println!("{} - {:?}", j, resp);
            }
        }));
    }

    future::join_all(joins).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let resp = client.write_val(16).await;
    println!("{:?}", resp);

    let resp = client.read().await;
    println!("{:?}", resp);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let resp = client.read().await;
    println!("{:?}", resp);
    assert_eq!(resp, Some(vec![0, 1, 4, 9, 16]));
}
