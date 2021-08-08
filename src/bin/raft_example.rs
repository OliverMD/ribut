use crossterm::event::{Event, EventStream, KeyCode};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use ribut::raft::client::RaftClient;
use ribut::raft::node::{run_node, RaftNode};
use std::net::IpAddr;
use std::net::SocketAddr;
use tokio_stream::StreamExt;

#[tokio::main]
pub async fn main() {
    // Start server
    let server = RaftNode::default();

    tokio::spawn(run_node(server));

    let mut client = RaftClient::new(SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 5000));
    client.connect().await;

    let mut reader = EventStream::new();

    enable_raw_mode();

    loop {
        match reader.next().await {
            Some(Ok(event)) => {
                println!("Got key");
                if event == Event::Key(KeyCode::Esc.into()) {
                    break;
                }
            }
            Some(Err(msg)) => {
                panic!("{}", msg);
            }
            None => {
                println!("Got None from event stream");
                break;
            }
        }
    }

    disable_raw_mode().unwrap();
    println!();
}
