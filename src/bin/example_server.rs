use clap::Parser;
use ribut::raft::node::start_raft_node;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, ToSocketAddrs};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Cli {
    #[clap(short, long)]
    num_threads: Option<u32>,

    #[clap(short, long, default_value_t = 7000)]
    protocol_port: u16,

    #[clap(short, long, default_value_t = 6000)]
    client_port: u16,

    others: Vec<String>,
}

fn main() {
    let args = Cli::parse();
    console_subscriber::init();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();

    if let Some(num_workers) = args.num_threads {
        builder.worker_threads(num_workers as usize);
    }
    builder.build().unwrap().block_on(async {
        start_raft_node(
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            args.client_port,
            args.protocol_port,
            parse_others(&args.others),
        )
        .await;

        loop {}
    });
}

fn parse_others<T: AsRef<str>>(input: &[T]) -> Vec<SocketAddr> {
    input
        .iter()
        .flat_map(|v| v.as_ref().to_socket_addrs().unwrap())
        .collect()
}
