use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use ribut::raft::node::start_raft_node;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Cli {
    #[clap(short, long)]
    num_threads: Option<u32>,

    #[clap(short, long, default_value_t = 7000)]
    protocol_port: u16,

    #[clap(short, long, default_value_t = 6000)]
    client_port: u16,

    #[clap(short, long)]
    tracing_enabled: bool,

    others: Vec<String>,
}

fn main() {
    let args = Cli::parse();

    Builder::new()
        .filter_level(LevelFilter::Info)
        .filter_module("tarpc", LevelFilter::Error)
        .filter_module("ribut::raft::node", LevelFilter::Info)
        .filter_module("stubborn_io::tokio::io", LevelFilter::Off)
        .init();

    if args.tracing_enabled {
        console_subscriber::init();
    }

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();

    if let Some(num_workers) = args.num_threads {
        builder.worker_threads(num_workers as usize);
    }
    builder.build().unwrap().block_on(async {
        start_raft_node::<u32>(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            args.client_port,
            args.protocol_port,
            parse_others(&args.others),
        )
        .await;

        // wait forever
        tokio::signal::ctrl_c()
            .await
            .expect("failed to wait for ctrl-c");
    });
}

fn parse_others<T: AsRef<str>>(input: &[T]) -> Vec<SocketAddr> {
    input
        .iter()
        .flat_map(|v| v.as_ref().to_socket_addrs().unwrap())
        .collect()
}
