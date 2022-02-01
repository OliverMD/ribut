use crate::Command::Write;
use clap::lazy_static::lazy_static;
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use regex::Regex;
use ribut::raft::client::Client;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use tokio::io::stdin;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LinesCodec};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Cli {
    #[clap(short, long, default_value_t = 2)]
    num_threads: u32,

    seeds: Vec<String>,
}

#[derive(Debug, Eq, PartialEq)]
enum Command {
    Read,
    Write(u32),
    Exit,
}

impl FromStr for Command {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref COMMAND_REGEX: Regex = Regex::new(r"(read|write|exit)\s*(\d+)*").unwrap();
        }

        let matches = &COMMAND_REGEX.captures(s).unwrap();

        match &matches[1] {
            "read" => Ok(Self::Read),
            "exit" => Ok(Self::Exit),
            "write" => {
                let val = &matches[2];

                Ok(Write(val.parse().unwrap()))
            }
            _ => Err(()),
        }
    }
}

fn main() {
    let args = Cli::parse();

    Builder::new()
        .filter_level(LevelFilter::Info)
        .filter_module("tarpc", LevelFilter::Error)
        .filter_module("ribut::raft::node", LevelFilter::Warn)
        .filter_module("ribut::raft::client", LevelFilter::Warn)
        .init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut client = Client::new(&parse_others(&args.seeds));
            let mut console_in = FramedRead::new(stdin(), LinesCodec::new()); // TODO: This is not a great use case for this

            while let Some(Ok(a)) = console_in.next().await {
                match Command::from_str(&a) {
                    Ok(Command::Read) => {
                        if let Some(res) = client.read().await {
                            println!("Read result: {:?}", res);
                        } else {
                            println!("Unable to read!");
                        }
                    }
                    Ok(Command::Write(val)) => {
                        if client.write_val(val).await.is_some() {
                            println!("Value written!");
                        } else {
                            println!("Unable to write value!");
                        }
                    }
                    Ok(Command::Exit) => {
                        println!("Exiting...");
                        break;
                    }
                    Err(_) => {
                        println!("Unrecognised command");
                    }
                }
            }
        });
}

fn parse_others<T: AsRef<str>>(input: &[T]) -> Vec<SocketAddr> {
    input
        .iter()
        .flat_map(|v| v.as_ref().to_socket_addrs().unwrap())
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::Command;
    use std::str::FromStr;

    #[test]
    fn command_from_str() {
        assert_eq!(Command::from_str("read").unwrap(), Command::Read);
        assert_eq!(Command::from_str("write 45").unwrap(), Command::Write(45));
        assert_eq!(Command::from_str("exit").unwrap(), Command::Exit)
    }
}
