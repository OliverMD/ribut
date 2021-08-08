use anyhow::Result;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};
use tokio_serde::formats::Bincode;
use tokio_serde::Framed as SerdeFramed;
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, Framed};

#[derive(Serialize, Deserialize)]
pub(crate) enum Response {
    Leader(SocketAddr),
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Request {
    Leader,
}

/// A connection to a node in the Raft cluster
struct Connection {
    inner: SerdeFramed<
        Framed<TcpStream, LengthDelimitedCodec>,
        Response,
        Request,
        Bincode<Response, Request>,
    >,
}

pub struct RaftClient {
    seed_address: SocketAddr,
    leader: Option<Connection>,
}

impl RaftClient {
    pub fn new(seed_address: SocketAddr) -> Self {
        Self {
            seed_address,
            leader: None,
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        loop {
            match self.find_leader().await {
                Some((addr, None)) => {
                    self.seed_address = addr;
                }
                Some((_, Some(conn))) => {
                    self.leader = Some(conn);
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn find_leader(&self) -> Option<(SocketAddr, Option<Connection>)> {
        let stream = TcpStream::connect(self.seed_address).await.unwrap();
        let mut conn = Connection {
            inner: SerdeFramed::new(
                Framed::new(stream, LengthDelimitedCodec::new()),
                Bincode::default(),
            ),
        };

        conn.inner.send(Request::Leader).await;
        if let Some(Ok(Response::Leader(addr))) = conn.inner.next().await {
            if addr == self.seed_address {
                println!("Correct leader");
                Some((addr, Some(conn)))
            } else {
                Some((addr, None))
            }
        } else {
            None
        }
    }

    fn get_log(&self) -> Result<()> {
        // Send request
        // Wait for response
        // Handle leader change
        // Try again
        // Maybe timeout?
        Ok(())
    }
}
