use anyhow::{anyhow, Context, Result};
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
    Log(Vec<u32>),
}
