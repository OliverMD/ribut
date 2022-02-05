use crate::raft::ClientRPCClient;
use futures::StreamExt;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::net::SocketAddr;
use tarpc::client;
use tarpc::client::RpcError;
use tarpc::context::Context;
use tokio_serde::formats::Bincode;

#[derive(Serialize, Deserialize, Debug)]
pub enum Response<T> {
    NotLeader(Option<SocketAddr>),
    Ok(T),
}

impl<T> Response<T> {
    pub fn unwrap(self) -> T {
        match self {
            Response::Ok(val) => val,
            Response::NotLeader(_) => panic!("called `Response::unwrap()` on a `NotLeader` value"),
        }
    }
}

#[derive(Clone)]
pub struct Client {
    rpc_client: Option<ClientRPCClient>,
    seeds: Vec<SocketAddr>,
}

impl Client {
    pub fn new(seeds: &[SocketAddr]) -> Client {
        // TODO: Add Result and deal with invalid inputs
        Client {
            rpc_client: Option::None,
            seeds: seeds.to_vec(),
        }
    }

    pub async fn read(&mut self) -> Option<Vec<u32>> {
        self.perform(|client| async move { client.read_log(Context::current()).await })
            .await
            .map(|r| bincode::deserialize(&r).unwrap())
    }

    pub async fn write_val(&mut self, val: u32) -> Option<()> {
        self.perform(|client| async move {
            client
                .add_entry(Context::current(), bincode::serialize(&val).unwrap())
                .await
        })
        .await
    }

    async fn perform<A, R, Fut>(&mut self, action: A) -> Option<R>
    where
        A: Fn(ClientRPCClient) -> Fut,
        Fut: Future<Output = Result<Response<R>, RpcError>>,
    {
        // If contacting all the seeds still results in no leader then give up
        // TODO: Assumption that all nodes are in seeds
        let client = self.get_or_fetch_leader().await?;

        match action(client).await {
            Ok(Response::Ok(val)) => return Some(val),
            Ok(Response::NotLeader(_)) => {
                // TODO: Add logic to connect to leader
                info!("Not leader received");
                self.rpc_client = None;
            }
            Err(err) => {
                error!("RPC Error {}", err);
                self.rpc_client = None;
            }
        }

        None
    }

    /// Gets our latest view of the leader
    async fn get_or_fetch_leader(&mut self) -> Option<ClientRPCClient> {
        if let Some(leader) = &self.rpc_client {
            Some(leader.clone())
        } else {
            self.rpc_client = self.client_from_seeds().await;
            self.rpc_client.clone()
        }
    }

    /// Contacts all seeds to try and find a leader
    async fn client_from_seeds(&self) -> Option<ClientRPCClient> {
        let leader = Box::pin(
            // TODO: Figure out why we need to clone these seeds
            tokio_stream::iter(self.seeds.clone())
                .map(|saddr| async move {
                    if let Some(client) = Client::try_connect(saddr).await {
                        debug!("Sending leader quest to {}", saddr);
                        let resp = client
                            .leader(Context::current())
                            .await
                            .ok()
                            .map(|resp| {
                                debug!("Leader response: {:?}", resp);

                                match resp {
                                    // Ignore the leader suggestion as we could be contacting it as part of this stream
                                    // TODO: There's an assumption here that we know all the seeds upfront
                                    Response::NotLeader(_) => None,
                                    Response::Ok(_) => Some(client),
                                }
                            })
                            .flatten();
                        resp
                    } else {
                        None
                    }
                })
                .buffer_unordered(self.seeds.len()),
        )
        .filter(|a| futures::future::ready(a.is_some()))
        .next()
        .await
        .flatten();

        if leader.is_none() {
            warn!("Unable to find leader from seeds!");
        }

        leader
    }

    async fn try_connect(node: SocketAddr) -> Option<ClientRPCClient> {
        let transport = tarpc::serde_transport::tcp::connect(node, Bincode::default);

        let client = ClientRPCClient::new(client::Config::default(), transport.await.ok()?).spawn();
        Some(client)
    }
}
