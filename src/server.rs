use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::{
    net::TcpListener,
    sync::{RwLock, mpsc, watch},
};
use tracing::{error, info};

use crate::{ClientId, Result, channel::Channel, client::Client, handle_client};

#[derive(Debug, Default)]
pub(crate) struct Server {
    pub(crate) clients: HashMap<ClientId, Client>,
    pub(crate) channels: HashMap<String, Channel>,
}

impl Server {
    pub(crate) fn disconnect_client(&mut self, client_id: ClientId) -> DisconnectResult {
        let nickname = self
            .clients
            .get(&client_id)
            .and_then(|client| client.nickname.clone());

        let mut member_ids = HashSet::new();

        for channel in self.channels.values() {
            if channel.members.contains(&client_id) {
                member_ids.extend(
                    channel
                        .members
                        .iter()
                        .filter(|member_id| **member_id != client_id)
                        .copied(),
                );
            }
        }

        let senders: Vec<mpsc::Sender<String>> = member_ids
            .iter()
            .filter_map(|member_id| {
                self.clients
                    .get(member_id)
                    .map(|client| client.sender.clone())
            })
            .collect();

        self.clients.remove(&client_id);

        for channel in self.channels.values_mut() {
            channel.members.remove(&client_id);
        }

        self.channels
            .retain(|_, channel| !channel.members.is_empty());

        DisconnectResult { nickname, senders }
    }
}

#[derive(Debug)]
pub(crate) struct DisconnectResult {
    pub(crate) nickname: Option<String>,
    pub(crate) senders: Vec<mpsc::Sender<String>>,
}

pub async fn run_server(listener: TcpListener, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let server = Arc::new(RwLock::new(Server::default()));
    let mut next_client_id: ClientId = 1;
    let mut client_tasks = Vec::new();

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, addr) = result?;

                let client_id = next_client_id;
                next_client_id += 1;

                info!(%client_id, %addr, "Client connected");

                let server = Arc::clone(&server);

                let client_shutdown = shutdown.clone();

                let task = tokio::spawn(async move {
                    if let Err(err) = handle_client(stream, client_id, server, client_shutdown).await {
                        error!(%client_id, error = %err, "Client error");
                    }
                });

                client_tasks.push(task);
            },
            result = shutdown.changed() => {
                result?;

                if *shutdown.borrow() {
                    break;
                }
            }
        }
    }

    for task in client_tasks {
        task.await
            .map_err(|err| format!("Client task failed: {err}"))?;
    }

    Ok(())
}
