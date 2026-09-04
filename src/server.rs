use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::{
    net::TcpListener,
    sync::{RwLock, mpsc, watch},
};
use tracing::{error, info};

use crate::{
    ClientId, Result,
    channel::Channel,
    client::{Client, handle_client},
};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnect_client_collects_other_channel_members() {
        let (sender1, _) = mpsc::channel(1);
        let (sender2, mut receiver2) = mpsc::channel(1);
        let (sender3, mut receiver3) = mpsc::channel(1);

        let mut server = Server::default();

        let mut client1 = Client::new(sender1);
        client1.nickname = Some("alice".to_string());

        let mut client2 = Client::new(sender2);
        client2.nickname = Some("bob".to_string());

        let mut client3 = Client::new(sender3);
        client3.nickname = Some("charlie".to_string());

        server.clients.insert(1, client1);
        server.clients.insert(2, client2);
        server.clients.insert(3, client3);

        let mut rust = Channel::new("#rust");
        rust.members.extend([1, 2, 3]);

        server.channels.insert("#rust".to_string(), rust);

        let result = server.disconnect_client(1);

        assert_eq!(result.nickname, Some("alice".to_string()));
        assert_eq!(result.senders.len(), 2);

        assert!(!server.clients.contains_key(&1));
        assert!(!server.channels["#rust"].members.contains(&1));
        assert!(server.channels["#rust"].members.contains(&2));
        assert!(server.channels["#rust"].members.contains(&3));

        result.senders[0]
            .try_send(":alice QUIT :Client Quit".to_string())
            .unwrap();

        result.senders[1]
            .try_send(":alice QUIT :Client Quit".to_string())
            .unwrap();

        let messages = [receiver2.try_recv().unwrap(), receiver3.try_recv().unwrap()];

        assert!(
            messages
                .iter()
                .all(|message| message == ":alice QUIT :Client Quit")
        );
    }

    #[test]
    fn disconnect_client_removes_empty_channels() {
        let (sender, _) = mpsc::channel(1);

        let mut server = Server::default();

        server.clients.insert(1, Client::new(sender));

        let mut channel = Channel::new("#rust");
        channel.members.insert(1);

        server.channels.insert("#rust".to_string(), channel);

        server.disconnect_client(1);

        assert!(!server.clients.contains_key(&1));
        assert!(!server.channels.contains_key("#rust"));
    }

    #[test]
    fn disconnect_client_keeps_non_empty_channels() {
        let (sender, _) = mpsc::channel(1);

        let mut server = Server::default();

        server.clients.insert(1, Client::new(sender));

        let mut channel = Channel::new("#rust");
        channel.members.insert(1);
        channel.members.insert(2);

        server.channels.insert("#rust".to_string(), channel);

        server.disconnect_client(1);

        assert!(server.channels.contains_key("#rust"));
        assert_eq!(server.channels["#rust"].members, HashSet::from([2]));
    }
}
