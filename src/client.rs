use std::sync::Arc;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{RwLock, mpsc, watch},
};
use tracing::info;

use crate::{ClientId, Result, command::parse_command, handler::handle_command, server::Server};

#[derive(Debug)]
pub(crate) struct Client {
    pub(crate) nickname: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) realname: Option<String>,
    pub(crate) sender: mpsc::Sender<String>,
}

impl Client {
    pub(crate) fn new(sender: mpsc::Sender<String>) -> Self {
        Self {
            nickname: None,
            username: None,
            realname: None,
            sender,
        }
    }

    /// NICK + USER -> registration
    pub(crate) fn is_registered(&self) -> bool {
        self.nickname.is_some() && self.username.is_some()
    }
}

pub(crate) async fn handle_client(
    stream: TcpStream,
    client_id: ClientId,
    server: Arc<RwLock<Server>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();

    let (sender, mut receiver) = mpsc::channel::<String>(100);

    {
        let mut server = server.write().await;

        server
            .clients
            .insert(client_id, Client::new(sender.clone()));
    }

    let read_task = async {
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        loop {
            tokio::select! {
                result = reader.read_line(&mut line) => {
                    let bytes_read = result?;

                    if bytes_read == 0 {
                        break;
                    }

                    let command = parse_command(&line);

                    let should_continue =
                        handle_command(command, client_id, Arc::clone(&server), &sender).await?;

                    line.clear();

                    if !should_continue {
                        break;
                    }
                }

                result = shutdown.changed() => {
                    result?;

                    if *shutdown.borrow() {
                        break;
                    }
                }
            }
        }

        Result::Ok(())
    };

    let write_task = async {
        while let Some(message) = receiver.recv().await {
            writer.write_all(message.as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
        }

        Result::Ok(())
    };

    tokio::select! {
        result = read_task => {
            result?;
        }
        result = write_task => {
            result?;
        }
    }

    {
        let result = {
            let mut server = server.write().await;
            server.disconnect_client(client_id)
        };

        if let Some(nickname) = result.nickname {
            let message = format!(":{nickname} QUIT :Client Quit");

            for sender in result.senders {
                sender.send(message.clone()).await?;
            }
        }
    }

    info!(%client_id, "Client disconnected");

    Ok(())
}
