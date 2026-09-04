mod channel;
mod client;
mod command;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{RwLock, mpsc, watch},
};
use tracing::{debug, error, info};

use crate::{
    channel::Channel,
    client::Client,
    command::{Command, parse_command},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ClientId = u64;

const RPL_WELCOME: &str = "001";

// const RPL_LISTSTART: &str = "321";
const RPL_LIST: &str = "322";
const RPL_LISTEND: &str = "323";
const RPL_TOPIC: &str = "332";
// const RPL_TOPICTIME: &str = "333";
const RPL_NAMREPLY: &str = "353";
const RPL_ENDOFNAMES: &str = "366";

const ERR_NOSUCHNICK: &str = "401";
// const ERR_NOSUCHSERVER: &str = "402";
const ERR_NOSUCHCHANNEL: &str = "403";
// const ERR_TOOMANYCHANNELS: &str = "405";
const ERR_NOTONCHANNEL: &str = "442";
const ERR_NOTREGISTERED: &str = "451";
const ERR_NEEDMOREPARAMS: &str = "461";
// const ERR_CHANNELISFULL: &str = "471";
// const ERR_INVITEONLYCHAN: &str = "473";
// const ERR_BANNEDFROMCHAN: &str = "474";
// const ERR_BADCHANNELKEY: &str = "475";

const ERR_NONICKNAMEGIVEN: &str = "431";
const ERR_ERRONEUSNICKNAME: &str = "432";
const ERR_NICKNAMEINUSE: &str = "433";
const SERVER_NAME: &str = "server";

#[derive(Debug, Default)]
struct Server {
    clients: HashMap<ClientId, Client>,
    channels: HashMap<String, Channel>,
}

impl Server {
    fn disconnect_client(&mut self, client_id: ClientId) -> DisconnectResult {
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
enum NickResult {
    Changed { registered_nickname: Option<String> },
    NicknameInUse,
}

#[derive(Debug)]
enum PartResult {
    Parted {
        nickname: String,
        senders: Vec<mpsc::Sender<String>>,
    },
    NotOnChannel {
        nickname: String,
    },
    NoSuchChannel {
        nickname: String,
    },
}

#[derive(Debug)]
enum PrivmsgResult {
    Channel {
        message: String,
        senders: Vec<mpsc::Sender<String>>,
    },
    User {
        message: String,
        sender: mpsc::Sender<String>,
    },
    NotRegistered,
    NoSuchChannel {
        nickname: String,
        channel: String,
    },
    NotOnChannel {
        nickname: String,
        channel: String,
    },
    NoSuchNick {
        nickname: String,
        target: String,
    },
}

#[derive(Debug)]
enum TopicResult {
    Changed {
        nickname: String,
        topic: String,
        senders: Vec<mpsc::Sender<String>>,
    },
    Queried {
        nickname: String,
        topic: Option<String>,
    },
    NotRegistered,
    NoSuchChannel {
        nickname: String,
        channel: String,
    },
    NotOnChannel {
        nickname: String,
        channel: String,
    },
}

#[derive(Debug)]
struct DisconnectResult {
    nickname: Option<String>,
    senders: Vec<mpsc::Sender<String>>,
}

#[derive(Debug)]
struct ListEntry {
    name: String,
    member_count: usize,
    topic: Option<String>,
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

async fn handle_client(
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

async fn handle_command(
    command: Command,
    client_id: ClientId,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<bool> {
    match command {
        Command::List(channel) => {
            handle_list(client_id, channel.as_deref(), server, sender).await?
        }
        Command::Names(channel) => handle_names(client_id, &channel, server, sender).await?,
        Command::Nick(nickname) => handle_nick(client_id, &nickname, server, sender).await?,
        Command::Notice { target, message } => {
            handle_notice(client_id, &target, &message, server).await?
        }
        Command::User { username, realname } => {
            handle_user(client_id, &username, &realname, server, sender).await?
        }
        Command::Join(channel) => handle_join(client_id, &channel, server, sender).await?,
        Command::Part(channel) => handle_part(client_id, &channel, server, sender).await?,
        Command::Privmsg { target, message } => {
            handle_privmsg(client_id, &target, &message, server, sender).await?
        }
        Command::Ping(token) => sender.send(format!("PONG :{token}")).await?,
        Command::Pong(token) => debug!(%client_id, %token, "PONG received"),
        Command::Quit => return Ok(false),
        Command::Topic { channel, topic } => {
            handle_topic(client_id, &channel, topic.as_deref(), server, sender).await?
        }
        Command::NeedMoreParams(command) => {
            let nickname = {
                let server = server.read().await;

                server
                    .clients
                    .get(&client_id)
                    .and_then(|client| client.nickname.clone())
                    .unwrap_or_else(|| "*".to_string())
            };

            sender.send(format!(":{SERVER_NAME} {ERR_NEEDMOREPARAMS} {nickname} {command} :Not enough parameters")).await?;
        }
        Command::Unknown(command) => debug!(%client_id, %command, "Unknown command"),
    }

    Ok(true)
}

async fn handle_list(
    client_id: ClientId,
    channel_name: Option<&str>,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<()> {
    let (nickname, mut entries) = {
        let server = server.read().await;

        let nickname = match server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
        {
            Some(nickname) => nickname,
            None => {
                sender
                    .send(format!(
                        ":{SERVER_NAME} {ERR_NOTREGISTERED} * :You have not registered"
                    ))
                    .await?;

                return Ok(());
            }
        };

        let entries: Vec<ListEntry> = match channel_name {
            Some(channel_name) => server
                .channels
                .get(channel_name)
                .map(|channel| {
                    vec![ListEntry {
                        name: channel.name.clone(),
                        member_count: channel.members.len(),
                        topic: channel.topic.clone(),
                    }]
                })
                .unwrap_or_default(),
            None => server
                .channels
                .values()
                .map(|channel| ListEntry {
                    name: channel.name.clone(),
                    member_count: channel.members.len(),
                    topic: channel.topic.clone(),
                })
                .collect(),
        };

        (nickname, entries)
    };

    entries.sort_by(|a, b| a.name.cmp(&b.name));

    for entry in entries {
        let topic = entry.topic.unwrap_or_default();

        sender
            .send(format!(
                ":{SERVER_NAME} {RPL_LIST} {nickname} {} {} :{topic}",
                entry.name, entry.member_count
            ))
            .await?;
    }

    sender
        .send(format!(
            ":{SERVER_NAME} {RPL_LISTEND} {nickname} :End of /LIST"
        ))
        .await?;

    Ok(())
}

async fn handle_names(
    client_id: ClientId,
    channel_name: &str,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<()> {
    let nickname = {
        let server = server.read().await;

        match server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
        {
            Some(nickname) => nickname,
            None => {
                drop(server);

                sender
                    .send(format!(
                        ":{SERVER_NAME} {ERR_NOTREGISTERED} * :You have not registered"
                    ))
                    .await?;
                return Ok(());
            }
        }
    };

    let mut member_nicknames: Vec<String> = {
        let server = server.read().await;

        let channel = match server.channels.get(channel_name) {
            Some(channel) => channel,
            None => {
                drop(server);

                sender.send(format!(":{SERVER_NAME} {ERR_NOSUCHCHANNEL} {nickname} {channel_name} :No such channel")).await?;
                return Ok(());
            }
        };

        channel
            .members
            .iter()
            .filter_map(|member_id| {
                server
                    .clients
                    .get(member_id)
                    .and_then(|client| client.nickname.clone())
            })
            .collect()
    };

    member_nicknames.sort();
    let names = member_nicknames.join(" ");

    sender
        .send(format!(
            ":{SERVER_NAME} {RPL_NAMREPLY} {nickname} = {channel_name} :{names}"
        ))
        .await?;
    sender
        .send(format!(
            ":{SERVER_NAME} {RPL_ENDOFNAMES} {nickname} {channel_name} :End of /NAMES list."
        ))
        .await?;

    Ok(())
}

async fn handle_nick(
    client_id: ClientId,
    nickname: &str,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<()> {
    if nickname.is_empty() {
        sender
            .send(format!(
                ":{SERVER_NAME} {ERR_NONICKNAMEGIVEN} * :No nickname given"
            ))
            .await?;

        return Ok(());
    }

    if !is_valid_nickname(nickname) {
        sender
            .send(format!(
                ":{SERVER_NAME} {ERR_ERRONEUSNICKNAME} * {nickname} :Erroneous nickname"
            ))
            .await?;

        return Ok(());
    }

    let result = {
        let mut server = server.write().await;

        let nickname_in_use = server
            .clients
            .iter()
            .any(|(id, client)| *id != client_id && client.nickname.as_deref() == Some(nickname));

        if nickname_in_use {
            NickResult::NicknameInUse
        } else {
            let client = server
                .clients
                .get_mut(&client_id)
                .ok_or("Client not found")?;

            let was_registered = client.is_registered();

            client.nickname = Some(nickname.to_string());

            NickResult::Changed {
                registered_nickname: registration_complete(client, was_registered),
            }
        }
    };

    match result {
        NickResult::Changed {
            registered_nickname: Some(nickname),
        } => {
            sender
                .send(format!(
                    ":{SERVER_NAME} {RPL_WELCOME} {nickname} :Welcome to the IRC server"
                ))
                .await?
        }
        NickResult::Changed {
            registered_nickname: None,
        } => {}
        NickResult::NicknameInUse => {
            sender
                .send(format!(
                    ":{SERVER_NAME} {ERR_NICKNAMEINUSE} * {nickname} :Nickname is already in use"
                ))
                .await?
        }
    }

    Ok(())
}

async fn handle_notice(
    client_id: ClientId,
    target: &str,
    message: &str,
    server: Arc<RwLock<Server>>,
) -> Result<()> {
    let nickname = {
        let server = server.read().await;

        match server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
        {
            Some(nickname) => nickname,
            None => return Ok(()),
        }
    };

    let message = format!(":{nickname} NOTICE {target} :{message}");

    if target.starts_with('#') {
        let senders: Vec<mpsc::Sender<String>> = {
            let server = server.read().await;

            let channel = match server.channels.get(target) {
                Some(channel) => channel,
                None => return Ok(()),
            };

            if !channel.members.contains(&client_id) {
                return Ok(());
            }

            channel
                .members
                .iter()
                .filter_map(|member_id| {
                    server
                        .clients
                        .get(member_id)
                        .map(|client| client.sender.clone())
                })
                .collect()
        };

        for sender in senders {
            sender.send(message.clone()).await?;
        }
    } else {
        let target_sender = {
            let server = server.read().await;

            match server
                .clients
                .values()
                .find(|client| client.nickname.as_deref() == Some(target))
                .map(|client| client.sender.clone())
            {
                Some(sender) => sender,
                None => return Ok(()),
            }
        };

        target_sender.send(message).await?;
    }

    Ok(())
}

async fn handle_user(
    client_id: ClientId,
    username: &str,
    realname: &str,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<()> {
    let registered_nickname = {
        let mut server = server.write().await;

        let client = server
            .clients
            .get_mut(&client_id)
            .ok_or("Client not found")?;

        let was_registered = client.is_registered();

        client.username = Some(username.to_string());
        client.realname = Some(realname.to_string());

        registration_complete(client, was_registered)
    };

    if let Some(nickname) = registered_nickname {
        sender
            .send(format!(
                ":{SERVER_NAME} {RPL_WELCOME} {nickname} :Welcome to the IRC server"
            ))
            .await?;
    }

    Ok(())
}

async fn handle_join(
    client_id: ClientId,
    channel_name: &str,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<()> {
    if !is_valid_channel_name(channel_name) {
        sender
            .send(format!(
                ":{SERVER_NAME} {ERR_NOSUCHCHANNEL} * {channel_name} :No such channel"
            ))
            .await?;

        return Ok(());
    }

    let (nickname, senders) = {
        let mut server = server.write().await;

        let nickname = match server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
        {
            Some(nickname) => nickname,
            None => {
                drop(server);

                sender
                    .send(format!(
                        ":{SERVER_NAME} {ERR_NOTREGISTERED} * :You have not registered"
                    ))
                    .await?;

                return Ok(());
            }
        };

        let member_ids: Vec<u64> = {
            let channel = server
                .channels
                .entry(channel_name.to_string())
                .or_insert_with(|| Channel::new(channel_name));

            let inserted = channel.members.insert(client_id);

            if !inserted {
                return Ok(());
            }

            channel.members.iter().copied().collect()
        };

        let senders: Vec<mpsc::Sender<String>> = member_ids
            .iter()
            .filter_map(|member_id| {
                server
                    .clients
                    .get(member_id)
                    .map(|client| client.sender.clone())
            })
            .collect();

        (nickname, senders)
    };

    let message = format!(":{nickname} JOIN {channel_name}");

    for sender in senders {
        sender.send(message.clone()).await?;
    }

    Ok(())
}

async fn handle_part(
    client_id: ClientId,
    channel_name: &str,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<()> {
    if !is_valid_channel_name(channel_name) {
        sender
            .send(format!(
                ":{SERVER_NAME} {ERR_NOSUCHCHANNEL} * {channel_name} :No such channel"
            ))
            .await?;

        return Ok(());
    }

    let result = {
        let mut server = server.write().await;

        let nickname = server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
            .ok_or("Client is not registered")?;

        let member_ids: Option<Vec<ClientId>> = server
            .channels
            .get(channel_name)
            .map(|channel| channel.members.iter().copied().collect());

        match member_ids {
            Some(member_ids) if !member_ids.contains(&client_id) => {
                PartResult::NotOnChannel { nickname }
            }
            Some(member_ids) => {
                let senders: Vec<mpsc::Sender<String>> = member_ids
                    .iter()
                    .filter_map(|member_id| {
                        server
                            .clients
                            .get(member_id)
                            .map(|client| client.sender.clone())
                    })
                    .collect();

                if let Some(channel) = server.channels.get_mut(channel_name) {
                    channel.members.remove(&client_id);

                    if channel.members.is_empty() {
                        server.channels.remove(channel_name);
                    }
                }

                PartResult::Parted { nickname, senders }
            }
            None => PartResult::NoSuchChannel { nickname },
        }
    };

    match result {
        PartResult::Parted { nickname, senders } => {
            let message = format!(":{nickname} PART {channel_name}");

            for sender in senders {
                sender.send(message.clone()).await?;
            }
        },
        PartResult::NotOnChannel { nickname } => sender.send(format!(":{SERVER_NAME} {ERR_NOTONCHANNEL} {nickname} {channel_name} :You're not on that channel")).await?,
        PartResult::NoSuchChannel { nickname } => sender.send(format!(":{SERVER_NAME} {ERR_NOSUCHCHANNEL} {nickname} {channel_name} :No such channel")).await?,
    }

    Ok(())
}

async fn handle_privmsg(
    client_id: ClientId,
    target: &str,
    message: &str,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<()> {
    let result: PrivmsgResult = {
        let server = server.read().await;

        if let Some(nickname) = server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
        {
            if target.starts_with('#') {
                match server.channels.get(target) {
                    None => PrivmsgResult::NoSuchChannel {
                        nickname: nickname.clone(),
                        channel: target.to_string(),
                    },
                    Some(channel) if !channel.members.contains(&client_id) => {
                        PrivmsgResult::NotOnChannel {
                            nickname: nickname.clone(),
                            channel: target.to_string(),
                        }
                    }
                    Some(channel) => {
                        let senders = channel
                            .members
                            .iter()
                            .filter_map(|member_id| {
                                server
                                    .clients
                                    .get(member_id)
                                    .map(|client| client.sender.clone())
                            })
                            .collect();

                        PrivmsgResult::Channel {
                            message: format!(":{nickname} PRIVMSG {target} :{message}"),
                            senders,
                        }
                    }
                }
            } else {
                match server
                    .clients
                    .values()
                    .find(|client| client.nickname.as_deref() == Some(target))
                {
                    Some(client) => PrivmsgResult::User {
                        message: format!(":{nickname} PRIVMSG {target} :{message}"),
                        sender: client.sender.clone(),
                    },
                    None => PrivmsgResult::NoSuchNick {
                        nickname,
                        target: target.to_string(),
                    },
                }
            }
        } else {
            PrivmsgResult::NotRegistered
        }
    };

    match result {
        PrivmsgResult::Channel { message, senders } => {
            for sender in senders {
                sender.send(message.clone()).await?;
            }
        }
        PrivmsgResult::User { message, sender } => sender.send(message).await?,
        PrivmsgResult::NotRegistered => {
            sender
                .send(format!(
                    ":{SERVER_NAME} {ERR_NOTREGISTERED} * :You have not registered"
                ))
                .await?
        }
        PrivmsgResult::NoSuchChannel { nickname, channel } => {
            sender
                .send(format!(
                    ":{SERVER_NAME} {ERR_NOSUCHCHANNEL} {nickname} {channel} :No such channel"
                ))
                .await?
        }
        PrivmsgResult::NotOnChannel { nickname, channel } => sender
            .send(format!(
                ":{SERVER_NAME} {ERR_NOTONCHANNEL} {nickname} {channel} :You're not on that channel"
            ))
            .await?,
        PrivmsgResult::NoSuchNick { nickname, target } => {
            sender
                .send(format!(
                    ":{SERVER_NAME} {ERR_NOSUCHNICK} {nickname} {target} :No such nick"
                ))
                .await?
        }
    }

    Ok(())
}

async fn handle_topic(
    client_id: ClientId,
    channel_name: &str,
    topic: Option<&str>,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<()> {
    if !is_valid_channel_name(channel_name) {
        sender
            .send(format!(
                ":{SERVER_NAME} {ERR_NOSUCHCHANNEL} * {channel_name} :No such channel"
            ))
            .await?;

        return Ok(());
    }

    let result: TopicResult = {
        let mut server = server.write().await;

        if let Some(nickname) = server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
        {
            if let Some(channel) = server.channels.get(channel_name) {
                let member_ids: Vec<ClientId> = channel.members.iter().copied().collect();

                if member_ids.contains(&client_id) {
                    match topic {
                        Some(topic) => {
                            if let Some(channel) = server.channels.get_mut(channel_name) {
                                channel.topic = Some(topic.to_string());
                            }

                            let senders: Vec<mpsc::Sender<String>> = member_ids
                                .iter()
                                .filter_map(|member_id| {
                                    server
                                        .clients
                                        .get(member_id)
                                        .map(|client| client.sender.clone())
                                })
                                .collect();

                            TopicResult::Changed {
                                nickname,
                                topic: topic.to_string(),
                                senders,
                            }
                        }
                        None => {
                            let current_topic = server
                                .channels
                                .get(channel_name)
                                .and_then(|channel| channel.topic.clone());

                            TopicResult::Queried {
                                nickname,
                                topic: current_topic,
                            }
                        }
                    }
                } else {
                    TopicResult::NotOnChannel {
                        nickname,
                        channel: channel_name.to_string(),
                    }
                }
            } else {
                TopicResult::NoSuchChannel {
                    nickname,
                    channel: channel_name.to_string(),
                }
            }
        } else {
            TopicResult::NotRegistered
        }
    };

    match result {
        TopicResult::Changed {
            nickname,
            topic,
            senders,
        } => {
            let message = format!(":{nickname} TOPIC {channel_name} :{topic}");

            for sender in senders {
                sender.send(message.clone()).await?;
            }
        }
        TopicResult::Queried { nickname, topic } => {
            if let Some(topic) = topic {
                sender
                    .send(format!(
                        ":{SERVER_NAME} {RPL_TOPIC} {nickname} {channel_name} :{topic}"
                    ))
                    .await?;
            }
        }
        TopicResult::NotRegistered => {
            sender
                .send(format!(
                    ":{SERVER_NAME} {ERR_NOTREGISTERED} * :You have not registered"
                ))
                .await?
        }
        TopicResult::NoSuchChannel { nickname, channel } => {
            sender
                .send(format!(
                    ":{SERVER_NAME} {ERR_NOSUCHCHANNEL} {nickname} {channel} :No such channel"
                ))
                .await?
        }
        TopicResult::NotOnChannel { nickname, channel } => sender
            .send(format!(
                ":{SERVER_NAME} {ERR_NOTONCHANNEL} {nickname} {channel} :You're not on that channel"
            ))
            .await?,
    }

    Ok(())
}

/// 첫글자: A-Za-z, IRC special character
/// 나머지: A-Za-z0-9, IRC special character
fn is_valid_nickname(nickname: &str) -> bool {
    if nickname.is_empty() || nickname.len() > 30 {
        return false;
    }

    let mut chars = nickname.chars();

    let Some(first) = chars.next() else {
        return false;
    };

    if !first.is_ascii_alphabetic() && !"-[]\\`^{}|".contains(first) {
        return false;
    }

    chars.all(|c| c.is_ascii_alphanumeric() || "-[]\\`^{}|".contains(c))
}

fn registration_complete(client: &Client, was_registered: bool) -> Option<String> {
    if !was_registered && client.is_registered() {
        client.nickname.clone()
    } else {
        None
    }
}

fn is_valid_channel_name(channel_name: &str) -> bool {
    if channel_name.len() <= 1 || channel_name.len() > 50 {
        return false;
    }

    if !channel_name.starts_with('#') {
        return false;
    }

    !channel_name
        .chars()
        .any(|c| c == ' ' || c == ',' || c == '\0')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_nickname() {
        assert!(is_valid_nickname("alice"));
        assert!(is_valid_nickname("Alice123"));
    }

    #[test]
    fn invalid_nickname() {
        assert!(!is_valid_nickname(""));
        assert!(!is_valid_nickname("alice!"));
        assert!(!is_valid_nickname("alice bob"));
        assert!(!is_valid_nickname("123alice"));
    }

    #[test]
    fn valid_nickname_with_special_character() {
        assert!(is_valid_nickname("[alice]"));
        assert!(is_valid_nickname("alice|"));
        assert!(is_valid_nickname("^alice"));
    }

    #[test]
    fn invalid_nickname_too_long() {
        let nickname = "a".repeat(31);

        assert!(!is_valid_nickname(&nickname));
    }

    #[test]
    fn valid_channel_name() {
        assert!(is_valid_channel_name("#rust"));
        assert!(is_valid_channel_name("#general"));
        assert!(is_valid_channel_name("#rust123"));
    }

    #[test]
    fn invalid_channel_name() {
        assert!(!is_valid_channel_name(""));
        assert!(!is_valid_channel_name("#"));
        assert!(!is_valid_channel_name("rust"));
        assert!(!is_valid_channel_name("#rust chat"));
        assert!(!is_valid_channel_name("#rust,chat"));
        assert!(!is_valid_channel_name("#rust\0"));
    }

    #[test]
    fn channel_name_max_length_is_valid() {
        let channel_name = format!("#{}", "a".repeat(49));

        assert!(is_valid_channel_name(&channel_name));
    }

    #[test]
    fn invalid_channel_name_too_long() {
        let channel_name = format!("#{}", "a".repeat(50));

        assert!(!is_valid_channel_name(&channel_name));
    }

    #[test]
    fn registration_is_completed_after_nick_and_user() {
        let (sender, _) = mpsc::channel(1);
        let mut client = Client::new(sender);

        assert!(!client.is_registered());

        client.nickname = Some("alice".to_string());
        assert!(!client.is_registered());

        let was_registered = client.is_registered();

        client.username = Some("alice".to_string());

        let nickname = registration_complete(&client, was_registered);

        assert_eq!(nickname, Some("alice".to_string()));
        assert!(client.is_registered());
    }

    #[test]
    fn registration_is_not_completed_twice() {
        let (sender, _) = mpsc::channel(1);

        let mut client = Client::new(sender);
        client.nickname = Some("alice".to_string());
        client.username = Some("alice".to_string());

        let was_registered = client.is_registered();

        client.nickname = Some("bob".to_string());

        let nickname = registration_complete(&client, was_registered);

        assert_eq!(nickname, None);
    }

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

    #[test]
    fn new_channel_has_no_topic() {
        let channel = Channel::new("#rust");

        assert_eq!(channel.topic, None);
    }

    #[tokio::test]
    async fn list_returns_all_channels() {
        let (sender, mut receiver) = mpsc::channel(3);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let mut rust = Channel::new("#rust");
        rust.topic = Some("Rust programming".to_string());
        rust.members.extend([1, 2]);

        let mut general = Channel::new("#general");
        general.members.insert(1);

        server.channels.insert("#rust".to_string(), rust);
        server.channels.insert("#general".to_string(), general);

        let server = Arc::new(RwLock::new(server));

        handle_list(1, None, Arc::clone(&server), &sender)
            .await
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 322 alice #general 1 :"
        );
        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 322 alice #rust 2 :Rust programming"
        );
        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 323 alice :End of /LIST"
        );
    }

    #[tokio::test]
    async fn list_returns_requested_channel_only() {
        let (sender, mut receiver) = mpsc::channel(2);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let mut rust = Channel::new("#rust");
        rust.topic = Some("Rust programming".to_string());
        rust.members.extend([1, 2]);

        let general = Channel::new("#general");

        server.channels.insert("#rust".to_string(), rust);
        server.channels.insert("#general".to_string(), general);

        let server = Arc::new(RwLock::new(server));

        handle_list(1, Some("#rust"), Arc::clone(&server), &sender)
            .await
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 322 alice #rust 2 :Rust programming"
        );
        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 323 alice :End of /LIST"
        );
    }

    #[tokio::test]
    async fn list_unknown_channel_returns_only_end() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let server = Arc::new(RwLock::new(server));

        handle_list(1, Some("#unknown"), Arc::clone(&server), &sender)
            .await
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 323 alice :End of /LIST"
        );
    }

    #[tokio::test]
    async fn list_returns_error_when_client_is_not_registered() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();
        server.clients.insert(1, Client::new(sender.clone()));

        let server = Arc::new(RwLock::new(server));

        handle_list(1, None, Arc::clone(&server), &sender)
            .await
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 451 * :You have not registered"
        );
    }

    #[tokio::test]
    async fn names_returns_channel_members() {
        let (sender1, mut receiver1) = mpsc::channel(2);
        let (sender2, _) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2.clone());
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let mut channel = Channel::new("#rust");
        channel.members.extend([1, 2]);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_names(1, "#rust", Arc::clone(&server), &sender1)
            .await
            .unwrap();

        assert_eq!(
            receiver1.recv().await.unwrap(),
            ":server 353 alice = #rust :alice bob"
        );
        assert_eq!(
            receiver1.recv().await.unwrap(),
            ":server 366 alice #rust :End of /NAMES list."
        );
    }

    #[tokio::test]
    async fn names_returns_empty_list_for_empty_channel() {
        let (sender1, mut receiver1) = mpsc::channel(2);

        let mut server = Server::default();

        let mut alice = Client::new(sender1.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        server
            .channels
            .insert("#rust".to_string(), Channel::new("#rust"));

        let server = Arc::new(RwLock::new(server));

        handle_names(1, "#rust", Arc::clone(&server), &sender1)
            .await
            .unwrap();

        assert_eq!(
            receiver1.recv().await.unwrap(),
            ":server 353 alice = #rust :"
        );
        assert_eq!(
            receiver1.recv().await.unwrap(),
            ":server 366 alice #rust :End of /NAMES list."
        );
    }

    #[tokio::test]
    async fn names_returns_error_when_client_is_not_registered() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();
        server.clients.insert(1, Client::new(sender.clone()));

        let channel = Channel::new("#rust");
        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_names(1, "#rust", Arc::clone(&server), &sender)
            .await
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 451 * :You have not registered"
        );
    }

    #[tokio::test]
    async fn names_returns_error_when_channel_does_not_exist() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let server = Arc::new(RwLock::new(server));

        let result = handle_names(1, "#unknown", Arc::clone(&server), &sender).await;

        assert!(result.is_ok());
        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 403 alice #unknown :No such channel"
        );
    }

    #[tokio::test]
    async fn notice_is_sent_to_target_user() {
        let (sender1, mut receiver1) = mpsc::channel(1);
        let (sender2, mut receiver2) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1);
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2);
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let server = Arc::new(RwLock::new(server));

        handle_notice(1, "bob", "hello bob", Arc::clone(&server))
            .await
            .unwrap();

        assert_eq!(
            receiver2.recv().await.unwrap(),
            ":alice NOTICE bob :hello bob"
        );
        assert!(receiver1.try_recv().is_err());
    }

    #[tokio::test]
    async fn notice_is_broadcast_to_channel_members() {
        let (sender1, mut receiver1) = mpsc::channel(1);
        let (sender2, mut receiver2) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1);
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2);
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let mut channel = Channel::new("#rust");
        channel.members.extend([1, 2]);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_notice(1, "#rust", "hello rust", Arc::clone(&server))
            .await
            .unwrap();

        assert_eq!(
            receiver1.recv().await.unwrap(),
            ":alice NOTICE #rust :hello rust"
        );
        assert_eq!(
            receiver2.recv().await.unwrap(),
            ":alice NOTICE #rust :hello rust"
        );
    }

    #[tokio::test]
    async fn notice_ignores_unknown_nickname() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let server = Arc::new(RwLock::new(server));

        handle_notice(1, "bob", "hello bob", Arc::clone(&server))
            .await
            .unwrap();

        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn notice_ignores_unknown_channel() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let server = Arc::new(RwLock::new(server));

        handle_notice(1, "#unknown", "hello", Arc::clone(&server))
            .await
            .unwrap();

        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn notice_ignores_when_client_is_not_on_channel() {
        let (sender1, mut receiver1) = mpsc::channel(1);
        let (sender2, _) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2);
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let mut channel = Channel::new("#rust");
        channel.members.insert(2);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_notice(1, "#rust", "hello", Arc::clone(&server))
            .await
            .unwrap();

        assert!(receiver1.try_recv().is_err());
    }

    #[tokio::test]
    async fn join_returns_error_when_client_is_not_registered() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();
        server.clients.insert(1, Client::new(sender.clone()));

        let channel = Channel::new("#rust");
        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_join(1, "#rust", Arc::clone(&server), &sender)
            .await
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 451 * :You have not registered"
        );
    }

    #[tokio::test]
    async fn part_removes_client_from_channel() {
        let (sender1, mut receiver1) = mpsc::channel(1);
        let (sender2, mut receiver2) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2.clone());
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let mut channel = Channel::new("#rust");
        channel.members.extend([1, 2]);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_part(1, "#rust", Arc::clone(&server), &sender1)
            .await
            .unwrap();

        {
            let server = server.read().await;

            assert!(!server.channels["#rust"].members.contains(&1));
            assert!(server.channels["#rust"].members.contains(&2));
        }

        assert_eq!(receiver1.recv().await.unwrap(), ":alice PART #rust");
        assert_eq!(receiver2.recv().await.unwrap(), ":alice PART #rust");
    }

    #[tokio::test]
    async fn part_removes_empty_channel() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let mut channel = Channel::new("#rust");
        channel.members.insert(1);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_part(1, "#rust", Arc::clone(&server), &sender)
            .await
            .unwrap();

        {
            let server = server.read().await;

            assert!(!server.channels.contains_key("#rust"));
        }

        assert_eq!(receiver.recv().await.unwrap(), ":alice PART #rust");
    }

    #[tokio::test]
    async fn part_returns_error_when_client_is_not_on_channel() {
        let (sender1, mut receiver1) = mpsc::channel(1);
        let (sender2, _) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2.clone());
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let mut channel = Channel::new("#rust");
        channel.members.insert(2);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_part(1, "#rust", Arc::clone(&server), &sender1)
            .await
            .unwrap();

        let message = receiver1.recv().await.unwrap();

        assert_eq!(
            message,
            ":server 442 alice #rust :You're not on that channel"
        );

        {
            let server = server.read().await;

            assert_eq!(server.channels["#rust"].members, HashSet::from([2]));
        }
    }

    #[tokio::test]
    async fn part_returns_error_when_channel_does_not_exist() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let server = Arc::new(RwLock::new(server));

        handle_part(1, "#rust", Arc::clone(&server), &sender)
            .await
            .unwrap();

        let message = receiver.recv().await.unwrap();

        assert_eq!(message, ":server 403 alice #rust :No such channel");

        {
            let server = server.read().await;

            assert!(server.channels.is_empty());
        }
    }

    #[tokio::test]
    async fn privmsg_is_sent_to_channel_members() {
        let (sender1, mut receiver1) = mpsc::channel(1);
        let (sender2, mut receiver2) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2);
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let mut channel = Channel::new("#rust");
        channel.members.extend([1, 2]);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_privmsg(1, "#rust", "hello rust", Arc::clone(&server), &sender1)
            .await
            .unwrap();

        assert_eq!(
            receiver1.recv().await.unwrap(),
            ":alice PRIVMSG #rust :hello rust"
        );
        assert_eq!(
            receiver2.recv().await.unwrap(),
            ":alice PRIVMSG #rust :hello rust"
        );
    }

    #[tokio::test]
    async fn privmsg_is_sent_to_target_user() {
        let (sender1, mut receiver1) = mpsc::channel(1);
        let (sender2, mut receiver2) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2);
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let server = Arc::new(RwLock::new(server));

        handle_privmsg(1, "bob", "hello bob", Arc::clone(&server), &sender1)
            .await
            .unwrap();

        assert_eq!(
            receiver2.recv().await.unwrap(),
            ":alice PRIVMSG bob :hello bob"
        );
        assert!(receiver1.try_recv().is_err());
    }

    #[tokio::test]
    async fn privmsg_returns_error_when_client_is_not_on_channel() {
        let (sender1, mut receiver1) = mpsc::channel(1);
        let (sender2, _) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2);
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let mut channel = Channel::new("#rust");
        channel.members.insert(2);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        let result = handle_privmsg(1, "#rust", "hello rust", Arc::clone(&server), &sender1).await;

        assert!(result.is_ok());
        assert_eq!(
            receiver1.recv().await.unwrap(),
            ":server 442 alice #rust :You're not on that channel"
        );
    }

    #[tokio::test]
    async fn privmsg_returns_error_when_target_nickname_does_not_exist() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let server = Arc::new(RwLock::new(server));

        let result = handle_privmsg(1, "bob", "hello bob", Arc::clone(&server), &sender).await;

        assert!(result.is_ok());
        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 401 alice bob :No such nick"
        );
    }

    #[tokio::test]
    async fn privmsg_returns_error_when_channel_does_not_exist() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let server = Arc::new(RwLock::new(server));

        let result = handle_privmsg(1, "#unknown", "hello", Arc::clone(&server), &sender).await;

        assert!(result.is_ok());
        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 403 alice #unknown :No such channel"
        );
    }

    #[tokio::test]
    async fn topic_is_changed() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let mut channel = Channel::new("#rust");
        channel.members.insert(1);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_topic(
            1,
            "#rust",
            Some("Rust programming"),
            Arc::clone(&server),
            &sender,
        )
        .await
        .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            ":alice TOPIC #rust :Rust programming"
        );
        assert_eq!(
            server
                .read()
                .await
                .channels
                .get("#rust")
                .unwrap()
                .topic
                .as_deref(),
            Some("Rust programming")
        );
    }

    #[tokio::test]
    async fn topic_can_be_queried() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let mut channel = Channel::new("#rust");
        channel.members.insert(1);
        channel.topic = Some("Rust programming".to_string());

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_topic(1, "#rust", None, Arc::clone(&server), &sender)
            .await
            .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 332 alice #rust :Rust programming"
        );
    }

    #[tokio::test]
    async fn topic_returns_error_when_client_is_not_registered() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();
        server.clients.insert(1, Client::new(sender.clone()));

        let channel = Channel::new("#rust");
        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_topic(
            1,
            "#rust",
            Some("Rust programming"),
            Arc::clone(&server),
            &sender,
        )
        .await
        .unwrap();

        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 451 * :You have not registered"
        );
    }

    #[tokio::test]
    async fn topic_returns_error_when_channel_does_not_exist() {
        let (sender, mut receiver) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender.clone());
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        server.clients.insert(1, alice);

        let server = Arc::new(RwLock::new(server));

        let result = handle_topic(
            1,
            "#unknown",
            Some("Rust programming"),
            Arc::clone(&server),
            &sender,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(
            receiver.recv().await.unwrap(),
            ":server 403 alice #unknown :No such channel"
        );
    }

    #[tokio::test]
    async fn test_server_listener_can_bind_ephemeral_port() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

        let addr = listener.local_addr().unwrap();

        assert_eq!(
            addr.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
        assert_ne!(addr.port(), 0);
    }
}
