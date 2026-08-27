use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{RwLock, mpsc},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type ClientId = u64;

const RPL_WELCOME: &str = "001";

// const RPL_TOPIC: &str = "332";
// const RPL_TOPICTIME: &str = "333";
// const RPL_NAMREPLY: &str = "353";

const ERR_NOSUCHCHANNEL: &str = "403";
// const ERR_TOOMANYCHANNELS: &str = "405";
const ERR_NOTONCHANNEL: &str = "442";
// const ERR_NEEDMOREPARAMS: &str = "461";
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
struct Client {
    nickname: Option<String>,
    username: Option<String>,
    realname: Option<String>,
    sender: mpsc::Sender<String>,
}

impl Client {
    fn new(sender: mpsc::Sender<String>) -> Self {
        Self {
            nickname: None,
            username: None,
            realname: None,
            sender,
        }
    }

    /// NICK + USER -> registration
    fn is_registered(&self) -> bool {
        self.nickname.is_some() && self.username.is_some()
    }
}

#[derive(Debug)]
struct Channel {
    name: String,
    members: HashSet<ClientId>,
}

impl Channel {
    fn new(name: String) -> Self {
        Self {
            name,
            members: HashSet::new(),
        }
    }
}

#[derive(Debug)]
enum Command {
    Nick(String),
    Notice { target: String, message: String },
    User { username: String, realname: String },
    Join(String),
    Part(String),
    Privmsg { target: String, message: String },
    Ping(String),
    Pong(String),
    Quit,
    Unknown(String),
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
struct DisconnectResult {
    nickname: Option<String>,
    senders: Vec<mpsc::Sender<String>>,
}

fn parse_command(line: &str) -> Command {
    let mut parts = line.splitn(2, ' ');

    let command = parts.next().unwrap_or("").to_uppercase();
    let rest = parts.next().unwrap_or("").trim();

    match command.as_str() {
        "NICK" => Command::Nick(rest.to_string()),
        "NOTICE" => {
            let mut parts = rest.splitn(2, ' ');

            let target = parts.next().unwrap_or("").to_string();

            let message = parts
                .next()
                .unwrap_or("")
                .trim_start_matches(':')
                .to_string();

            Command::Notice { target, message }
        }
        "USER" => {
            let mut parts = rest.splitn(4, ' ');

            let username = parts.next().unwrap_or("").to_string();

            // hostname
            let _ = parts.next();

            // servername
            let _ = parts.next();

            let realname = parts
                .next()
                .unwrap_or("")
                .trim_start_matches(':')
                .to_string();

            Command::User { username, realname }
        }
        "JOIN" => Command::Join(rest.to_string()),
        "PART" => Command::Part(rest.to_string()),
        "PRIVMSG" => {
            let mut parts = rest.splitn(2, ' ');

            let target = parts.next().unwrap_or("").to_string();

            let message = parts
                .next()
                .unwrap_or("")
                .trim_start_matches(':')
                .to_string();

            Command::Privmsg { target, message }
        }
        "PING" => Command::Ping(rest.trim_start_matches(':').to_string()),
        "PONG" => Command::Pong(rest.trim_start_matches(':').to_string()),
        "QUIT" => Command::Quit,
        _ => Command::Unknown(line.to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listen_addr = "0.0.0.0:6667";
    let listener = TcpListener::bind(listen_addr).await?;

    let server = Arc::new(RwLock::new(Server::default()));
    let mut next_client_id: ClientId = 1;

    println!("IRC server listening on {listen_addr}");

    loop {
        let (stream, addr) = listener.accept().await?;

        let client_id = next_client_id;
        next_client_id += 1;

        println!("Client {client_id} connected: {addr}");

        let server = Arc::clone(&server);

        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, client_id, server).await {
                eprintln!("Client {client_id} error: {err}");
            }
        });
    }
}

async fn handle_client(
    stream: TcpStream,
    client_id: ClientId,
    server: Arc<RwLock<Server>>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();

    let (sender, mut receiver) = mpsc::channel::<String>(100);

    {
        let mut server = server.write().await;

        server
            .clients
            .insert(client_id, Client::new(sender.clone()));
    }

    sender.send("Welcome to my IRC server!".to_string()).await?;

    let read_task = async {
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            let command = parse_command(&line);

            let should_continue =
                handle_command(command, client_id, Arc::clone(&server), &sender).await?;

            if !should_continue {
                break;
            }
        }

        Ok::<(), Box<dyn std::error::Error>>(())
    };

    let write_task = async {
        while let Some(message) = receiver.recv().await {
            writer.write_all(message.as_bytes()).await?;
            writer.write_all(b"\r\n").await?;
        }

        Ok::<(), Box<dyn std::error::Error>>(())
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

    println!("Client {client_id} disconnected");

    Ok(())
}

async fn handle_command(
    command: Command,
    client_id: ClientId,
    server: Arc<RwLock<Server>>,
    sender: &mpsc::Sender<String>,
) -> Result<bool> {
    match command {
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
            handle_privmsg(client_id, &target, &message, server).await?
        }
        Command::Ping(token) => sender.send(format!("PONG :{token}")).await?,
        Command::Pong(token) => println!("[{client_id}] PONG {token}"),
        Command::Quit => return Ok(false),
        Command::Unknown(command) => println!("[{client_id}] Unknown command: {command}"),
    }

    Ok(true)
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

        server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
            .ok_or("Client is not registered")?
    };

    let message = format!(":{nickname} NOTICE {target} :{message}");

    if target.starts_with('#') {
        let senders: Vec<mpsc::Sender<String>> = {
            let server = server.read().await;

            let channel = server.channels.get(target).ok_or("No such channel")?;

            if !channel.members.contains(&client_id) {
                return Err("Client is not on channel".into());
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

            server
                .clients
                .values()
                .find(|client| client.nickname.as_deref() == Some(target))
                .map(|client| client.sender.clone())
                .ok_or("No such nickname")?
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

        let nickname = server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
            .ok_or("Client is not registered")?;

        let member_ids: Vec<u64> = {
            let channel = server
                .channels
                .entry(channel_name.to_string())
                .or_insert_with(|| Channel::new(channel_name.to_string()));

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
) -> Result<()> {
    let nickname = {
        let server = server.read().await;

        server
            .clients
            .get(&client_id)
            .filter(|client| client.is_registered())
            .and_then(|client| client.nickname.clone())
            .ok_or("Client is not registered")?
    };

    let message = format!(":{nickname} PRIVMSG {target} :{message}");

    if target.starts_with('#') {
        let senders: Vec<mpsc::Sender<String>> = {
            let server = server.read().await;

            let channel = server.channels.get(target).ok_or("No such channel")?;

            if !channel.members.contains(&client_id) {
                return Err("Client is not on channel".into());
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

            server
                .clients
                .values()
                .find(|client| client.nickname.as_deref() == Some(target))
                .map(|client| client.sender.clone())
                .ok_or("No such nickname")?
        };

        target_sender.send(message).await?;
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
    use std::assert_matches;

    use super::*;

    #[test]
    fn parse_nick() {
        let command = parse_command("NICK alice");

        assert_matches!(command, Command::Nick(nickname) if nickname == "alice");
    }

    #[test]
    fn parse_notice() {
        let command = parse_command("NOTICE bob :hello");

        assert_matches!(command, Command::Notice { target, message } if target == "bob" && message == "hello");
    }

    #[test]
    fn parse_user() {
        let command = parse_command("USER alice 0 * :Alice Smith");

        assert_matches!(command, Command::User { username, realname } if username == "alice" && realname == "Alice Smith")
    }

    #[test]
    fn parse_join() {
        let command = parse_command("JOIN #rust");

        assert_matches!(command, Command::Join(channel) if channel == "#rust");
    }

    #[test]
    fn parse_privmsg() {
        let command = parse_command("PRIVMSG #rust :hello");

        assert_matches!(command, Command::Privmsg { target, message } if target == "#rust" && message == "hello");
    }

    #[test]
    fn parse_privmsg_preserves_message_spaces() {
        let command = parse_command("PRIVMSG #rust :hello rust server");

        assert_matches!(command, Command::Privmsg { target, message } if target == "#rust" && message == "hello rust server");
    }

    #[test]
    fn parse_privmsg_is_case_insensitive() {
        let command = parse_command("privmsg #rust :hello");

        assert_matches!(command, Command::Privmsg { target, message } if target == "#rust" && message == "hello");
    }

    #[test]
    fn parse_part() {
        let command = parse_command("PART #rust");

        assert_matches!(command, Command::Part(channel) if channel == "#rust");
    }

    #[test]
    fn parse_ping() {
        let command = parse_command("PING :12345");

        assert_matches!(command, Command::Ping(token) if token == "12345")
    }

    #[test]
    fn parse_command_is_case_insensitive() {
        let command = parse_command("ping :12345");

        assert_matches!(command, Command::Ping(token) if token == "12345");
    }

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

        let mut rust = Channel::new("#rust".to_string());
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

        let mut channel = Channel::new("#rust".to_string());
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

        let mut channel = Channel::new("#rust".to_string());
        channel.members.insert(1);
        channel.members.insert(2);

        server.channels.insert("#rust".to_string(), channel);

        server.disconnect_client(1);

        assert!(server.channels.contains_key("#rust"));
        assert_eq!(server.channels["#rust"].members, HashSet::from([2]));
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

        let mut channel = Channel::new("#rust".to_string());
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

        let mut channel = Channel::new("#rust".to_string());
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

        let mut channel = Channel::new("#rust".to_string());
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

        let mut channel = Channel::new("#rust".to_string());
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

        let mut alice = Client::new(sender1);
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2);
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let mut channel = Channel::new("#rust".to_string());
        channel.members.extend([1, 2]);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        handle_privmsg(1, "#rust", "hello rust", Arc::clone(&server))
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
    async fn privmsg_fails_when_client_is_not_channel_member() {
        let (sender1, _) = mpsc::channel(1);
        let (sender2, _) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender1);
        alice.nickname = Some("alice".to_string());
        alice.username = Some("alice".to_string());

        let mut bob = Client::new(sender2);
        bob.nickname = Some("bob".to_string());
        bob.username = Some("bob".to_string());

        server.clients.insert(1, alice);
        server.clients.insert(2, bob);

        let mut channel = Channel::new("#rust".to_string());
        channel.members.insert(2);

        server.channels.insert("#rust".to_string(), channel);

        let server = Arc::new(RwLock::new(server));

        let result = handle_privmsg(1, "#rust", "hello rust", Arc::clone(&server)).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn privmsg_is_sent_to_target_user() {
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

        handle_privmsg(1, "bob", "hello bob", Arc::clone(&server))
            .await
            .unwrap();

        assert_eq!(
            receiver2.recv().await.unwrap(),
            ":alice PRIVMSG bob :hello bob"
        );
        assert!(receiver1.try_recv().is_err());
    }

    #[tokio::test]
    async fn privmsg_fails_when_target_nickname_does_not_exist() {
        let (sender, _) = mpsc::channel(1);

        let mut server = Server::default();

        let mut alice = Client::new(sender);
        alice.nickname = Some("alcie".to_string());
        alice.username = Some("alcie".to_string());

        server.clients.insert(1, alice);

        let server = Arc::new(RwLock::new(server));

        let result = handle_privmsg(1, "bob", "hello bob", Arc::clone(&server)).await;

        assert!(result.is_err());
    }
}
