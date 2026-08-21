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
    fn remove_client(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);

        for channel in self.channels.values_mut() {
            channel.members.remove(&client_id);
        }

        self.channels
            .retain(|_, channel| !channel.members.is_empty());
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
    User { username: String, realname: String },
    Join(String),
    Part(String),
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

fn parse_command(line: &str) -> Command {
    let mut parts = line.splitn(2, ' ');

    let command = parts.next().unwrap_or("").to_uppercase();
    let rest = parts.next().unwrap_or("").trim();

    match command.as_str() {
        "NICK" => Command::Nick(rest.to_string()),
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
        let mut server = server.write().await;
        server.remove_client(client_id);
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
        Command::User { username, realname } => {
            handle_user(client_id, &username, &realname, server, sender).await?
        }
        Command::Join(channel) => handle_join(client_id, &channel, server, sender).await?,
        Command::Part(channel) => handle_part(client_id, &channel, server, sender).await?,
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

        // 채널 존재 여부 확인
        if let Some(channel) = server.channels.get_mut(channel_name) {
            let is_on_channel = channel.members.remove(&client_id);
    
            // 멤버 part 및 ID 수집
            if is_on_channel {
                let member_ids: Vec<ClientId> = channel.members.iter().copied().collect();
                let is_empty = channel.members.is_empty();
                // 여기서 channel의 reference 사용 종료
    
                let senders: Vec<mpsc::Sender<String>> = member_ids
                    .iter()
                    .filter_map(|member_id| {
                        server
                            .clients
                            .get(member_id)
                            .map(|client| client.sender.clone())
                    })
                    .collect();
    
                // 채널이 비어있으면 server.channels에서 제거
                if is_empty {
                    server.channels.remove(channel_name);
                }
    
                PartResult::Parted { nickname, senders }
            } else {
                PartResult::NotOnChannel { nickname }
            }
        } else {
            PartResult::NoSuchChannel { nickname }
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
    fn remove_client_removes_client_from_channels() {
        let (sender, _) = mpsc::channel(1);

        let mut server = Server::default();

        server.clients.insert(1, Client::new(sender));

        let mut rust = Channel::new("#rust".to_string());
        rust.members.insert(1);
        rust.members.insert(2);

        let mut general = Channel::new("#general".to_string());
        general.members.insert(1);
        general.members.insert(3);

        server.channels.insert("#rust".to_string(), rust);
        server.channels.insert("#general".to_string(), general);

        server.remove_client(1);

        assert!(!server.clients.contains_key(&1));

        assert!(!server.channels["#rust"].members.contains(&1));
        assert!(server.channels["#rust"].members.contains(&2));

        assert!(!server.channels["#general"].members.contains(&1));
        assert!(server.channels["#general"].members.contains(&3));
    }

    #[test]
    fn remove_client_removes_empty_channels() {
        let (sender, _) = mpsc::channel(1);

        let mut server = Server::default();

        server.clients.insert(1, Client::new(sender));

        let mut channel = Channel::new("#rust".to_string());
        channel.members.insert(1);

        server.channels.insert("#rust".to_string(), channel);

        server.remove_client(1);

        assert!(!server.clients.contains_key(&1));
        assert!(!server.channels.contains_key("#rust"));
    }

    #[test]
    fn remove_client_keeps_non_empty_channels() {
        let (sender, _) = mpsc::channel(1);

        let mut server = Server::default();

        server.clients.insert(1, Client::new(sender));

        let mut channel = Channel::new("#rust".to_string());
        channel.members.insert(1);
        channel.members.insert(2);

        server.channels.insert("#rust".to_string(), channel);

        server.remove_client(1);

        assert!(server.channels.contains_key("#rust"));
        assert_eq!(server.channels["#rust"].members, HashSet::from([2]));
    }
}
