use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{RwLock, mpsc},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type ClientId = u64;

const ERR_NONICKNAMEGIVEN: &str = "431";
const ERR_ERRONEUSNICKNAME: &str = "432";
const ERR_NICKNAMEINUSE: &str = "433";
const SERVER_NAME: &str = "server";

#[derive(Debug, Default)]
struct Server {
    clients: HashMap<ClientId, Client>,
}

#[derive(Debug)]
struct Client {
    nickname: Option<String>,
    username: Option<String>,
    sender: mpsc::Sender<String>,
}

#[derive(Debug)]
enum Command {
    Nick(String),
    User { username: String, realname: String },
    Ping(String),
    Pong(String),
    Quit,
    Unknown(String),
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

        server.clients.insert(
            client_id,
            Client {
                nickname: None,
                username: None,
                sender: sender.clone(),
            },
        );
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
        server.clients.remove(&client_id);
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
            println!("[{client_id}] USER username={username}, realname={realname}")
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

    let nickname_in_use = {
        let mut server = server.write().await;

        let nickname_in_use = server
            .clients
            .iter()
            .any(|(id, client)| *id != client_id && client.nickname.as_deref() == Some(nickname));

        if !nickname_in_use {
            let client = server
                .clients
                .get_mut(&client_id)
                .ok_or("Client not found")?;

            client.nickname = Some(nickname.to_string());
        }

        nickname_in_use
    };

    if nickname_in_use {
        sender
            .send(format!(
                ":{SERVER_NAME} {ERR_NICKNAMEINUSE} * {nickname} :Nickname is already in use"
            ))
            .await?;
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
    fn parse_ping() {
        let command = parse_command("PING :12345");

        assert_matches!(command, Command::Ping(token) if token == "12345")
    }

    #[test]
    fn parse_user() {
        let command = parse_command("USER alice 0 * :Alice Smith");

        assert_matches!(command, Command::User { username, realname } if username == "alice" && realname == "Alice Smith")
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
}
