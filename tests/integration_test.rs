use std::time::Duration;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[tokio::test]
async fn registration_sends_welcome() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        kirc_server::run_server(listener).await.unwrap();
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = stream.into_split();

    let mut reader = BufReader::new(reader);
    let mut buf = String::new();

    writer.write_all(b"NICK alice\r\n").await.unwrap();
    writer
        .write_all(b"USER alice 0 * :Alice\r\n")
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        buf.trim_end(),
        ":server 001 alice :Welcome to the IRC server"
    );
}

#[tokio::test]
async fn clients_can_join_channel_and_exchange_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        kirc_server::run_server(listener).await.unwrap();
    });

    let alice = TcpStream::connect(addr).await.unwrap();
    let (alice_reader, mut alice_writer) = alice.into_split();
    let mut alice_reader = BufReader::new(alice_reader);

    let bob = TcpStream::connect(addr).await.unwrap();
    let (bob_reader, mut bob_writer) = bob.into_split();
    let mut bob_reader = BufReader::new(bob_reader);

    let mut buf = String::new();

    // Alice registration
    alice_writer.write_all(b"NICK alice\r\n").await.unwrap();
    alice_writer
        .write_all(b"USER alice 0 * :Alice\r\n")
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), alice_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        buf.trim_end(),
        ":server 001 alice :Welcome to the IRC server"
    );

    // Bob registration
    bob_writer.write_all(b"NICK bob\r\n").await.unwrap();
    bob_writer
        .write_all(b"USER bob 0 * :Bob\r\n")
        .await
        .unwrap();

    buf.clear();
    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(buf.trim_end(), ":server 001 bob :Welcome to the IRC server");

    // Alice joins the channel
    alice_writer.write_all(b"JOIN #rust\r\n").await.unwrap();

    buf.clear();
    tokio::time::timeout(Duration::from_secs(1), alice_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(buf.trim_end(), ":alice JOIN #rust");

    // Bob joins the same channel
    bob_writer.write_all(b"JOIN #rust\r\n").await.unwrap();

    // Bob receives his own JOIN
    buf.clear();
    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(buf.trim_end(), ":bob JOIN #rust");

    // Alice receivers Bob's JOIN
    buf.clear();
    tokio::time::timeout(Duration::from_secs(1), alice_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(buf.trim_end(), ":bob JOIN #rust");

    // Alice sends a channel message
    alice_writer
        .write_all(b"PRIVMSG #rust :hello bob\r\n")
        .await
        .unwrap();

    // Alice receivers her own PRIVMSG
    buf.clear();
    tokio::time::timeout(Duration::from_secs(1), alice_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(buf.trim_end(), ":alice PRIVMSG #rust :hello bob");

    // Bob receives Alice's message
    buf.clear();
    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(buf.trim_end(), ":alice PRIVMSG #rust :hello bob");
}

#[tokio::test]
async fn part_and_disconnect_broadcast_to_channel_members() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        kirc_server::run_server(listener).await.unwrap();
    });

    let alice = TcpStream::connect(addr).await.unwrap();
    let (alice_reader, mut alice_writer) = alice.into_split();
    let mut alice_reader = BufReader::new(alice_reader);

    let bob = TcpStream::connect(addr).await.unwrap();
    let (bob_reader, mut bob_writer) = bob.into_split();
    let mut bob_reader = BufReader::new(bob_reader);

    let mut alice_buf = String::new();
    let mut bob_buf = String::new();

    // Alice registration
    alice_writer.write_all(b"NICK alice\r\n").await.unwrap();
    alice_writer
        .write_all(b"USER alice 0 * :Alice\r\n")
        .await
        .unwrap();

    tokio::time::timeout(
        Duration::from_secs(1),
        alice_reader.read_line(&mut alice_buf),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        alice_buf.trim_end(),
        ":server 001 alice :Welcome to the IRC server"
    );

    // Bob registration
    bob_writer.write_all(b"NICK bob\r\n").await.unwrap();
    bob_writer
        .write_all(b"USER bob 0 * :Bob\r\n")
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut bob_buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        bob_buf.trim_end(),
        ":server 001 bob :Welcome to the IRC server"
    );

    // Alice joins the channel
    alice_writer.write_all(b"JOIN #rust\r\n").await.unwrap();

    alice_buf.clear();
    tokio::time::timeout(
        Duration::from_secs(1),
        alice_reader.read_line(&mut alice_buf),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(alice_buf.trim_end(), ":alice JOIN #rust");

    // Bob joins the channel
    bob_writer.write_all(b"JOIN #rust\r\n").await.unwrap();

    bob_buf.clear();
    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut bob_buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(bob_buf.trim_end(), ":bob JOIN #rust");

    alice_buf.clear();
    tokio::time::timeout(
        Duration::from_secs(1),
        alice_reader.read_line(&mut alice_buf),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(alice_buf.trim_end(), ":bob JOIN #rust");

    // Alice leaves the channel
    alice_writer.write_all(b"PART #rust\r\n").await.unwrap();

    // Alice receives her own PART
    alice_buf.clear();
    tokio::time::timeout(
        Duration::from_secs(1),
        alice_reader.read_line(&mut alice_buf),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(alice_buf.trim_end(), ":alice PART #rust");

    // Bob receives Alice's PART
    bob_buf.clear();
    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut bob_buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(bob_buf.trim_end(), ":alice PART #rust");

    // Alice joins again so that we can test disconnect cleanup
    alice_writer.write_all(b"JOIN #rust\r\n").await.unwrap();

    alice_buf.clear();
    tokio::time::timeout(
        Duration::from_secs(1),
        alice_reader.read_line(&mut alice_buf),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(alice_buf.trim_end(), ":alice JOIN #rust");

    bob_buf.clear();
    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut bob_buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(bob_buf.trim_end(), ":alice JOIN #rust");

    // Disconnect Alice
    alice_writer.shutdown().await.unwrap();

    // Bob must receive Alice's QUIT
    bob_buf.clear();
    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut bob_buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(bob_buf.trim_end(), ":alice QUIT :Client Quit");
}

#[tokio::test]
async fn missing_parameters_return_need_more_params() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        kirc_server::run_server(listener).await.unwrap();
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = stream.into_split();

    let mut reader = BufReader::new(reader);
    let mut buf = String::new();

    // NICK without a parameter
    writer.write_all(b"NICK\r\n").await.unwrap();

    tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(buf.trim_end(), ":server 461 * NICK :Not enough parameters");
}

#[tokio::test]
async fn clients_can_send_private_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        kirc_server::run_server(listener).await.unwrap();
    });

    let alice = TcpStream::connect(addr).await.unwrap();
    let (alice_reader, mut alice_writer) = alice.into_split();
    let mut alice_reader = BufReader::new(alice_reader);

    let bob = TcpStream::connect(addr).await.unwrap();
    let (bob_reader, mut bob_writer) = bob.into_split();
    let mut bob_reader = BufReader::new(bob_reader);

    let mut buf = String::new();

    // Alice registration
    alice_writer.write_all(b"NICK alice\r\n").await.unwrap();
    alice_writer
        .write_all(b"USER alice 0 * :Alice\r\n")
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), alice_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        buf.trim_end(),
        ":server 001 alice :Welcome to the IRC server"
    );

    // Bob registration
    bob_writer.write_all(b"NICK bob\r\n").await.unwrap();
    bob_writer
        .write_all(b"USER bob 0 * :Bob\r\n")
        .await
        .unwrap();

    buf.clear();
    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(buf.trim_end(), ":server 001 bob :Welcome to the IRC server");

    // Alice sends a private message to Bob
    alice_writer
        .write_all(b"PRIVMSG bob :hello privately\r\n")
        .await
        .unwrap();

    buf.clear();
    tokio::time::timeout(Duration::from_secs(1), bob_reader.read_line(&mut buf))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(buf.trim_end(), ":alice PRIVMSG bob :hello privately");
}
