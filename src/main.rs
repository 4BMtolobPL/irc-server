use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:6667";
    let listener = TcpListener::bind(addr).await?;

    println!("IRC server listening on {addr}");

    loop {
        let (stream, addr) = listener.accept().await?;

        println!("Client connected: {addr}");

        tokio::spawn(async move {
            if let Err(err) = handle_client(stream).await {
                eprintln!("Client error: {err}");
            }
        });
    }
}

async fn handle_client(stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        println!("Received: {line}");
    }

    Ok(())
}
