use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listen_addr = "0.0.0.0:6667";
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;

    println!("IRC server listening on {listen_addr}");

    kirc_server::run_server(listener).await
}
