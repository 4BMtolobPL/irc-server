use tokio::{signal, sync::watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listen_addr = "0.0.0.0:6667";
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;

    println!("IRC server listening on {listen_addr}");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let server_task = tokio::spawn(kirc_server::run_server(listener, shutdown_rx));

    signal::ctrl_c().await?;

    println!("Shutdown signal received");

    shutdown_tx.send(true)?;
    server_task.await??;

    println!("IRC server stopped");

    Ok(())
}
