use tokio::{signal, sync::watch};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("kirc_server=info")),
        )
        .init();

    let listen_addr = "0.0.0.0:6667";
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;

    info!(%listen_addr, "IRC server listening");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let server_task = tokio::spawn(kirc_server::run_server(listener, shutdown_rx));

    signal::ctrl_c().await?;

    info!("Shutdown signal received");

    shutdown_tx.send(true)?;
    server_task.await??;

    info!("IRC server stopped");

    Ok(())
}
