mod channel;
mod client;
mod command;
mod handler;
mod server;

pub use server::run_server;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ClientId = u64;
