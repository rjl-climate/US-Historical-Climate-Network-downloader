mod cli;
mod deserialise;
mod download;
mod parquet;
mod reading;

use anyhow::{Error, Result};
use clap::Parser;
use cli::{command, Cli, Commands};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Daily {} => match command::daily().await {
            Ok(_) => {}
            Err(e) => eprintln!("Error: {}", e),
        },
        Commands::Monthly {} => match command::monthly().await {
            Ok(_) => {}
            Err(e) => eprintln!("Error: {}", e),
        },
    }

    Ok(())
}
