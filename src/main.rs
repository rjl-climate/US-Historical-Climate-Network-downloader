mod cli;
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
            Ok(filename) => println!("File saved to `{}`", filename),
            Err(e) => eprintln!("Error: {}", e),
        },
        Commands::Monthly {} => match command::monthly().await {
            Ok(filename) => println!("File saved to `{}`", filename),
            Err(e) => eprintln!("Error: {}", e),
        },
    }

    Ok(())
}
