//! **ushcn** is a command line tool to download US Historical Climatology Network data.
//!
//! NOAA's [US Historical Climatology Network](https://www.ncei.noaa.gov/products/land-based-station/us-historical-climatology-network)
//! (USHCN) is a high-quality dataset of daily and monthly climate measurements from over
//! 1200 weather stations across the United States. The dataset includes measurements of
//! temperature, precipitation, and other climate variables.
//!
//! It is, however, a bit of a pain to download and work with. This tool aims to make it easier by
//! providing a simple command line interface to download daily and monthly data and save it
//! in a compressed format ([parquet](https://docs.rs/parquet/52.1.0/parquet/))
//! that is easy to work with.
//!
//! # Example
//!
//! ```rust,no_run
//! > ushcn daily
//! Downloading
//! Unpacking
//! ...
//! File saved to `/Users/richardlyon/ushcn-daily-2024-07-16.parquet`
//! ```

use anyhow::{Error, Result};
use clap::Parser;

use cli::{Cli, command, Commands};

mod cli;
mod deserialise;
mod download;
mod parquet;
mod reading;

#[tokio::main]
/// The command line utility.
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
        Commands::Stations {} => match command::stations().await {
            Ok(filename) => println!("Stations saved to `{}`", filename),
            Err(e) => eprintln!("Error: {}", e),
        },
    }

    Ok(())
}
