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
//! > ushcn
//! Downloading and processing US Historical Climate Network data...
//! 
//! Processing daily data...
//! Daily: Created 1 dataset files: ushcn-daily-unknown-2024-07-16.parquet
//! 
//! Processing monthly data...
//! Monthly: Created 3 dataset files: ushcn-monthly-raw-2024-07-16.parquet, ushcn-monthly-tob-2024-07-16.parquet, ushcn-monthly-fls52-2024-07-16.parquet
//! 
//! Processing stations data...
//! Stations: ushcn-stations-2024-07-16.parquet
//! ```

use anyhow::{Error, Result};
use clap::Parser;

use cli::{Cli, command};

mod cli;
mod deserialise;
mod download;
mod parquet;
mod reading;

#[tokio::main]
/// The command line utility.
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    println!("Downloading and processing US Historical Climate Network data...\n");

    // Download USHCN stations data for monthly coordinate injection
    println!("Downloading USHCN stations data...");
    let ushcn_stations = match command::stations(cli.cache).await {
        Ok((result, stations_data)) => {
            println!("USHCN Stations: {}\n", result);
            stations_data
        },
        Err(e) => {
            eprintln!("USHCN Stations error: {}\n", e);
            return Ok(());
        }
    };

    // Download GHCN stations data for daily coordinate injection
    println!("Downloading GHCN stations data...");
    let ghcn_stations = match command::ghcn_stations(cli.cache).await {
        Ok((result, stations_data)) => {
            println!("GHCN Stations: {}\n", result);
            stations_data
        },
        Err(e) => {
            eprintln!("GHCN Stations error: {}\n", e);
            return Ok(());
        }
    };

    // Generate daily data with GHCN stations for coordinate injection
    println!("Processing daily data...");
    match command::daily(cli.cache, &ghcn_stations).await {
        Ok(result) => println!("Daily: {}\n", result),
        Err(e) => eprintln!("Daily error: {}\n", e),
    }

    // Generate monthly data with USHCN stations for coordinate injection
    println!("Processing monthly data...");
    match command::monthly(cli.cache, &ushcn_stations).await {
        Ok(result) => println!("Monthly: {}\n", result),
        Err(e) => eprintln!("Monthly error: {}\n", e),
    }

    Ok(())
}
