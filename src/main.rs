use std::{io, path::PathBuf, time::Duration};

use anyhow::{Error, Result};

use chrono::{Datelike, Local};
use deserialise::deserialise;
use dirs;
use download::{download_tar, extract_tar};
use indicatif::ProgressBar;
use temp_dir::TempDir;

mod deserialise;
mod download;
mod parquet;
mod reading;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // download the file if it doesn't exist
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.child("ghcnd_hcn.tar.gz");

    let bar = spinner("Downloading".to_string());
    download_tar(file_path.clone()).await?;
    bar.finish();

    // extract the contents if it doesn't exist
    let binding = TempDir::with_prefix("GHCN").unwrap();
    let working_dir = binding.path();

    let bar = spinner("Unpacking".to_string());
    extract_tar(file_path, working_dir).await?;
    bar.finish();

    // get the files to process
    let files: Vec<PathBuf> = working_dir
        .join("ghcnd_hcn")
        .read_dir()?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let readings = deserialise(files).await?;

    // save to database
    let today = Local::now();
    let file_name = format!(
        "ushcn-daily-{}-{:02}-{:02}.parquet",
        today.year(),
        today.month(),
        today.day()
    );

    let db_path = dirs::home_dir().unwrap().join(file_name);
    parquet::save(&readings, &db_path)?;

    println!("File saved to `{}`", db_path.to_string_lossy());

    Ok(())
}

fn spinner(message: String) -> ProgressBar {
    let bar = ProgressBar::new_spinner().with_message(message.clone());
    bar.enable_steady_tick(Duration::from_millis(100));
    bar
}
