use std::{io, path::PathBuf};

use anyhow::Result;
use chrono::{Datelike, Local};
use temp_dir::TempDir;

use crate::{
    cli::spinner,
    deserialise::deserialise,
    download::{download_tar, extract_tar},
    parquet,
};

pub async fn daily() -> Result<()> {
    // download the file
    let url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd_hcn.tar.gz";
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.child("ghcnd_hcn.tar.gz");

    let bar = spinner("Downloading...".to_string());
    download_tar(url, file_path.clone()).await?;
    bar.finish_with_message("Downloaded");

    // extract the contents
    let binding = TempDir::with_prefix("GHCN").unwrap();
    let working_dir = binding.path();

    let bar = spinner("Unpacking...".to_string());
    extract_tar(&file_path, working_dir).await?;
    bar.finish_with_message("Unpacked");

    // get the files to process
    let files: Vec<PathBuf> = working_dir
        .join("ghcnd_hcn")
        .read_dir()?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let readings = deserialise(files).await?;

    // save to parquet file
    let today = Local::now();
    let file_name = format!(
        "ushcn-daily-{}-{:02}-{:02}.parquet",
        today.year(),
        today.month(),
        today.day()
    );

    let db_path = dirs::home_dir().unwrap().join(file_name);
    parquet::save_daily(&readings, &db_path)?;

    println!("File saved to `{}`", db_path.to_string_lossy());

    Ok(())
}
