use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Error, Result};

use deserialise::deserialise;
use download::{download_tar, extract_tar};
use indicatif::ProgressBar;

mod db;
mod deserialise;
mod download;
mod reading;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let file_path = Path::new("ghcnd_hcn.tar.gz");
    let db_path = "ghcnd_hcn";
    let working_dir = Path::new("ghcnd_hcn");

    // download the file if it doesn't exist
    if !file_path.exists() {
        let bar = spinner(format!("Downloading '{}'.", file_path.to_string_lossy()));

        download_tar(file_path).await?;

        bar.finish();
    }

    // extract the contents if it doesn't exist
    if !working_dir.exists() {
        let bar = spinner("Extracting".to_string());

        extract_tar(file_path).await?;

        bar.finish();
    }

    // get the files to process
    let files: Vec<PathBuf> = working_dir
        .read_dir()?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let readings = deserialise(files).await?;

    // save to database
    db::parquet::save_parquet(&readings, db_path)?;

    Ok(())
}

fn spinner(message: String) -> ProgressBar {
    let bar = ProgressBar::new_spinner().with_message(message.clone());
    bar.enable_steady_tick(Duration::from_millis(100));
    bar
}
