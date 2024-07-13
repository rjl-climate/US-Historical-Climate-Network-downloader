use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Error, Result};

use deserialise::process_files_in_parallel;
use download::{download_tar, extract_tar};
use indicatif::ProgressBar;

mod db;
mod deserialise;
mod download;
mod reading;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let file_path = Path::new("ghcnd_hcn.tar.gz");
    let db_path = PathBuf::from("ghcnd_hcn.sqlite");
    let working_dir = Path::new("ghcnd_hcn");

    // download the file if it doesn't exist
    if !file_path.exists() {
        let message = format!("Downloading '{}'.", file_path.to_string_lossy());
        let bar = ProgressBar::new_spinner().with_message(message);
        bar.enable_steady_tick(Duration::from_millis(100));
        download_tar(file_path).await?;
        bar.finish();
    }

    // extract the contents if it doesn't exist
    if !working_dir.exists() {
        let message = format!("Extracting '{}'.", file_path.to_string_lossy());
        let bar = ProgressBar::new_spinner().with_message(message);
        bar.enable_steady_tick(Duration::from_millis(100));
        extract_tar(file_path).await?;
        bar.finish();
    }

    // get the files to process
    let files: Vec<PathBuf> = working_dir
        .read_dir()?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let readings = process_files_in_parallel(files).await?;

    // save to database
    db::write_readings_to_sqlite(readings, db_path).await?;

    Ok(())
}
