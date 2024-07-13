use std::{
    io,
    path::{Path, PathBuf},
};

use anyhow::{Error, Result};

use deserialise::process_files_in_parallel;
use download::{download_tar, extract_tar};

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
        println!("Downloading '{}'.", file_path.to_string_lossy());
        download_tar(file_path).await?;
    }

    // extract the contents if it doesn't exist
    if !working_dir.exists() {
        println!("Extracting '{}'.", file_path.to_string_lossy());
        extract_tar(file_path).await?;
    }

    // get the files to process
    let files: Vec<PathBuf> = working_dir
        .read_dir()?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let readings = process_files_in_parallel(files).await?;
    println!("Got {} readings", readings.len());

    // save to database
    db::write_readings_to_sqlite(readings, db_path).await?;

    Ok(())
}
