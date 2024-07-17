use std::{
    fs::{self},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use tempfile::TempDir;

use crate::{
    cli::create_spinner,
    deserialise::deserialise,
    download::{download_tar, extract_tar},
    parquet,
};

use super::make_parquet_file_name;

pub async fn daily() -> Result<String> {
    let temp_dir = TempDir::new()?;

    let archive_filepath = download_archive(temp_dir.path()).await?;
    let archive_dir = extract_archive(&archive_filepath).await?;
    let readings = deserialise(&archive_dir).await?;

    let parquet_file_name = make_parquet_file_name("daily");
    parquet::save_daily(&readings, &parquet_file_name)?;

    Ok(parquet_file_name.to_string_lossy().to_string())
}

async fn download_archive(temp_dir: &Path) -> Result<PathBuf> {
    let url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd_hcn.tar.gz";
    let file_name = url.split('/').last().unwrap();
    let file_path = temp_dir.join(file_name);

    let bar = create_spinner("Downloading archive...".to_string());
    download_tar(url, file_path.clone()).await?;
    bar.finish_with_message("Downloaded");

    Ok(file_path)
}

async fn extract_archive(archive_filepath: &PathBuf) -> Result<PathBuf> {
    let archive_dir = archive_filepath.parent().unwrap();

    let bar = create_spinner("Unpacking archives...".to_string());
    extract_tar(archive_filepath, archive_dir).await?;
    bar.finish_with_message("Unpacked");

    let extraction_dir = get_archive_dir(archive_dir)?;

    Ok(extraction_dir)
}

// Gets the path to a directory in archive_dir if it is the only one
fn get_archive_dir(archive_dir: &Path) -> Result<PathBuf> {
    let mut directories: Vec<PathBuf> = Vec::new();

    // Read the directory
    for entry in fs::read_dir(archive_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Check if the entry is a directory
        if path.is_dir() {
            directories.push(path);
        }
    }

    if directories.len() != 1 {
        return Err(anyhow!(
            "Expected one directory in archive, found {}",
            directories.len()
        ));
    }

    Ok(directories[0].clone())
}
