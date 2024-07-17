use std::{
    collections::HashSet,
    fs::{self, File},
    io::{self, BufRead},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Error, Result};
use chrono::{Datelike, Local};
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use tempfile::TempDir;

use crate::{
    cli::spinner,
    download::{download_tar, extract_tar},
    parquet,
    reading::{DailyReading, Element},
};

pub async fn daily() -> Result<String> {
    let temp_dir = TempDir::new()?;

    let parquet_file_name = make_parquet_file_name();

    let archive_filepath = download_archive(temp_dir.path()).await?;
    let archive_dir = extract_archive(&archive_filepath).await?;
    let readings = deserialise(&archive_dir).await?;

    parquet::save_daily(&readings, &parquet_file_name)?;

    Ok(parquet_file_name.to_string_lossy().to_string())
}

async fn download_archive(temp_dir: &Path) -> Result<PathBuf> {
    let url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd_hcn.tar.gz";
    let file_name = url.split('/').last().unwrap();
    let file_path = temp_dir.join(file_name);

    let bar = spinner("Downloading archive...".to_string());
    download_tar(url, file_path.clone()).await?;
    bar.finish_with_message("Downloaded");

    Ok(file_path)
}

async fn extract_archive(archive_filepath: &PathBuf) -> Result<PathBuf> {
    let archive_dir = archive_filepath.parent().unwrap();

    let bar = spinner("Unpacking archives...".to_string());
    extract_tar(archive_filepath, archive_dir).await?;
    bar.finish_with_message("Unpacked");

    let extraction_dir = get_archive_dir(archive_dir)?;

    Ok(extraction_dir)
}

/// Load a readings file from the file system and deserialise to a Reading object
pub async fn deserialise(extraction_dir: &Path) -> Result<Vec<DailyReading>, Error> {
    let files: Vec<PathBuf> = extraction_dir
        .read_dir()?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let progress_bar = Arc::new(Mutex::new(
        ProgressBar::new(files.len() as u64).with_message("Processing files"),
    ));
    progress_bar.lock().unwrap().set_style(
        ProgressStyle::with_template("[{eta_precise}] {bar:40.cyan/blue} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    let tasks: Vec<_> = files
        .iter()
        .map(|file| {
            let file = file.clone();
            let pb = Arc::clone(&progress_bar);
            tokio::spawn(async move { process_file(&file, pb).await })
        })
        .collect();

    let mut readings = Vec::new();
    for result in join_all(tasks).await {
        match result {
            Ok(Ok(file_readings)) => readings.extend(file_readings),
            Ok(Err(e)) => eprintln!("Error processing file: {:?}", e),
            Err(e) => eprintln!("Task join error: {:?}", e),
        }
    }
    progress_bar
        .lock()
        .unwrap()
        .finish_with_message("Processing complete");

    Ok(readings)
}

async fn process_file(
    file_path: &Path,
    progress_bar: Arc<Mutex<ProgressBar>>,
) -> Result<Vec<DailyReading>, Error> {
    let mut readings = Vec::new();

    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let reading = DailyReading::from_line(&line)?;
        if [Element::Prcp, Element::Tmax, Element::Tmin].contains(&reading.properties.element) {
            readings.push(reading);
        }
    }

    {
        let pb = progress_bar.lock().unwrap();
        pb.inc(1);
    }

    Ok(readings)
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

fn make_parquet_file_name() -> PathBuf {
    let today = Local::now();
    let file_name = format!(
        "ushcn-daily-{}-{:02}-{:02}.parquet",
        today.year(),
        today.month(),
        today.day()
    );

    dirs::home_dir().unwrap().join(file_name)
}
