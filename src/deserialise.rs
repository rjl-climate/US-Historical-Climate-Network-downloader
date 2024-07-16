//! Handles deserialising a folder of readings

use anyhow::Error;
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};

use std::{
    fs::File,
    io::{self, BufRead},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::reading::Reading;

/// Load a readings file from the file system and deserialise to a Reading object
pub async fn deserialise(files: Vec<PathBuf>) -> Result<Vec<Reading>, Error> {
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
) -> Result<Vec<Reading>, Error> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);

    let mut readings = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let reading = Reading::from_line(&line)?;
        if ["PRCP", "TMAX", "TMIN"].contains(&reading.element.as_str()) {
            readings.push(reading);
        }
    }

    {
        let pb = progress_bar.lock().unwrap();
        pb.inc(1);
    }

    Ok(readings)
}
