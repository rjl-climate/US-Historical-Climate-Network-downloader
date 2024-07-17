//! Deserialises a folder of archive files to a Vec of Readings.
//!
//! The deserialise function is generic over the Reading trait, which is implemented by the Reading struct.

use std::fs::File;
use std::io::{self, BufRead};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::Result;

use futures::future::join_all;
use indicatif::ProgressBar;

use crate::cli::create_progress_bar;
use crate::reading::Reading;

/// Read a directory of archive files and deserialise to vec of Readings.
pub async fn deserialise<R: Reading + Send + 'static>(extraction_dir: &Path) -> Result<Vec<R>> {
    let files: Vec<PathBuf> = extraction_dir
        .read_dir()?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let pb = create_progress_bar(files.len() as u64, "Processing files".to_string());
    let progress_bar = Arc::new(Mutex::new(pb));

    let tasks: Vec<_> = files
        .iter()
        .map(|file| {
            let file = file.clone();
            let pb = Arc::clone(&progress_bar);
            tokio::spawn(async move { process_file::<R>(&file, pb).await })
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

/// Processes a single file and return a vec of Readings.
async fn process_file<R: Reading>(
    file_path: &Path,
    progress_bar: Arc<Mutex<ProgressBar>>,
) -> Result<Vec<R>> {
    let mut readings = Vec::new();

    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let reading = R::from_line(&line, "")?;
        if reading.is_valid() {
            readings.push(reading);
        }
    }

    {
        let pb = progress_bar.lock().unwrap();
        pb.inc(1);
    }

    Ok(readings)
}
