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

pub async fn process_files_in_parallel(files: Vec<PathBuf>) -> Result<Vec<Reading>, Error> {
    let progress_bar = Arc::new(Mutex::new(ProgressBar::new(files.len() as u64)));
    progress_bar
        .lock()
        .unwrap()
        .set_style(ProgressStyle::default_bar());

    println!("WARNING: REMOVE 'TAKE' AFTER DEBUGGING!");

    let tasks: Vec<_> = files
        .iter()
        .take(1)
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

    let mut lines = reader.lines();
    let mut readings = Vec::new();

    while let Some(line) = lines.next() {
        let line = line?;
        let reading = Reading::from_line(&line)?;
        readings.push(reading);
    }

    {
        let pb = progress_bar.lock().unwrap();
        pb.inc(1);
    }

    Ok(readings)
}
