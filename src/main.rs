use anyhow::{Error, Result};
use flate2::read::GzDecoder;
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use reading::Reading;
use std::{
    fs::File,
    io::{self, copy, BufRead, Cursor},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tar::Archive;

mod reading;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let file_path = Path::new("ghcnd_hcn.tar.gz");
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

    //
    let files: Vec<PathBuf> = working_dir
        .read_dir()?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let readings = process_files_in_parallel(files).await?;

    println!("Got {} readings", readings.len());

    Ok(())
}

async fn download_tar(file_path: &Path) -> Result<(), Error> {
    let url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd_hcn.tar.gz";
    let response = reqwest::get(url).await.expect("Failed to download file");

    if response.status().is_success() {
        // Create a file to save the downloaded content
        let mut file = File::create(file_path)?;
        let mut content = Cursor::new(response.bytes().await?);
        // Write the content to the file
        copy(&mut content, &mut file)?;

        println!("File downloaded successfully!");
    } else {
        println!("Failed to download file: {}", response.status());
    }

    Ok(())
}

async fn extract_tar(tar_gz_path: &Path) -> Result<(), Error> {
    // Open the tar file
    let tar_gz = File::open(tar_gz_path)?;

    // Create a GzDecoder to decode the gzip file
    let tar = GzDecoder::new(tar_gz);

    // Create an archive from the decoded tar
    let mut archive = Archive::new(tar);

    // Extract the archive to the current directory
    archive.unpack(".")?;

    println!("Archive extracted successfully!");

    Ok(())
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

async fn process_files_in_parallel(files: Vec<PathBuf>) -> Result<Vec<Reading>, Error> {
    let progress_bar = Arc::new(Mutex::new(ProgressBar::new(files.len() as u64)));
    progress_bar
        .lock()
        .unwrap()
        .set_style(ProgressStyle::default_bar());

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
