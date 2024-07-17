use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Error, Result};
use chrono::{Datelike, Local};
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use tempfile::TempDir;

use crate::{
    cli::make_progress_bar,
    download::{download_tar, extract_tar, get_extraction_folder},
    parquet,
    reading::{MonthlyReading, Reading},
};

pub async fn monthly() -> Result<String> {
    let temp_dir = TempDir::new()?;
    let parquet_file_name = make_parquet_file_name();

    let archive_paths = download_archives(temp_dir.path()).await?;
    let extraction_folder = extract_archives(&archive_paths, temp_dir.path()).await?;
    let readings = deserialise(&extraction_folder).await?;
    parquet::save_monthly(&readings, &parquet_file_name)?;

    Ok(parquet_file_name.to_string_lossy().to_string())
}

async fn download_archives(temp_dir: &Path) -> Result<Vec<PathBuf>> {
    let element_map = element_map();
    let dataset_map = dataset_map();
    let file_urls = generate_file_urls(&element_map, &dataset_map);

    let total_files = file_urls.len() as u64;
    let pb = make_progress_bar(total_files, "Downloading archives...");
    let mut files = vec![];

    for file_url in file_urls {
        let filename = file_url.split('/').last().unwrap();
        let file_path = temp_dir.join(filename);

        download_tar(&file_url, file_path.clone()).await?;
        files.push(file_path);

        pb.inc(1);
    }
    pb.finish_with_message("Archives downloaded");

    Ok(files)
}

async fn extract_archives(archive_paths: &Vec<PathBuf>, working_dir: &Path) -> Result<PathBuf> {
    let total_files = archive_paths.len() as u64;
    let pb = make_progress_bar(total_files, "Extracting files...");

    for archive_path in archive_paths {
        extract_tar(archive_path, working_dir).await?;
        pb.inc(1);
    }
    pb.finish_with_message("Files extracted");

    let extraction_folder = get_extraction_folder(working_dir)?;

    Ok(extraction_folder)
}

async fn deserialise(extraction_folder: &Path) -> Result<Vec<MonthlyReading>> {
    let files: Vec<PathBuf> = extraction_folder
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
) -> Result<Vec<MonthlyReading>, Error> {
    let mut readings = Vec::new();

    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let file_name = file_path.file_name().unwrap().to_str().unwrap();

    for line in reader.lines() {
        let line = line?;
        let reading = MonthlyReading::from_line(&line, file_name)?;
        readings.push(reading);
    }

    {
        let pb = progress_bar.lock().unwrap();
        pb.inc(1);
    }

    Ok(readings)
}

fn element_map() -> HashMap<&'static str, &'static str> {
    let mut element_map = HashMap::new();
    element_map.insert("max", "tmax");
    element_map.insert("min", "tmin");
    element_map.insert("avg", "tavg");

    element_map
}

fn dataset_map() -> HashMap<&'static str, &'static str> {
    let mut dataset_map = HashMap::new();
    dataset_map.insert("fls52", "FLs.52j");
    dataset_map.insert("raw", "raw");
    dataset_map.insert("tob", "tob");

    dataset_map
}

fn generate_file_urls(
    element_map: &HashMap<&str, &str>,
    dataset_map: &HashMap<&str, &str>,
) -> Vec<String> {
    let root = "https://www.ncei.noaa.gov/pub/data/ushcn/v2.5";
    let mut urls = vec![];

    for element in element_map.keys() {
        for dataset in dataset_map.keys() {
            let element_name = element_map.get(element).unwrap();
            let dataset_name = dataset_map.get(dataset).unwrap();
            let file_name = format!("ushcn.{}.latest.{}.tar.gz", element_name, dataset_name);
            urls.push(format!("{}/{}", root, file_name));
        }
    }

    urls
}

fn make_parquet_file_name() -> PathBuf {
    let today = Local::now();
    let file_name = format!(
        "ushcn-monthly-{}-{:02}-{:02}.parquet",
        today.year(),
        today.month(),
        today.day()
    );

    dirs::home_dir().unwrap().join(file_name)
}
// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn should_generate_file_urls() {
        let element_map = element_map();
        let dataset_map = dataset_map();
        let file_urls = generate_file_urls(&element_map, &dataset_map);

        assert_eq!(file_urls.len(), 9);

        assert!(file_urls.contains(
            &"https://www.ncei.noaa.gov/pub/data/ushcn/v2.5/ushcn.tmin.latest.raw.tar.gz"
                .to_string()
        ));
    }
}
