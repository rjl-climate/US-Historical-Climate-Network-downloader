use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::Result;
use chrono::{Datelike, Local};
use tempfile::TempDir;

use crate::{
    cli::make_progress_bar,
    deserialise::deserialise,
    download::{download_tar, extract_tar, get_extraction_folder},
    parquet,
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
