use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::Result;
use tempfile::TempDir;

use crate::{
    cli::{command::stations::Station, create_progress_bar},
    deserialise::deserialise,
    download::{download_tar, extract_tar, get_extraction_folder},
    parquet,
    reading::{Dataset, MonthlyReading},
};

use super::make_dataset_parquet_file_name;

pub async fn monthly(_use_persistent_cache: bool, stations: &[Station]) -> Result<String> {
    let temp_dir = TempDir::new()?;

    let archive_paths = download_archives(temp_dir.path()).await?;
    let extraction_folder = extract_archives(&archive_paths, temp_dir.path()).await?;
    let mut readings: Vec<MonthlyReading> = deserialise(&extraction_folder).await?;
    
    // Inject coordinates using provided stations data
    readings = inject_coords_monthly(readings, stations.to_vec())?;
    
    // Group readings by dataset type
    let mut datasets = HashMap::new();
    for reading in readings {
        datasets.entry(reading.properties.dataset.clone())
               .or_insert_with(Vec::new)
               .push(reading);
    }

    let mut created_files = Vec::new();

    // Create separate parquet files for each dataset
    for (dataset, dataset_readings) in datasets {
        if dataset_readings.is_empty() {
            continue;
        }

        let dataset_name = dataset_to_string(&dataset);
        let parquet_file_name = make_dataset_parquet_file_name("monthly", &dataset_name);
        
        parquet::save_monthly(&dataset_readings, &parquet_file_name)?;
        created_files.push(parquet_file_name.to_string_lossy().to_string());
        
        println!("✓ Created {} monthly parquet file with {} readings", 
                dataset_name, dataset_readings.len());
    }

    // Return summary of created files
    Ok(format!("Created {} monthly dataset files: {}", 
              created_files.len(), 
              created_files.join(", ")))
}

fn dataset_to_string(dataset: &Dataset) -> String {
    match dataset {
        Dataset::Raw => "RAW".to_string(),
        Dataset::Tob => "TOB".to_string(),
        Dataset::Fls52 => "FLS52".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

fn inject_coords_monthly(readings: Vec<MonthlyReading>, stations: Vec<Station>) -> Result<Vec<MonthlyReading>> {
    let mut readings_with_coords = Vec::new();
    let lookup = make_lookup_monthly(&stations);

    for mut reading in readings {
        if let Some(coords) = lookup.get(&reading.id) {
            reading.lat = Some(coords.0);
            reading.lon = Some(coords.1);
        }

        readings_with_coords.push(reading);
    }

    Ok(readings_with_coords)
}

// Make a lookup table of station IDs to lat/lon for monthly data
fn make_lookup_monthly(stations: &Vec<Station>) -> HashMap<String, (f32, f32)> {
    let mut lookup = HashMap::new();

    for station in stations {
        if let (Some(lat), Some(lon)) = (station.latitude, station.longitude) {
            lookup.insert(station.station_id(), (lat, lon));
        }
    }

    lookup
}

/// Download the monthly archives and return a vector of the paths to the downloaded files.
async fn download_archives(temp_dir: &Path) -> Result<Vec<PathBuf>> {
    let element_map = element_map();
    let dataset_map = dataset_map();
    let file_urls = generate_file_urls(&element_map, &dataset_map);

    let total_files = file_urls.len() as u64;
    let pb = create_progress_bar(total_files, "Downloading monthly archives (parallel)...".to_string());
    
    // Download all files in parallel
    let download_tasks: Vec<_> = file_urls.into_iter().map(|file_url| {
        let filename = file_url.split('/').last().unwrap().to_string();
        let file_path = temp_dir.join(&filename);
        let pb_clone = pb.clone();
        
        async move {
            let result = download_tar(&file_url, file_path.clone()).await;
            pb_clone.inc(1);
            result.map(|_| file_path)
        }
    }).collect();
    
    let files: Result<Vec<_>, _> = futures::future::try_join_all(download_tasks).await;
    let files = files?;

    pb.finish_with_message("✓ Monthly archives downloaded (parallel)");

    Ok(files)
}

///Extract the archives in `archive_paths` to the `working_dir` and return the path to the extraction folder.
async fn extract_archives(archive_paths: &Vec<PathBuf>, working_dir: &Path) -> Result<PathBuf> {
    let total_files = archive_paths.len() as u64;
    let pb = create_progress_bar(total_files, "Extracting monthly archives...".to_string());

    for archive_path in archive_paths {
        extract_tar(archive_path, working_dir).await?;
        pb.inc(1);
    }
    pb.finish_with_message("Monthly archives extracted");

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
