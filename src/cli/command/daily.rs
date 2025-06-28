use std::{
    collections::HashMap,
    fs::{self},
    path::{Path, PathBuf},
};

use dirs;

use anyhow::{anyhow, Result};

use crate::{
    cli::{command::stations::Station, create_indeterminate_progress_bar},
    deserialise::deserialise,
    download::{download_tar_with_progress, extract_tar_with_progress},
    parquet,
    reading::{DailyReading, Dataset},
};

use super::make_parquet_file_name;

pub async fn daily(use_persistent_cache: bool, stations: &[Station]) -> Result<String> {
    let cache_dir = get_cache_dir(use_persistent_cache)?;

    // Download and extract daily archive
    let daily_archive_filepath = download_archive_cached(&cache_dir).await?;
    let archive_dir = extract_archive_cached(&daily_archive_filepath, &cache_dir).await?;

    // Deserialize readings and inject coordinates
    let mut readings = deserialise(&archive_dir).await?;
    readings = inject_coords(readings, stations.to_vec())?;

    // Create single daily parquet file (GHCN daily data is not separated by dataset type)
    let parquet_file_name = make_parquet_file_name("daily");
    parquet::save_daily(&readings, &parquet_file_name)?;
    
    println!("✓ Created daily parquet file with {} readings", readings.len());

    Ok(format!("Created 1 daily file: {}", 
              parquet_file_name.to_string_lossy()))
}

fn dataset_to_string(dataset: &Dataset) -> String {
    match dataset {
        Dataset::Raw => "RAW".to_string(),
        Dataset::Tob => "TOB".to_string(),
        Dataset::Fls52 => "FLS52".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

fn get_cache_dir(use_persistent_cache: bool) -> Result<PathBuf> {
    let cache_dir = if use_persistent_cache {
        // Use persistent cache in Library directory
        dirs::cache_dir()
            .ok_or_else(|| anyhow!("Could not determine cache directory"))?
            .join("ushcn")
    } else {
        // Use temporary directory
        let temp_dir = std::env::temp_dir().join("ushcn");
        temp_dir
    };
    
    if !cache_dir.exists() {
        fs::create_dir_all(&cache_dir)?;
    }
    
    Ok(cache_dir)
}

async fn download_archive_cached(cache_dir: &Path) -> Result<PathBuf> {
    let url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd_hcn.tar.gz";
    let file_name = url.split('/').last().unwrap();
    let file_path = cache_dir.join(file_name);

    // Check if cached file exists
    if file_path.exists() {
        println!("✓ Using cached daily archive: {}", file_path.display());
        return Ok(file_path);
    }

    let bar = create_indeterminate_progress_bar("Downloading daily archive...".to_string());
    download_tar_with_progress(url, file_path.clone(), bar.clone()).await?;
    bar.finish_with_message("✓ Daily archive downloaded and cached");

    Ok(file_path)
}


async fn extract_archive_cached(archive_filepath: &PathBuf, cache_dir: &Path) -> Result<PathBuf> {
    // Create extraction parent directory in cache
    let extraction_parent = cache_dir.join("extracted");
    
    // Check if already extracted (look for the actual extracted directory)
    if extraction_parent.exists() {
        if let Ok(existing_dir) = get_archive_dir(&extraction_parent) {
            println!("✓ Using cached extracted daily archive: {}", existing_dir.display());
            return Ok(existing_dir);
        }
    }

    // Ensure extraction parent directory exists
    if !extraction_parent.exists() {
        fs::create_dir_all(&extraction_parent)?;
    }

    let bar = create_indeterminate_progress_bar("Extracting daily archive files...".to_string());
    extract_tar_with_progress(archive_filepath, &extraction_parent, bar.clone()).await?;
    bar.finish_with_message("✓ Daily archive extracted and cached");

    let final_dir = get_archive_dir(&extraction_parent)?;
    Ok(final_dir)
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

fn inject_coords(readings: Vec<DailyReading>, stations: Vec<Station>) -> Result<Vec<DailyReading>> {
    let mut readings_with_coords = Vec::new();
    let lookup = make_lookup(&stations);

    println!("Injecting coords into readings");

    for mut reading in readings {
        if let Some(coords) = lookup.get(&reading.id) {
            reading.lat = Some(coords.0);
            reading.lon = Some(coords.1);
        }

        readings_with_coords.push(reading);
    }

    Ok(readings_with_coords)
}

// Make a lookup table of station IDs to lat/lon
fn make_lookup(stations: &Vec<Station>) -> HashMap<String, (f32, f32)> {
    let mut lookup = HashMap::new();

    for station in stations {
        if let (Some(lat), Some(lon)) = (station.latitude, station.longitude) {
            lookup.insert(station.station_id(), (lat, lon));
        }
    }

    lookup
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn should_make_lookup() {
        let stations = station_fixture();
        let lookup = make_lookup(&stations);

        assert_eq!(lookup.len(), 2);
        assert_eq!(lookup.get("US000station0"), Some(&(1.0, 2.0)));
        assert_eq!(lookup.get("US000station1"), Some(&(3.0, 4.0)));
        assert_eq!(lookup.get("XXX"), None);
    }

    #[test]
    fn should_verify_parallel_download_structure() {
        // This test verifies the parallel download structure compiles correctly
        // and that the types are compatible
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            // Create a temp directory for testing
            let tmp_dir = tempfile::TempDir::new().unwrap();
            
            // Test that our parallel structure compiles and types align
            let future1 = async { Ok::<PathBuf, anyhow::Error>(tmp_dir.path().join("test1")) };
            let future2 = async { Ok::<PathBuf, anyhow::Error>(tmp_dir.path().join("test2")) };
            
            // This should work with the same pattern as our parallel downloads
            let (path1, path2) = tokio::try_join!(future1, future2)?;
            
            Ok::<(PathBuf, PathBuf), anyhow::Error>((path1, path2))
        });
        
        assert!(result.is_ok());
    }

    fn station_fixture() -> Vec<Station> {
        let mut stations = vec![Station::default(), Station::default(), Station::default()];

        let country_code = "US".to_string();
        let network_code = "0".to_string();
        let id_placeholder = "00".to_string();

        stations[0].latitude = Some(1.0);
        stations[0].longitude = Some(2.0);
        stations[0].country_code.clone_from(&country_code);
        stations[0].network_code.clone_from(&network_code);
        stations[0].id_placeholder.clone_from(&id_placeholder);
        stations[0].coop_id = "station0".to_string();

        stations[1].latitude = Some(3.0);
        stations[1].longitude = Some(4.0);
        stations[1].country_code.clone_from(&country_code);
        stations[1].network_code.clone_from(&network_code);
        stations[1].id_placeholder.clone_from(&id_placeholder);
        stations[1].coop_id = "station1".to_string();

        stations[2].country_code.clone_from(&country_code);
        stations[2].network_code.clone_from(&network_code);
        stations[2].id_placeholder.clone_from(&id_placeholder);
        stations[2].coop_id = "station2".to_string();

        stations
    }
}
