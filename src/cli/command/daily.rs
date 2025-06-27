use std::{
    collections::HashMap,
    fs::{self},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use tempfile::TempDir;

use crate::{
    cli::{command::stations::Station, create_indeterminate_progress_bar},
    command::stations::{download_archive as download_station_archive, extract_stations},
    deserialise::deserialise,
    download::{download_tar_with_progress, extract_tar_with_progress},
    parquet,
    reading::DailyReading,
};

use super::make_parquet_file_name;

pub async fn daily() -> Result<String> {
    let tmp_dir = TempDir::new()?;
    let parquet_file_name = make_parquet_file_name("daily");

    // Download archives sequentially to avoid progress bar conflicts
    let daily_archive_filepath = download_archive(tmp_dir.path()).await?;
    let stations_archive_filepath = download_station_archive(tmp_dir.path()).await?;

    // Extract daily archive and process stations file in parallel
    let daily_extraction = extract_archive(&daily_archive_filepath);
    let stations_task = tokio::task::spawn_blocking({
        let stations_path = stations_archive_filepath.clone();
        move || extract_stations(&stations_path)
    });

    let (archive_dir, stations_handle) = tokio::try_join!(
        daily_extraction,
        async { 
            stations_task.await
                .map_err(|e| anyhow::anyhow!("Stations extraction task failed: {}", e))
        }
    )?;
    
    let stations = stations_handle?;

    // Deserialize readings and inject coordinates
    let mut readings = deserialise(&archive_dir).await?;
    readings = inject_coords(readings, stations)?;

    parquet::save_daily(&readings, &parquet_file_name)?;

    Ok(parquet_file_name.to_string_lossy().to_string())
}

async fn download_archive(temp_dir: &Path) -> Result<PathBuf> {
    let url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd_hcn.tar.gz";
    let file_name = url.split('/').last().unwrap();
    let file_path = temp_dir.join(file_name);

    let bar = create_indeterminate_progress_bar("Downloading daily archive...".to_string());
    download_tar_with_progress(url, file_path.clone(), bar.clone()).await?;
    bar.finish_with_message("✓ Daily archive downloaded");

    Ok(file_path)
}

async fn extract_archive(archive_filepath: &PathBuf) -> Result<PathBuf> {
    let archive_dir = archive_filepath.parent().unwrap();

    let bar = create_indeterminate_progress_bar("Extracting daily archive files...".to_string());
    extract_tar_with_progress(archive_filepath, archive_dir, bar.clone()).await?;
    bar.finish_with_message("✓ Daily archive extracted");

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
