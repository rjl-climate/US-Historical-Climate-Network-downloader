//! Download station data and save to disk.
//!
//! See "USHCN v2.5 readme.txt Section 2" for format.

use std::{
    fs::File,
    io::{self, BufRead},
    path::{Path, PathBuf},
};

use anyhow::Result;
use chrono::{Datelike, Local};
use tempfile::TempDir;

use crate::{cli::create_spinner, download::download_tar, parquet};

#[derive(Debug, Default, Clone)]
pub struct Station {
    pub country_code: String,
    pub network_code: String,
    pub id_placeholder: String,
    pub coop_id: String,
    pub latitude: Option<f32>,
    pub longitude: Option<f32>,
    pub elevation: Option<f32>,
    pub state: Option<String>,
    pub name: String,
}

// .parse::<f64>(
impl Station {
    fn from_ushcn_line(line: &str) -> Result<Self> {
        let country_code = line[0..2].to_string();
        let network_code = line[2..3].to_string();
        let id_placeholder = line[3..5].to_string();
        let coop_id = line[5..11].to_string();
        let latitude = parse_and_filter_f32(&line[12..20]);
        let longitude = parse_and_filter_f32(&line[21..30]);
        let elevation = parse_and_filter_f32(&line[32..37]);
        let state = parse_str(&line[38..40]);
        let name = line[41..71].trim().to_string();

        Ok(Station {
            country_code,
            network_code,
            id_placeholder,
            coop_id,
            latitude,
            longitude,
            elevation,
            state,
            name,
        })
    }

    fn from_ghcn_line(line: &str) -> Result<Self> {
        let country_code = line[0..2].to_string();
        let network_code = line[2..3].to_string();
        let id_placeholder = line[3..5].to_string();
        let coop_id = line[5..11].to_string();
        let latitude = parse_and_filter_f32(&line[11..20]);
        let longitude = parse_and_filter_f32(&line[21..30]);
        let elevation = parse_and_filter_f32(&line[32..37]);
        let state = parse_str(&line[38..40]);
        let name = line[41..71].trim().to_string();

        Ok(Station {
            country_code,
            network_code,
            id_placeholder,
            coop_id,
            latitude,
            longitude,
            elevation,
            state,
            name,
        })
    }

    pub fn station_id(&self) -> String {
        format!(
            "{}{}{}{}",
            self.country_code, self.network_code, self.id_placeholder, self.coop_id
        )
    }
}
pub async fn stations(_use_persistent_cache: bool) -> Result<(String, Vec<Station>)> {
    let tmp_dir = TempDir::new()?;

    let archive_filepath = download_ushcn_archive(tmp_dir.path()).await?;
    let stations = extract_ushcn_stations(&archive_filepath)?;
    let parquet_file_name = make_ushcn_parquet_file_name();
    parquet::save_stations(&stations, &parquet_file_name)?;

    Ok((parquet_file_name.to_string_lossy().to_string(), stations))
}

pub async fn ghcn_stations(_use_persistent_cache: bool) -> Result<(String, Vec<Station>)> {
    let tmp_dir = TempDir::new()?;

    let archive_filepath = download_ghcn_archive(tmp_dir.path()).await?;
    let stations = extract_ghcn_stations(&archive_filepath)?;
    let parquet_file_name = make_ghcn_parquet_file_name();
    parquet::save_stations(&stations, &parquet_file_name)?;

    Ok((parquet_file_name.to_string_lossy().to_string(), stations))
}

pub async fn download_ushcn_archive(temp_dir: &Path) -> Result<PathBuf> {
    let url = "https://www.ncei.noaa.gov/pub/data/ushcn/v2.5/ushcn-v2.5-stations.txt";
    let file_name = url.split('/').last().unwrap();
    let file_path = temp_dir.join(file_name);

    let bar = create_spinner("Downloading USHCN stations data...".to_string());
    download_tar(url, file_path.clone()).await?;
    bar.finish_with_message("✓ USHCN stations data downloaded");

    Ok(file_path)
}

pub async fn download_ghcn_archive(temp_dir: &Path) -> Result<PathBuf> {
    let url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt";
    let file_name = url.split('/').last().unwrap();
    let file_path = temp_dir.join(file_name);

    let bar = create_spinner("Downloading GHCN stations data...".to_string());
    download_tar(url, file_path.clone()).await?;
    bar.finish_with_message("✓ GHCN stations data downloaded");

    Ok(file_path)
}

pub fn extract_ushcn_stations(archive_filepath: &PathBuf) -> Result<Vec<Station>> {
    let mut stations: Vec<Station> = Vec::new();

    let file = File::open(archive_filepath)?;
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let reading = Station::from_ushcn_line(&line)?;
        stations.push(reading);
    }

    Ok(stations)
}

pub fn extract_ghcn_stations(archive_filepath: &PathBuf) -> Result<Vec<Station>> {
    let mut stations: Vec<Station> = Vec::new();

    let file = File::open(archive_filepath)?;
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let reading = Station::from_ghcn_line(&line)?;
        stations.push(reading);
    }

    Ok(stations)
}

fn parse_and_filter_f32(s: &str) -> Option<f32> {
    s.trim().parse::<f32>().ok().filter(|&v| v != -999.9)
}

fn parse_str(s: &str) -> Option<String> {
    s.trim().parse::<String>().ok().filter(|v| !v.is_empty())
}

pub fn make_ushcn_parquet_file_name() -> PathBuf {
    let today = Local::now();
    let file_name = format!(
        "ushcn-stations-{}-{:02}-{:02}.parquet",
        today.year(),
        today.month(),
        today.day()
    );

    dirs::home_dir().unwrap().join(file_name)
}

pub fn make_ghcn_parquet_file_name() -> PathBuf {
    let today = Local::now();
    let file_name = format!(
        "ghcnd-stations-{}-{:02}-{:02}.parquet",
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
    fn should_process_ushcn_line() {
        let line =
            "USH00011084  31.0581  -87.0547   25.9 AL BREWTON 3 SSE                  ------ ------ ------ +6";
        let s = Station::from_ushcn_line(line).unwrap();

        assert_eq!(s.country_code, "US");
        assert_eq!(s.network_code, "H");
        assert_eq!(s.id_placeholder, "00");
        assert_eq!(s.coop_id, "011084");
        assert_eq!(s.latitude, Some(31.0581));
        assert_eq!(s.longitude, Some(-87.0547));
        assert_eq!(s.elevation, Some(25.9));
        assert_eq!(s.state, Some("AL".to_string()));
        assert_eq!(s.name, "BREWTON 3 SSE".to_string());
    }

    #[test]
    fn should_process_ghcn_line() {
        let line =
            "USC00437054  44.4200  -72.0194  213.4 VT SAINT JOHNSBURY                    HCN 72614";
        let s = Station::from_ghcn_line(line).unwrap();

        assert_eq!(s.country_code, "US");
        assert_eq!(s.network_code, "C");
        assert_eq!(s.id_placeholder, "00");
        assert_eq!(s.coop_id, "437054");
        assert_eq!(s.latitude, Some(44.42));
        assert_eq!(s.longitude, Some(-72.0194));
        assert_eq!(s.elevation, Some(213.4));
        assert_eq!(s.state, Some("VT".to_string()));
        assert_eq!(s.name, "SAINT JOHNSBURY".to_string());
    }

    #[test]
    fn should_parse_f64() {
        let s = "  44.4200";
        let f = parse_and_filter_f32(s).unwrap();
        assert_eq!(f, 44.42);
    }

    #[test]
    fn should_filter_999() {
        let s = " -999.9 ";
        let f = parse_and_filter_f32(s);
        assert!(f.is_none());
    }

    #[test]
    fn should_make_ushcn_station_id() {
        let line =
            "USH00011084  31.0581  -87.0547   25.9 AL BREWTON 3 SSE                  ------ ------ ------ +6";
        let s = Station::from_ushcn_line(line).unwrap();

        assert_eq!(s.station_id(), "USH00011084");
    }

    #[test]
    fn should_make_ghcn_station_id() {
        let line =
            "USC00437054  44.4200  -72.0194  213.4 VT SAINT JOHNSBURY                    HCN 72614";
        let s = Station::from_ghcn_line(line).unwrap();

        assert_eq!(s.station_id(), "USC00437054");
    }
}
