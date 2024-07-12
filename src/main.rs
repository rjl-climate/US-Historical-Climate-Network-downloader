use anyhow::{Error, Result};
use flate2::read::GzDecoder;
use std::fs::File;
use std::io;
use std::io::copy;
use std::io::BufRead;
use std::io::Cursor;
use std::path::Path;
use std::path::PathBuf;
use tar::Archive;

#[derive(Debug)]
struct Reading {
    id: String,
    year: u16,
    month: u8,
    element: String,
    values: Vec<Option<f32>>,
}

impl Reading {
    fn from_line(line: &str) -> Result<Self> {
        let id = line[0..11].to_string();
        let year = line[11..15].parse()?;
        let month = line[15..17].parse()?;
        let element = line[17..21].to_string();
        let values = parse_values(line);

        Ok(Reading {
            id,
            year,
            month,
            element,
            values,
        })
    }
}

fn parse_values(line: &str) -> Vec<Option<f32>> {
    let start_pos = 21;
    let chunk_length = 8;
    let num_chunks = 31;

    let values: Vec<Option<f32>> = (0..num_chunks)
        .map(|i| {
            let chunk = &line[start_pos + i * chunk_length..start_pos + (i + 1) * chunk_length];
            let first_five = &chunk[..5].trim();
            match first_five.parse::<i32>() {
                Ok(v) if v != -9999 => Some((v as f32) / 100.0),
                _ => None,
            }
        })
        .collect();

    values
}

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

    let mut readings = Vec::<Reading>::new();

    for file in files.iter().take(2) {
        println!("Processing '{}'.", file.to_string_lossy());
        let new_readings = process_file(&file).await?;
        readings.extend(new_readings);
    }

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

async fn process_file(file_path: &Path) -> Result<Vec<Reading>, Error> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);

    let mut lines = reader.lines();
    let mut readings = Vec::new();

    while let Some(line) = lines.next() {
        let line = line?;
        let reading = Reading::from_line(&line)?;
        readings.push(reading);
    }

    Ok(readings)
}

// -- Tests ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn should_parse_line() {
        let line = "USC00011084192601TOBS-9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999     217  6   28  6   39  6   44  6  100  6  106  6  117  6  106  6  128  6   94  6  189  6";
        let reading = Reading::from_line(line).unwrap();

        assert_eq!(reading.id, "USC00011084");
        assert_eq!(reading.year, 1926);
        assert_eq!(reading.month, 1);
        assert_eq!(reading.element, "TOBS");
        assert!(reading.values.len() == 31);
        assert_eq!(reading.values[0], None);
        assert_eq!(reading.values[30], Some(1.89));
    }
}
