//! Downloads and extracts the latest version of the specified release.

use std::{
    fs::File,
    io::{copy, Cursor},
    path::Path,
};

use anyhow::Error;
use flate2::read::GzDecoder;
use tar::Archive;

pub async fn download_tar(file_path: &Path) -> Result<(), Error> {
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

pub async fn extract_tar(tar_gz_path: &Path) -> Result<(), Error> {
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
