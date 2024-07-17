//! Downloads and extracts the latest version of the specified dataset.

use std::{
    fs::{self, File},
    io::{copy, Cursor},
    path::{Path, PathBuf},
};

use anyhow::{Error, Result};
use flate2::read::GzDecoder;
use tar::Archive;

/// Downloads the tarball from the specified URL and saves it to the specified file path.
pub async fn download_tar(url: &str, file_path: PathBuf) -> Result<(), Error> {
    let response = reqwest::get(url).await.expect("Failed to download file");

    if response.status().is_success() {
        // Create a file to save the downloaded content

        let mut file = File::create(file_path)?;

        let mut content = Cursor::new(response.bytes().await?);

        // Write the content to the file
        copy(&mut content, &mut file)?;
    } else {
        println!("Failed to download file: {}", response.status());
    }

    Ok(())
}

/// Extracts the tarball at the specified path to the specified working directory.
pub async fn extract_tar(tar_gz_path: &PathBuf, working_dir: &Path) -> Result<(), Error> {
    // Open the tar file
    let tar_gz = File::open(tar_gz_path)?;

    // Create a GzDecoder to decode the gzip file
    let tar = GzDecoder::new(tar_gz);

    // Create an archive from the decoded tar
    let mut archive = Archive::new(tar);

    // Extract the archive to the current directory
    archive.unpack(working_dir)?;

    Ok(())
}

/// Returns the path of the extracted folder.
pub fn get_extraction_folder(working_dir: &Path) -> Result<PathBuf> {
    let mut extracted_folders = Vec::new();
    for entry in fs::read_dir(working_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Assuming the tarball extracts into one or more directories, we filter for directories.
        if path.is_dir() {
            extracted_folders.push(path);
        }
    }

    // Return the first extracted directory
    if let Some(folder_path) = extracted_folders.first() {
        Ok(folder_path.clone())
    } else {
        Err(Error::msg("No extracted folder found"))
    }
}
