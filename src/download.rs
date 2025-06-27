//! Downloads and extracts the latest version of the specified dataset.

use std::{
    fs::{self, File},
    io::{copy, Cursor, Write},
    path::{Path, PathBuf},
};

use anyhow::{Error, Result};
use flate2::read::GzDecoder;
use tar::Archive;
use indicatif::ProgressBar;
use futures::StreamExt;

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

/// Downloads with progress bar based on content length
pub async fn download_tar_with_progress(url: &str, file_path: PathBuf, progress_bar: ProgressBar) -> Result<(), Error> {
    let response = reqwest::get(url).await.map_err(|e| Error::msg(format!("Failed to download file: {}", e)))?;

    if !response.status().is_success() {
        return Err(Error::msg(format!("Failed to download file: {}", response.status())));
    }

    // Get content length and convert spinner to progress bar if we have size info
    let total_size = response.content_length().unwrap_or(0);
    if total_size > 0 {
        // Convert to proper progress bar with known size
        use indicatif::ProgressStyle;
        progress_bar.set_length(total_size);
        progress_bar.set_style(
            ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {eta}")
                .unwrap()
                .progress_chars("=> "),
        );
    }

    let mut file = File::create(file_path)?;
    let mut downloaded = 0u64;
    let mut stream = response.bytes_stream();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| Error::msg(format!("Error reading chunk: {}", e)))?;
        file.write_all(&chunk)?;
        downloaded += chunk.len() as u64;
        progress_bar.set_position(downloaded);
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

/// Extracts the tarball with progress tracking by counting files
pub async fn extract_tar_with_progress(tar_gz_path: &PathBuf, working_dir: &Path, progress_bar: ProgressBar) -> Result<(), Error> {
    // First pass: count total files
    let tar_gz = File::open(tar_gz_path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    
    let total_files = archive.entries()?.count() as u64;
    
    // Convert spinner to progress bar now that we know the total
    progress_bar.set_length(total_files);
    progress_bar.set_style(
        indicatif::ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {pos}/{len} files ({percent}%) {eta}")
            .unwrap()
            .progress_chars("=> "),
    );
    
    // Second pass: extract with progress
    let tar_gz = File::open(tar_gz_path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    
    let mut count = 0u64;
    for entry in archive.entries()? {
        let mut entry = entry?;
        entry.unpack_in(working_dir)?;
        count += 1;
        progress_bar.set_position(count);
    }

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

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use indicatif::ProgressStyle;

    #[test]
    fn should_create_progress_bar_for_download() {
        // Test that we can create a progress bar with the expected setup
        let pb = ProgressBar::new(1000);
        pb.set_style(ProgressStyle::default_bar()
            .template("{msg} [{bar:40}] {pos}/{len} ({percent}%)")
            .unwrap());
        
        pb.set_length(1000);
        pb.set_position(500);
        
        assert_eq!(pb.length().unwrap(), 1000);
        assert_eq!(pb.position(), 500);
    }
    
    #[tokio::test]
    async fn should_demonstrate_progress_tracking_structure() {
        // This test demonstrates the structure for progress tracking
        // without actually downloading anything
        
        let temp_dir = TempDir::new().unwrap();
        let _file_path = temp_dir.path().join("test.txt");
        
        // Create a progress bar like we do in the real function
        let pb = ProgressBar::new(0);
        pb.set_length(100);
        
        // Simulate progress updates
        for i in 0..=100 {
            pb.set_position(i);
        }
        
        pb.finish_with_message("✓ Test completed");
        
        assert_eq!(pb.position(), 100);
    }

    #[test]
    fn should_show_progress_bar_conversion() {
        use crate::cli::create_indeterminate_progress_bar;
        
        // Start with indeterminate (spinner)
        let pb = create_indeterminate_progress_bar("Testing...".to_string());
        
        // Convert to determinate progress bar (like we do in download)
        pb.set_length(1000);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{msg} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%)")
                .unwrap()
                .progress_chars("=> "),
        );
        
        // Simulate progress
        pb.set_position(500);
        
        // Check values before finishing
        assert_eq!(pb.length().unwrap(), 1000);
        assert_eq!(pb.position(), 500);
        
        pb.finish_with_message("✓ Conversion test completed");
    }
}
