#!/usr/bin/env rust
//! Monthly Data Investigation Utility
//!
//! Downloads and examines raw USHCN monthly data files to understand:
//! - File structure and naming conventions
//! - Temperature data format (TMAX, TMIN, TAVG)
//! - Data quality variants (raw, tob, FLs.52j)

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use anyhow::Result;
use tempfile::TempDir;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔍 Monthly Data Investigation Utility\n");
    
    println!("📋 USHCN Monthly Data Structure:");
    println!("The monthly data consists of 9 files (3 elements × 3 quality levels):\n");
    
    println!("🌡️ Elements Available:");
    println!("  • TMAX - Monthly maximum temperature");
    println!("  • TMIN - Monthly minimum temperature"); 
    println!("  • TAVG - Monthly average temperature\n");
    
    println!("📊 Quality Levels:");
    println!("  • RAW    - Original unadjusted data");
    println!("  • TOB    - Time-of-observation bias corrected");
    println!("  • FLs.52j - Fully homogenized and corrected\n");
    
    println!("📁 Downloaded Files:");
    let elements = vec![("tmax", "Maximum"), ("tmin", "Minimum"), ("tavg", "Average")];
    let datasets = vec![("raw", "Raw"), ("tob", "TOB-adjusted"), ("FLs.52j", "Fully corrected")];
    
    for (element, element_desc) in &elements {
        for (dataset, dataset_desc) in &datasets {
            let url = format!("https://www.ncei.noaa.gov/pub/data/ushcn/v2.5/ushcn.{}.latest.{}.tar.gz", 
                             element, dataset);
            println!("  • {} {} Temperature: {}", dataset_desc, element_desc, url);
        }
        println!();
    }
    
    println!("🔍 Let's examine a sample file...\n");
    
    // Download and examine one file
    examine_sample_file().await?;
    
    Ok(())
}

async fn examine_sample_file() -> Result<()> {
    println!("📦 Downloading ushcn.tmax.latest.raw.tar.gz for examination...");
    
    let temp_dir = TempDir::new()?;
    let url = "https://www.ncei.noaa.gov/pub/data/ushcn/v2.5/ushcn.tmax.latest.raw.tar.gz";
    let filename = "ushcn.tmax.latest.raw.tar.gz";
    let file_path = temp_dir.path().join(filename);
    
    // Download the file
    download_file(url, &file_path).await?;
    println!("✅ Downloaded to: {:?}", file_path);
    
    // Extract the tar.gz
    extract_tar_gz(&file_path, temp_dir.path())?;
    println!("✅ Extracted archive");
    
    // Find the extracted files
    let extraction_path = temp_dir.path();
    examine_extracted_files(extraction_path)?;
    
    Ok(())
}

async fn download_file(url: &str, file_path: &Path) -> Result<()> {
    use reqwest;
    use std::io::Write;
    
    let response = reqwest::get(url).await?;
    let mut file = File::create(file_path)?;
    let content = response.bytes().await?;
    file.write_all(&content)?;
    Ok(())
}

fn extract_tar_gz(tar_gz_path: &Path, output_dir: &Path) -> Result<()> {
    use flate2::read::GzDecoder;
    use tar::Archive;
    
    let tar_gz_file = File::open(tar_gz_path)?;
    let tar = GzDecoder::new(tar_gz_file);
    let mut archive = Archive::new(tar);
    archive.unpack(output_dir)?;
    Ok(())
}

fn examine_extracted_files(dir: &Path) -> Result<()> {
    println!("\n📂 Examining extracted files...");
    
    // List all files in the directory
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() && path.extension().map_or(false, |ext| ext == "tmax") {
            println!("\n📄 Found data file: {}", path.file_name().unwrap().to_string_lossy());
            examine_data_file(&path)?;
            break; // Just examine one file for now
        }
    }
    
    Ok(())
}

fn examine_data_file(file_path: &Path) -> Result<()> {
    println!("🔍 Examining file structure...\n");
    
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().collect::<Result<Vec<_>, _>>()?;
    
    if lines.is_empty() {
        println!("❌ File is empty");
        return Ok(());
    }
    
    println!("📊 File Statistics:");
    println!("  • Total lines: {}", lines.len());
    println!("  • First line length: {}", lines[0].len());
    
    println!("\n📋 Sample lines:");
    for (i, line) in lines.iter().take(5).enumerate() {
        println!("  {}: {}", i + 1, line);
    }
    
    println!("\n🔍 Line Structure Analysis:");
    if let Some(first_line) = lines.first() {
        analyze_line_structure(first_line);
    }
    
    println!("\n📈 Data Analysis:");
    analyze_data_content(&lines);
    
    Ok(())
}

fn analyze_line_structure(line: &str) {
    println!("  • Line length: {} characters", line.len());
    
    if line.len() >= 16 {
        let station_id = &line[0..11];
        let year = &line[12..16];
        
        println!("  • Station ID (pos 1-11): '{}'", station_id);
        println!("  • Year (pos 13-16): '{}'", year);
        
        if line.len() > 16 {
            println!("  • Data section starts at position 17");
            println!("  • Data section: '{}'", &line[16..]);
            
            // Analyze monthly data chunks
            let data_section = &line[16..];
            let chunks: Vec<&str> = data_section.as_bytes()
                .chunks(9)
                .map(|chunk| std::str::from_utf8(chunk).unwrap_or("???"))
                .collect();
            
            println!("  • Monthly data chunks (9 chars each):");
            for (month, chunk) in chunks.iter().enumerate() {
                if month < 12 {
                    println!("    Month {}: '{}'", month + 1, chunk);
                }
            }
        }
    }
}

fn analyze_data_content(lines: &[String]) {
    let mut stations = std::collections::HashSet::new();
    let mut years = std::collections::HashSet::new();
    
    for line in lines.iter().take(100) { // Sample first 100 lines
        if line.len() >= 16 {
            let station_id = &line[0..11];
            let year_str = &line[12..16];
            
            stations.insert(station_id.to_string());
            if let Ok(year) = year_str.parse::<u16>() {
                years.insert(year);
            }
        }
    }
    
    println!("  • Unique stations (first 100 lines): {}", stations.len());
    if !years.is_empty() {
        println!("  • Year range: {} - {}", 
                years.iter().min().unwrap(), 
                years.iter().max().unwrap());
    }
    
    println!("\n🎯 Temperature Metrics Available:");
    println!("  ✅ Monthly Maximum Temperature (TMAX) - This file");
    println!("  📁 Monthly Minimum Temperature (TMIN) - In separate file");
    println!("  📁 Monthly Average Temperature (TAVG) - In separate file");
    
    println!("\n💡 Key Insights:");
    println!("  • Each line = 1 station × 1 year × 12 monthly values");
    println!("  • Temperature values in centidegrees (divide by 100)");
    println!("  • -9999 indicates missing data");
    println!("  • Quality flags and metadata included in 9-char chunks");
}