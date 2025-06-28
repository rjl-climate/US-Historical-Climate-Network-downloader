//! Save the daily readings to a parquet file.

use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow::{
    array::{Date32Builder, Float32Builder, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::{Datelike, NaiveDate};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use crate::{
    cli::create_progress_bar,
    reading::{DailyReading, Element},
};

pub fn save_daily(readings: &[DailyReading], file_path: &PathBuf) -> Result<()> {
    let chunk_size = 100000;
    
    // Calculate total actual rows (only for days with values)
    let mut total_actual_rows = 0;
    for reading in readings {
        for value in &reading.values {
            if value.is_some() {
                total_actual_rows += 1;
            }
        }
    }

    let file = File::create(file_path)?;

    // Optimized schema with better compression for Python processing
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("date", DataType::Date32, false),
        Field::new("element", DataType::Utf8, false),
        Field::new("dataset", DataType::Utf8, false),
        Field::new("value", DataType::Float32, false),
        Field::new("lat", DataType::Float32, true),
        Field::new("lon", DataType::Float32, true),
    ]));

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default()))  // Better compression for Python
        .set_dictionary_enabled(true)  // Enable dictionary encoding for repeated strings
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    let pb = create_progress_bar(total_actual_rows as u64, "Writing parquet file".to_string());

    // Pre-allocate builders for better performance
    let mut id_builder = StringBuilder::with_capacity(chunk_size, chunk_size * 12);
    let mut date_builder = Date32Builder::with_capacity(chunk_size);
    let mut element_builder = StringBuilder::with_capacity(chunk_size, chunk_size * 4);
    let mut dataset_builder = StringBuilder::with_capacity(chunk_size, chunk_size * 6);
    let mut value_builder = Float32Builder::with_capacity(chunk_size);
    let mut lat_builder = Float32Builder::with_capacity(chunk_size);
    let mut lon_builder = Float32Builder::with_capacity(chunk_size);

    // Pre-calculate epoch offset
    let epoch_offset = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().num_days_from_ce();

    let mut current_batch_rows = 0;
    let mut progress_counter = 0;

    for reading in readings {
        let year = reading.year;
        let month = reading.month.unwrap();
        
        // Pre-calculate strings once per reading  
        let element_str = element_to_string(&reading.properties.element);
        let dataset_str = dataset_to_string(&reading.properties.dataset);

        for (day_index, value_opt) in reading.values.iter().enumerate() {
            if let Some(value) = value_opt {
                let day = (day_index + 1) as u32;
                
                // Create date - optimized calculation
                if let Some(valid_date) = NaiveDate::from_ymd_opt(year as i32, month as u32, day) {
                    let date32 = valid_date.num_days_from_ce() - epoch_offset;

                    // Use builders for better performance
                    id_builder.append_value(&reading.id);
                    date_builder.append_value(date32);
                    element_builder.append_value(&element_str);
                    dataset_builder.append_value(&dataset_str);
                    value_builder.append_value(*value);
                    lat_builder.append_option(reading.lat);
                    lon_builder.append_option(reading.lon);

                    current_batch_rows += 1;
                    progress_counter += 1;

                    // Batch progress updates for better performance
                    if progress_counter % 10000 == 0 {
                        pb.set_position(progress_counter as u64);
                    }

                    // Write batch when full
                    if current_batch_rows >= chunk_size {
                        write_batch_optimized(&mut writer, &schema, &mut id_builder, &mut date_builder, 
                                            &mut element_builder, &mut dataset_builder, &mut value_builder, 
                                            &mut lat_builder, &mut lon_builder)?;
                        current_batch_rows = 0;
                    }
                }
            }
        }
    }

    // Write remaining data
    if current_batch_rows > 0 {
        write_batch_optimized(&mut writer, &schema, &mut id_builder, &mut date_builder, 
                            &mut element_builder, &mut dataset_builder, &mut value_builder, 
                            &mut lat_builder, &mut lon_builder)?;
    }

    pb.finish_with_message("Finished writing Parquet file");
    writer.close()?;
    Ok(())
}

fn write_batch_optimized(
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
    id_builder: &mut StringBuilder,
    date_builder: &mut Date32Builder,
    element_builder: &mut StringBuilder,
    dataset_builder: &mut StringBuilder,
    value_builder: &mut Float32Builder,
    lat_builder: &mut Float32Builder,
    lon_builder: &mut Float32Builder,
) -> Result<()> {
    let ids_array = id_builder.finish();
    let dates_array = date_builder.finish();
    let elements_array = element_builder.finish();
    let datasets_array = dataset_builder.finish();
    let values_array = value_builder.finish();
    let lats_array = lat_builder.finish();
    let lons_array = lon_builder.finish();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids_array),
            Arc::new(dates_array),
            Arc::new(elements_array),
            Arc::new(datasets_array),
            Arc::new(values_array),
            Arc::new(lats_array),
            Arc::new(lons_array),
        ],
    )?;

    writer.write(&batch)?;
    Ok(())
}

fn element_to_string(element: &Element) -> String {
    match element {
        Element::Tmax => "TMAX".to_string(),
        Element::Tmin => "TMIN".to_string(),
        Element::Prcp => "PRCP".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

fn dataset_to_string(dataset: &crate::reading::Dataset) -> String {
    match dataset {
        crate::reading::Dataset::Raw => "RAW".to_string(),
        crate::reading::Dataset::Tob => "TOB".to_string(),
        crate::reading::Dataset::Fls52 => "FLS52".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod test {
    use std::fs;
    use arrow::array::{Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::NamedTempFile;

    use crate::reading::{Dataset, FileProperties};

    use super::*;

    #[test]
    fn should_round_trip_multi_column() {
        // arrange
        let readings = readings_fixture();
        assert_eq!(readings[0].values[0], Some(10.0));
        assert_eq!(readings[0].values[30], Some(40.0));

        // act
        save_daily(&readings, &PathBuf::from("test")).unwrap();
    }

    #[test]
    fn should_validate_new_long_format_schema_and_data() {
        // Create test readings with known data
        let readings = comprehensive_test_fixture();
        
        // Create temporary file
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path().to_path_buf();
        
        // Save to parquet
        save_daily(&readings, &temp_path).unwrap();
        
        // Read back and validate
        let file = fs::File::open(&temp_path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        
        let mut total_rows = 0;
        let mut tmax_count = 0;
        let mut tmin_count = 0;
        let mut prcp_count = 0;
        
        for batch_result in reader {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
            
            // Validate new long format schema
            let schema = batch.schema();
            assert_eq!(schema.fields().len(), 7);
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(1).name(), "date");
            assert_eq!(schema.field(2).name(), "element");
            assert_eq!(schema.field(3).name(), "dataset");
            assert_eq!(schema.field(4).name(), "value");
            assert_eq!(schema.field(5).name(), "lat");
            assert_eq!(schema.field(6).name(), "lon");
            
            // Count element types
            let element_array = batch.column(2);
            let value_array = batch.column(4);
            
            // All values should be non-null in long format
            assert_eq!(value_array.null_count(), 0);
            
            // Count elements by type
            if let Some(string_array) = element_array.as_any().downcast_ref::<StringArray>() {
                for i in 0..batch.num_rows() {
                    let element_str = string_array.value(i);
                    match element_str {
                        "TMAX" => tmax_count += 1,
                        "TMIN" => tmin_count += 1,
                        "PRCP" => prcp_count += 1,
                        _ => {}
                    }
                }
            }
        }
        
        // In long format, we only have rows for actual values (no nulls)
        // Each reading has 31 non-null values, so 3 readings = 93 rows total
        assert_eq!(total_rows, 93);
        assert_eq!(tmax_count, 31);
        assert_eq!(tmin_count, 31);
        assert_eq!(prcp_count, 31);
    }

    #[test] 
    fn should_demonstrate_efficiency_improvement() {
        // Create test data with realistic sparsity
        let readings = create_realistic_test_data();
        
        // Create temporary files
        let new_file = NamedTempFile::new().unwrap();
        let new_path = new_file.path().to_path_buf();
        
        // Save with new long format
        save_daily(&readings, &new_path).unwrap();
        
        // Check file size and row count
        let new_file_size = fs::metadata(&new_path).unwrap().len();
        
        // Read back to count rows
        let file = fs::File::open(&new_path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        
        let mut actual_rows = 0;
        for batch_result in reader {
            let batch = batch_result.unwrap();
            actual_rows += batch.num_rows();
        }
        
        // With old format, this would be 100 readings * 31 days = 3,100 rows
        // With new format, we only have rows for actual values
        let expected_old_format_rows = readings.len() * 31;
        
        println!("New long format:");
        println!("  File size: {} bytes", new_file_size);
        println!("  Actual rows: {}", actual_rows);
        println!("Old wide format would have:");
        println!("  Rows: {} (with ~68% NULL values)", expected_old_format_rows);
        
        // New format should have significantly fewer rows (only non-null values)
        assert!(actual_rows < expected_old_format_rows);
        // Should be roughly 32% of old format rows (since ~68% were NULL)
        let ratio = actual_rows as f64 / (expected_old_format_rows as f64);
        assert!(ratio < 0.5);
    }

    fn create_realistic_test_data() -> Vec<DailyReading> {
        let mut readings = Vec::new();
        
        // Create 100 stations with realistic data patterns
        for station_id in 0..100 {
            let mut values = vec![None; 31];
            
            // Fill only some days with data (realistic sparsity)
            for day in 0..31 {
                if day % 3 == 0 {  // Roughly 1/3 of days have data
                    values[day] = Some(20.0 + day as f32);
                }
            }
            
            readings.push(DailyReading {
                id: format!("USW000{:05}", station_id),
                lat: Some(40.0 + station_id as f32 * 0.1),
                lon: Some(-100.0 + station_id as f32 * 0.1),
                year: 2024,
                month: Some(1),
                properties: FileProperties {
                    dataset: Dataset::Unknown,
                    element: Element::Tmax,
                },
                values,
            });
        }
        
        readings
    }

    #[test]
    #[ignore] // Only run manually since file may not exist
    fn should_validate_existing_parquet_file() {
        use std::path::Path;
        
        let parquet_path = Path::new("data/ushcn-daily-2025-06-27.parquet");
        if !parquet_path.exists() {
            println!("Skipping test - parquet file not found");
            return;
        }
        
        let file = fs::File::open(parquet_path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        
        let mut total_rows = 0;
        let mut tmax_count = 0;
        let mut tmin_count = 0;
        let mut prcp_count = 0;
        let mut batches_processed = 0;
        
        for batch_result in reader {
            let batch = batch_result.unwrap();
            batches_processed += 1;
            total_rows += batch.num_rows();
            
            // Validate schema on first batch
            if batches_processed == 1 {
                let schema = batch.schema();
                println!("Schema: {:?}", schema);
                assert_eq!(schema.fields().len(), 7);
                assert_eq!(schema.field(0).name(), "id");
                assert_eq!(schema.field(1).name(), "date");
                assert_eq!(schema.field(2).name(), "tmax");
                assert_eq!(schema.field(3).name(), "tmin");
                assert_eq!(schema.field(4).name(), "prcp");
                assert_eq!(schema.field(5).name(), "lat");
                assert_eq!(schema.field(6).name(), "lon");
            }
            
            // Count non-null values in each measurement column
            let tmax_array = batch.column(2);
            let tmin_array = batch.column(3);
            let prcp_array = batch.column(4);
            
            tmax_count += tmax_array.len() - tmax_array.null_count();
            tmin_count += tmin_array.len() - tmin_array.null_count();
            prcp_count += prcp_array.len() - prcp_array.null_count();
            
            // Just process first few batches to avoid taking too long
            if batches_processed >= 3 {
                break;
            }
        }
        
        println!("Total rows processed: {}", total_rows);
        println!("Non-null tmax values: {}", tmax_count);
        println!("Non-null tmin values: {}", tmin_count);
        println!("Non-null prcp values: {}", prcp_count);
        println!("Batches processed: {}", batches_processed);
        
        // Basic validation - we should have some data
        assert!(total_rows > 0);
        assert!(tmax_count > 0 || tmin_count > 0 || prcp_count > 0);
    }

    fn comprehensive_test_fixture() -> Vec<DailyReading> {
        let mut values = vec![];
        for v in 0..31 {
            values.push(Some((v as f32) + 10.0));
        }

        vec![
            // TMAX reading
            DailyReading {
                id: "USW00094728".to_string(),
                lat: Some(60.0),
                lon: Some(-150.0),
                year: 2019,
                month: Some(1),
                properties: FileProperties {
                    dataset: Dataset::Unknown,
                    element: Element::Tmax,
                },
                values: values.clone(),
            },
            // TMIN reading  
            DailyReading {
                id: "USW00094728".to_string(),
                lat: Some(60.0),
                lon: Some(-150.0),
                year: 2019,
                month: Some(1),
                properties: FileProperties {
                    dataset: Dataset::Unknown,
                    element: Element::Tmin,
                },
                values: values.iter().map(|v| v.map(|x| x - 5.0)).collect(),
            },
            // PRCP reading
            DailyReading {
                id: "USW00094728".to_string(),
                lat: Some(60.0),
                lon: Some(-150.0),
                year: 2019,
                month: Some(1),
                properties: FileProperties {
                    dataset: Dataset::Unknown,
                    element: Element::Prcp,
                },
                values: values.iter().map(|v| v.map(|x| x * 0.1)).collect(),
            },
        ]
    }

    fn readings_fixture() -> Vec<DailyReading> {
        let properties = FileProperties {
            dataset: Dataset::Unknown,
            element: Element::Tmax,
        };
        let mut values = vec![];
        for v in 0..31 {
            values.push(Some((v as f32) + 10.0));
        }

        vec![
            DailyReading {
                id: "USW00094728".to_string(),
                lat: Some(60.0),
                lon: Some(-150.0),
                year: 2019,
                month: Some(1),
                properties: properties.clone(),
                values: values.clone(),
            },
            DailyReading {
                id: "USW00094729".to_string(),
                lat: Some(60.0),
                lon: Some(-150.0),
                year: 2020,
                month: Some(2),
                properties: properties.clone(),
                values,
            },
        ]
    }
}
