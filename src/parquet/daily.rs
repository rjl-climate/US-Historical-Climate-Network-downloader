//! Save the daily readings to a parquet file.

use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow::{
    array::{ArrayRef, Date32Array, Float32Array, StringArray},
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
    let days_per_month = 31;
    let chunk_size = 100000;
    let total_rows = readings.len() * days_per_month;

    // Initialize the Parquet writer
    let file = File::create(file_path)?;

    // Define the schema for the RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("date", DataType::Date32, true),
        Field::new("tmax", DataType::Float32, true),
        Field::new("tmin", DataType::Float32, true),
        Field::new("prcp", DataType::Float32, true),
        Field::new("lat", DataType::Float32, true),
        Field::new("lon", DataType::Float32, true),
    ]));

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut rows_processed = 0;

    // Prepare vectors to hold column data
    let pb = create_progress_bar(total_rows as u64, "Writing parquet file chunks".to_string());

    while rows_processed < total_rows {
        let remaining_rows = total_rows - rows_processed;
        let batch_size = chunk_size.min(remaining_rows);

        let mut ids = Vec::with_capacity(batch_size);
        let mut date32s = Vec::with_capacity(batch_size);
        let mut tmaxs = Vec::with_capacity(batch_size);
        let mut tmins = Vec::with_capacity(batch_size);
        let mut prcps = Vec::with_capacity(batch_size);
        let mut lats = Vec::with_capacity(batch_size);
        let mut lons = Vec::with_capacity(batch_size);

        let mut rows_in_batch = 0;

        for reading in &readings[rows_processed / days_per_month..] {
            let id = reading.id.as_str();
            let year = reading.year;
            let month = reading.month.unwrap();

            for day in 1..=days_per_month {
                ids.push(id);

                lats.push(reading.lat);
                lons.push(reading.lon);

                // Convert year, month, and day to a NaiveDate
                let date = NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32);
                match date {
                    Some(valid_date) => {
                        let date32 = valid_date.num_days_from_ce()
                            - NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .num_days_from_ce();
                        date32s.push(Some(date32));
                    }
                    None => {
                        date32s.push(None);
                    }
                }

                let value_index = day - 1;
                match reading.properties.element {
                    Element::Tmax => {
                        if let Some(value) = reading.values.get(value_index) {
                            tmaxs.push(*value);
                        } else {
                            tmaxs.push(None);
                        }
                        tmins.push(None); // No tmin data for this reading
                        prcps.push(None); // No prcp data for this reading
                    }
                    Element::Tmin => {
                        if let Some(value) = reading.values.get(value_index) {
                            tmins.push(*value);
                        } else {
                            tmins.push(None);
                        }
                        tmaxs.push(None); // No tmax data for this reading
                        prcps.push(None); // No prcp data for this reading
                    }
                    Element::Prcp => {
                        if let Some(value) = reading.values.get(value_index) {
                            prcps.push(*value);
                        } else {
                            prcps.push(None);
                        }
                        tmaxs.push(None); // No tmax data for this reading
                        tmins.push(None); // No tmin data for this reading
                    }
                    _ => {
                        // Placeholder for other elements if needed
                        tmaxs.push(None);
                        tmins.push(None);
                        prcps.push(None);
                    }
                }
                rows_processed += 1;
                rows_in_batch += 1;
                pb.inc(1);

                if rows_in_batch == batch_size {
                    break;
                }
            }

            if rows_in_batch == batch_size {
                break;
            }
        }

        // Create Arrow arrays from vectors
        let ids_array = StringArray::from(ids);
        let date32s_array = Date32Array::from(date32s);
        let tmaxs_array = Float32Array::from(tmaxs);
        let tmins_array = Float32Array::from(tmins);
        let prcps_array = Float32Array::from(prcps);
        let lats_array = Float32Array::from(lats);
        let lons_array = Float32Array::from(lons);

        // Create a vector for the RecordBatch
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("id", Arc::new(ids_array) as ArrayRef),
            ("date", Arc::new(date32s_array) as ArrayRef),
            ("tmax", Arc::new(tmaxs_array) as ArrayRef),
            ("tmin", Arc::new(tmins_array) as ArrayRef),
            ("prcp", Arc::new(prcps_array) as ArrayRef),
            ("lat", Arc::new(lats_array) as ArrayRef),
            ("lon", Arc::new(lons_array) as ArrayRef),
        ];

        // Ensure all columns have the same number of rows
        let num_rows = columns.first().map(|(_, col)| col.len()).unwrap_or(0);
        for (name, array) in &columns {
            assert_eq!(
                array.len(),
                num_rows,
                "Column '{}' has incorrect number of rows: expected {}, got {}",
                name,
                num_rows,
                array.len()
            );
        }

        // Create RecordBatch
        let batch = RecordBatch::try_from_iter(columns).expect("Failed to create record batch");

        writer.write(&batch)?;
    }
    pb.finish_with_message("Finished writing Parquet file");

    writer.close()?;
    Ok(())
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod test {
    use std::fs;
    use arrow::array::Array;
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
    fn should_validate_parquet_schema_and_data() {
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
            
            // Validate schema
            let schema = batch.schema();
            assert_eq!(schema.fields().len(), 7);
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(1).name(), "date");
            assert_eq!(schema.field(2).name(), "tmax");
            assert_eq!(schema.field(3).name(), "tmin");
            assert_eq!(schema.field(4).name(), "prcp");
            assert_eq!(schema.field(5).name(), "lat");
            assert_eq!(schema.field(6).name(), "lon");
            
            // Count non-null values in each measurement column
            let tmax_array = batch.column(2);
            let tmin_array = batch.column(3);
            let prcp_array = batch.column(4);
            
            tmax_count += tmax_array.len() - tmax_array.null_count();
            tmin_count += tmin_array.len() - tmin_array.null_count();
            prcp_count += prcp_array.len() - prcp_array.null_count();
        }
        
        // Validate expected data structure
        // Each reading should produce 31 rows (days), so 3 readings = 93 rows
        assert_eq!(total_rows, 93);
        
        // Only tmax reading should have non-null tmax values (31 values)
        // Only tmin reading should have non-null tmin values (31 values)  
        // Only prcp reading should have non-null prcp values (31 values)
        assert_eq!(tmax_count, 31);
        assert_eq!(tmin_count, 31);
        assert_eq!(prcp_count, 31);
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
