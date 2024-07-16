use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow::{
    array::{ArrayRef, Date32Array, Float32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::{Datelike, NaiveDate};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use crate::{cli::make_progress_bar, reading::DailyReading};

pub fn save_daily(readings: &[DailyReading], file_path: &PathBuf) -> Result<()> {
    // let readings = &readings[..100000];
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
    ]));

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut rows_processed = 0;

    // Prepare vectors to hold column data
    let pb = make_progress_bar(total_rows as u64, "Writing parquet file chunks");

    while rows_processed < total_rows {
        let remaining_rows = total_rows - rows_processed;
        let batch_size = chunk_size.min(remaining_rows);

        let mut ids = Vec::with_capacity(batch_size);
        let mut date32s = Vec::with_capacity(batch_size);
        let mut tmaxs = Vec::with_capacity(batch_size);
        let mut tmins = Vec::with_capacity(batch_size);
        let mut prcps = Vec::with_capacity(batch_size);

        let mut rows_in_batch = 0;

        for reading in &readings[rows_processed / days_per_month..] {
            let id = reading.id.as_str();
            let year = reading.year;
            let month = reading.month;

            for day in 1..=days_per_month {
                ids.push(id);

                // Convert year, month, and day to a NaiveDate
                let date = NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32);
                match date {
                    Some(valid_date) => {
                        let date32 = valid_date.num_days_from_ce()
                            - NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .num_days_from_ce();
                        date32s.push(Some(date32 as i32));
                    }
                    None => {
                        date32s.push(None);
                    }
                }

                let value_index = day - 1;
                match reading.element.as_str().to_lowercase().as_str() {
                    "tmax" => {
                        if let Some(value) = reading.values.get(value_index) {
                            tmaxs.push(*value);
                        } else {
                            tmaxs.push(None);
                        }
                        tmins.push(None); // No tmin data for this reading
                        prcps.push(None); // No prcp data for this reading
                    }
                    "tmin" => {
                        if let Some(value) = reading.values.get(value_index) {
                            tmins.push(*value);
                        } else {
                            tmins.push(None);
                        }
                        tmaxs.push(None); // No tmax data for this reading
                        prcps.push(None); // No prcp data for this reading
                    }
                    "prcp" => {
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

        // Create a vector for the RecordBatch
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("id", Arc::new(ids_array) as ArrayRef),
            ("date", Arc::new(date32s_array) as ArrayRef),
            ("tmax", Arc::new(tmaxs_array) as ArrayRef),
            ("tmin", Arc::new(tmins_array) as ArrayRef),
            ("prcp", Arc::new(prcps_array) as ArrayRef),
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

    fn readings_fixture() -> Vec<DailyReading> {
        let mut values = vec![];
        for v in 0..31 {
            values.push(Some((v as f32) + 10.0));
        }

        vec![
            DailyReading {
                id: "USW00094728".to_string(),
                year: 2019,
                month: 1,
                element: "TMAX".to_string(),
                values: values.clone(),
            },
            DailyReading {
                id: "USW00094729".to_string(),
                year: 2020,
                month: 2,
                element: "TMIN".to_string(),
                values: values,
            },
        ]
    }
}
