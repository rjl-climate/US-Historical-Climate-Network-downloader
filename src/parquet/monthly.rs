use anyhow::Result;
use arrow::{
    array::{ArrayRef, Date32Array, Float32Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use chrono::{Datelike, NaiveDate};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{fs::File, path::PathBuf, sync::Arc};

use crate::{
    cli::make_progress_bar,
    reading::{
        monthly::{Dataset, Element},
        MonthlyReading,
    },
};

pub fn save_monthly(readings: &[MonthlyReading], file_path: &PathBuf) -> Result<()> {
    let months_per_year = 12;
    let chunk_size = 100000;
    let total_rows = readings.len() * months_per_year;

    // Initialize the Parquet writer
    let file = File::create(file_path)?;

    // Define the schema for the RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("date", DataType::Date32, true),
        Field::new("max_raw", DataType::Float32, true),
        Field::new("max_tob", DataType::Float32, true),
        Field::new("max_fls52", DataType::Float32, true),
        Field::new("min_raw", DataType::Float32, true),
        Field::new("min_tob", DataType::Float32, true),
        Field::new("min_fls52", DataType::Float32, true),
        Field::new("avg_raw", DataType::Float32, true),
        Field::new("avg_tob", DataType::Float32, true),
        Field::new("avg_fls52", DataType::Float32, true),
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
        let mut max_raws = Vec::with_capacity(batch_size);
        let mut max_tobs = Vec::with_capacity(batch_size);
        let mut max_fls52s = Vec::with_capacity(batch_size);
        let mut min_raws = Vec::with_capacity(batch_size);
        let mut min_tobs = Vec::with_capacity(batch_size);
        let mut min_fls52s = Vec::with_capacity(batch_size);
        let mut avg_raws = Vec::with_capacity(batch_size);
        let mut avg_tobs = Vec::with_capacity(batch_size);
        let mut avg_fls52s = Vec::with_capacity(batch_size);

        let mut rows_in_batch = 0;

        for r in &readings[rows_processed / months_per_year..] {
            let id = r.id.as_str();
            let year = r.year;

            for month in 1..=months_per_year {
                ids.push(id);

                // Convert year, month, and day to a NaiveDate
                let date = NaiveDate::from_ymd_opt(year as i32, month as u32, 1);
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

                let idx = month - 1;
                match r.element {
                    Element::Max => match r.dataset {
                        Dataset::Raw => {
                            push_value_or_none(&r.values, idx, &mut max_raws);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_raws.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                        Dataset::Tob => {
                            push_value_or_none(&r.values, idx, &mut max_tobs);
                            max_raws.push(None);
                            max_fls52s.push(None);
                            min_raws.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                        Dataset::Fls52 => {
                            push_value_or_none(&r.values, idx, &mut max_fls52s);
                            max_raws.push(None);
                            max_tobs.push(None);
                            min_raws.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                        _ => {
                            max_raws.push(None);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_raws.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                    },
                    Element::Min => match r.dataset {
                        Dataset::Raw => {
                            push_value_or_none(&r.values, idx, &mut min_raws);
                            max_raws.push(None);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                        Dataset::Tob => {
                            push_value_or_none(&r.values, idx, &mut min_tobs);
                            max_raws.push(None);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_raws.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                        Dataset::Fls52 => {
                            push_value_or_none(&r.values, idx, &mut min_fls52s);
                            max_raws.push(None);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_raws.push(None);
                            min_tobs.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                        _ => {
                            min_raws.push(None);
                            max_raws.push(None);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                    },
                    Element::Avg => match r.dataset {
                        Dataset::Raw => {
                            push_value_or_none(&r.values, idx, &mut avg_raws);
                            max_raws.push(None);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_raws.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                        Dataset::Tob => {
                            push_value_or_none(&r.values, idx, &mut avg_tobs);
                            max_raws.push(None);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_raws.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_fls52s.push(None);
                        }
                        Dataset::Fls52 => {
                            push_value_or_none(&r.values, idx, &mut avg_fls52s);
                            max_raws.push(None);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_raws.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                        }
                        _ => {
                            max_raws.push(None);
                            max_tobs.push(None);
                            max_fls52s.push(None);
                            min_raws.push(None);
                            min_tobs.push(None);
                            min_fls52s.push(None);
                            avg_raws.push(None);
                            avg_tobs.push(None);
                            avg_fls52s.push(None);
                        }
                    },
                    _ => {
                        max_raws.push(None);
                        max_tobs.push(None);
                        max_fls52s.push(None);
                        min_raws.push(None);
                        min_tobs.push(None);
                        min_fls52s.push(None);
                        avg_raws.push(None);
                        avg_tobs.push(None);
                        avg_fls52s.push(None);
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
        let max_raws_array = Float32Array::from(max_raws);
        let max_tobs_array = Float32Array::from(max_tobs);
        let max_fls52s_array = Float32Array::from(max_fls52s);
        let min_raws_array = Float32Array::from(min_raws);
        let min_tobs_array = Float32Array::from(min_tobs);
        let min_fls52s_array = Float32Array::from(min_fls52s);
        let avg_raws_array = Float32Array::from(avg_raws);
        let avg_tobs_array = Float32Array::from(avg_tobs);
        let avg_fls52s_array = Float32Array::from(avg_fls52s);

        // Create a vector for the RecordBatch
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("id", Arc::new(ids_array) as ArrayRef),
            ("date", Arc::new(date32s_array) as ArrayRef),
            ("max_raw", Arc::new(max_raws_array) as ArrayRef),
            ("max_tob", Arc::new(max_tobs_array) as ArrayRef),
            ("max_fls52", Arc::new(max_fls52s_array) as ArrayRef),
            ("min_raw", Arc::new(min_raws_array) as ArrayRef),
            ("min_tob", Arc::new(min_tobs_array) as ArrayRef),
            ("min_fls52", Arc::new(min_fls52s_array) as ArrayRef),
            ("avg_raw", Arc::new(avg_raws_array) as ArrayRef),
            ("avg_tob", Arc::new(avg_tobs_array) as ArrayRef),
            ("avg_fls52", Arc::new(avg_fls52s_array) as ArrayRef),
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

fn push_value_or_none(values: &Vec<Option<f32>>, index: usize, target_vec: &mut Vec<Option<f32>>) {
    if let Some(value) = values.get(index) {
        target_vec.push(*value);
    } else {
        target_vec.push(None);
    }
}
