use std::{fs::File, sync::Arc, time::Duration};

use crate::reading::Reading;
use anyhow::Result;
use arrow::{
    array::{Array, ArrayRef, Float64Array, Int32Array, StringArray},
    record_batch::RecordBatch,
};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};

pub fn persist(readings: &[Reading], file_name: &str) -> Result<()> {
    make_batch_multi_column(readings, file_name)?;

    Ok(())
}

// Create a parquet file with a column of readings for each element type
// This may make deserialisation faster if only one element is needed
fn make_batch_multi_column(readings: &[Reading], file_name: &str) -> Result<()> {
    // Initialise the progress bar
    // let records = readings.len() * 31;
    // let pb = make_progress_bar(readings.len(), "Initialising");

    // Create the record batch
    let bar = ProgressBar::new_spinner().with_message("Initialising arrays");
    bar.enable_steady_tick(Duration::from_millis(100));

    let ids: Vec<&str> = readings.iter().map(|r| r.id.as_str()).collect();
    let years: Vec<i32> = readings.iter().map(|r| r.year as i32).collect();
    let months: Vec<i32> = readings.iter().map(|r| r.month as i32).collect();

    // Initialize vectors for each element type
    let mut tmax_values: Vec<Vec<Option<f32>>> = vec![vec![None; readings.len()]; 31];
    let mut tmin_values: Vec<Vec<Option<f32>>> = vec![vec![None; readings.len()]; 31];
    // Add more vectors here for other elements as needed

    bar.finish();

    let size = readings.len() as u64;
    let pb = make_progress_bar(size, "Loading value arrays");

    for (i, reading) in readings.iter().enumerate() {
        for day in 0..31 {
            match reading.element.as_str() {
                "TMAX" => tmax_values[day][i] = reading.values[day],
                "TMIN" => tmin_values[day][i] = reading.values[day],
                // Add more cases here for other elements as needed
                _ => (),
            }
        }
        pb.inc(1);
    }

    pb.finish_with_message("Value arrays loaded");

    let ids = StringArray::from(ids);
    let years = Int32Array::from(years);
    let months = Int32Array::from(months);

    let bar = ProgressBar::new_spinner().with_message("Initialising max/min values");
    bar.enable_steady_tick(Duration::from_millis(100));

    // Create Arrow arrays for each day of the month for each element type
    let tmax_columns: Vec<Arc<dyn arrow::array::Array>> = tmax_values
        .iter()
        .enumerate()
        .map(|(_day, values)| {
            Arc::new(Float64Array::from(
                values
                    .clone()
                    .into_iter()
                    .map(|v| v.map(|f| f as f64))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();

    let tmin_columns: Vec<Arc<dyn arrow::array::Array>> = tmin_values
        .iter()
        .enumerate()
        .map(|(day, values)| {
            Arc::new(Float64Array::from(
                values
                    .clone()
                    .into_iter()
                    .map(|v| v.map(|f| f as f64))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();

    // Create more columns for other elements as needed

    // Collect all columns for the RecordBatch
    let mut columns: Vec<(&str, Arc<dyn arrow::array::Array>)> = vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("year", Arc::new(years) as ArrayRef),
        ("month", Arc::new(months) as ArrayRef),
    ];

    let mut tmax_column_names: Vec<String> = Vec::new();
    let mut tmin_column_names: Vec<String> = Vec::new();

    for day in 0..31 {
        tmax_column_names.push(format!("TMAX_day_{}", day + 1));
        tmin_column_names.push(format!("TMIN_day_{}", day + 1));
    }

    for day in 0..31 {
        columns.push((tmax_column_names[day].as_str(), tmax_columns[day].clone()));
        columns.push((tmin_column_names[day].as_str(), tmin_columns[day].clone()));
        // Add more element columns for other days as needed
    }

    let batch = RecordBatch::try_from_iter(columns).expect("Failed to create record batch");

    bar.finish();

    // Write it to a parquet file
    let bar = ProgressBar::new_spinner().with_message("Saving parquet file");
    bar.enable_steady_tick(Duration::from_millis(100));

    save(&batch, file_name)?;

    bar.finish();

    Ok(())
}

fn save(batch: &RecordBatch, file_name: &str) -> Result<()> {
    let file = File::create(format!("{file_name}.parquet"))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn read_readings(file_name: &str) -> Result<Vec<Reading>> {
    // Open the parquet file and get the record batch reader
    let file = File::open(format!("{file_name}.parquet"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let batch = reader.next().unwrap()?;

    println!("Read {} records.", batch.num_rows());

    // deserialise the record batch

    let mut readings: Vec<Reading> = Vec::new();

    // Extract columns
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let years = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let months = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    // Initialize maps to store element values
    let mut tmax_values: Vec<Vec<Option<f32>>> = vec![vec![None; batch.num_rows()]; 31];
    let mut tmin_values: Vec<Vec<Option<f32>>> = vec![vec![None; batch.num_rows()]; 31];

    for day in 0..31 {
        let tmax_column = batch
            .column(3 + day * 2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let tmin_column = batch
            .column(4 + day * 2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            tmax_values[day][i] = if tmax_column.is_null(i) {
                None
            } else {
                Some(tmax_column.value(i) as f32)
            };
            tmin_values[day][i] = if tmin_column.is_null(i) {
                None
            } else {
                Some(tmin_column.value(i) as f32)
            };
        }

        // Add more elements as needed
    }

    // Combine into readings
    for row in 0..batch.num_rows() {
        let id = ids.value(row).to_string();
        let year = years.value(row);
        let month = months.value(row);

        for day in 0..31 {
            readings.push(Reading {
                id: id.clone(),
                year: year as u16,
                month: month as u8,
                element: format!("TMAX_day_{}", day + 1),
                values: tmax_values[day].clone(),
            });
            readings.push(Reading {
                id: id.clone(),
                year: year as u16,
                month: month as u8,
                element: format!("TMIN_day_{}", day + 1),
                values: tmin_values[day].clone(),
            });
            // Add more elements as needed
        }
    }

    Ok(readings)
}

fn make_progress_bar(size: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(size).with_message(message.to_string());
    pb.set_style(
        ProgressStyle::with_template("[{eta_precise}] {bar:40.cyan/blue} {pos:>10}/{len:10} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );
    pb
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn should_round_trip() {
        // arrange
        let readings = readings_fixture();
        assert_eq!(readings[0].values[0], Some(10.0));
        assert_eq!(readings[0].values[30], Some(40.0));

        // act
        make_batch_multi_column(&readings, "test").unwrap();
        let readings = read_readings("test").unwrap();

        // assert
        assert_eq!(readings[0].id, "USW00094728");
        assert_eq!(readings[0].year, 2019);
        assert_eq!(readings[0].month, 1);
        assert_eq!(readings[0].element, "TMAX_day_1");
    }

    fn readings_fixture() -> Vec<Reading> {
        let mut values = vec![];
        for v in 0..31 {
            values.push(Some((v as f32) + 10.0));
        }

        vec![
            Reading {
                id: "USW00094728".to_string(),
                year: 2019,
                month: 1,
                element: "TMAX".to_string(),
                values: values.clone(),
            },
            Reading {
                id: "USW00094729".to_string(),
                year: 2020,
                month: 2,
                element: "TMIN".to_string(),
                values: values,
            },
        ]
    }
}
