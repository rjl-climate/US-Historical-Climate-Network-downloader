use std::{fs::File, sync::Arc, time::Duration};

use anyhow::Result;
use arrow::{
    array::{ArrayRef, Float32Array, StringArray, UInt16Array, UInt8Array},
    record_batch::RecordBatch,
};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use crate::reading::Reading;

pub fn save_parquet(readings: &[Reading], file_name: &str) -> Result<()> {
    let days_per_month = 31;
    let total_rows = (readings.len() * days_per_month);

    // Prepare vectors to hold column data
    let pb = make_progress_bar(total_rows as u64, "Preparing vectors");

    let mut ids = Vec::with_capacity(total_rows);
    let mut years = Vec::with_capacity(total_rows);
    let mut months = Vec::with_capacity(total_rows);
    let mut days = Vec::with_capacity(total_rows);
    let mut tmaxs = Vec::with_capacity(total_rows);
    let mut tmins = Vec::with_capacity(total_rows);

    for reading in readings {
        let id = reading.id.as_str();
        let year = reading.year;
        let month = reading.month;

        for day in 1..=days_per_month {
            ids.push(id);
            years.push(year);
            months.push(month);
            days.push(day as u16); // Assuming day_idx is 0-based

            let value_index = day - 1;
            match reading.element.as_str() {
                "tmax" => {
                    if let Some(value) = reading.values.get(value_index) {
                        tmaxs.push(*value);
                    } else {
                        tmaxs.push(None);
                    }
                    tmins.push(None); // No tmin data for this reading
                }
                "tmin" => {
                    if let Some(value) = reading.values.get(value_index) {
                        tmins.push(*value);
                    } else {
                        tmins.push(None);
                    }
                    tmaxs.push(None); // No tmax data for this reading
                }
                _ => {
                    // Placeholder for other elements if needed
                    tmaxs.push(None);
                    tmins.push(None);
                }
            }
            pb.inc(1);
        }
    }
    pb.finish_with_message("Vectors prepared");

    // Create Arrow arrays from vectors
    let bar = ProgressBar::new_spinner().with_message("Creating arrow arrays");
    bar.enable_steady_tick(Duration::from_millis(100));

    let ids_array = StringArray::from(ids);
    let years_array = UInt16Array::from(years);
    let months_array = UInt8Array::from(months);
    let days_array = UInt16Array::from(days);
    let tmaxs_array = Float32Array::from(tmaxs);
    let tmins_array = Float32Array::from(tmins);

    // Create a vector for the RecordBatch
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("id", Arc::new(ids_array) as ArrayRef),
        ("year", Arc::new(years_array) as ArrayRef),
        ("month", Arc::new(months_array) as ArrayRef),
        ("day", Arc::new(days_array) as ArrayRef),
        ("tmax", Arc::new(tmaxs_array) as ArrayRef),
        ("tmin", Arc::new(tmins_array) as ArrayRef),
    ];
    bar.finish();

    // Ensure all columns have the same number of rows
    let bar = ProgressBar::new_spinner().with_message("Checking");
    bar.enable_steady_tick(Duration::from_millis(100));

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
    bar.finish();

    // Create RecordBatch
    let bar = ProgressBar::new_spinner().with_message("Creating record batch");
    bar.enable_steady_tick(Duration::from_millis(100));

    let batch = RecordBatch::try_from_iter(columns).expect("Failed to create record batch");

    bar.finish();

    // Initialize the Parquet writer
    let bar = ProgressBar::new_spinner().with_message("Saving");
    bar.enable_steady_tick(Duration::from_millis(100));
    let file = File::create(format!("{file_name}.parquet"))?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    bar.finish();

    Ok(())
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
    fn should_round_trip_multi_column() {
        // arrange
        let readings = readings_fixture();
        assert_eq!(readings[0].values[0], Some(10.0));
        assert_eq!(readings[0].values[30], Some(40.0));

        // act
        save_parquet(&readings, "test").unwrap();
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
