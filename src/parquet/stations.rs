//! Save the stations readings to a parquet file.

use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow::{
    array::{ArrayRef, Float32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use crate::cli::command::stations::Station;

pub fn save_stations(stations: &[Station], file_path: &PathBuf) -> Result<()> {
    // Initialize the Parquet writer
    let file = File::create(file_path)?;

    // Define the schema for the RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("country_code", DataType::Utf8, false),
        Field::new("network_code", DataType::Utf8, false),
        Field::new("id_placeholder", DataType::Utf8, false),
        Field::new("coop_id", DataType::Utf8, false),
        Field::new("latitude", DataType::Float32, true),
        Field::new("longitude", DataType::Float32, true),
        Field::new("elevation", DataType::Float32, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let num_rows = stations.len();

    let mut country_codes = Vec::with_capacity(num_rows);
    let mut network_codes = Vec::with_capacity(num_rows);
    let mut id_placeholders = Vec::with_capacity(num_rows);
    let mut coop_ids = Vec::with_capacity(num_rows);
    let mut latitudes = Vec::with_capacity(num_rows);
    let mut longitudes = Vec::with_capacity(num_rows);
    let mut elevations = Vec::with_capacity(num_rows);
    let mut states = Vec::with_capacity(num_rows);
    let mut names = Vec::with_capacity(num_rows);

    for s in stations {
        country_codes.push(s.country_code.clone());
        network_codes.push(s.network_code.clone());
        id_placeholders.push(s.id_placeholder.clone());
        coop_ids.push(s.coop_id.clone());
        latitudes.push(s.latitude);
        longitudes.push(s.longitude);
        elevations.push(s.elevation);
        states.push(s.state.clone());
        names.push(s.name.clone());
    }

    // Create Arrow arrays from vectors
    let country_code_array = StringArray::from(country_codes);
    let network_code_array = StringArray::from(network_codes);
    let id_placeholder_array = StringArray::from(id_placeholders);
    let coop_id_array = StringArray::from(coop_ids);
    let latitude_array = Float32Array::from(latitudes);
    let longitude_array = Float32Array::from(longitudes);
    let elevation_array = Float32Array::from(elevations);
    let state_array = StringArray::from(states);
    let name_array = StringArray::from(names);

    // Create a vector for the RecordBatch
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("country_code", Arc::new(country_code_array)),
        ("network_code", Arc::new(network_code_array)),
        ("id_placeholder", Arc::new(id_placeholder_array)),
        ("coop_id", Arc::new(coop_id_array)),
        ("latitude", Arc::new(latitude_array)),
        ("longitude", Arc::new(longitude_array)),
        ("elevation", Arc::new(elevation_array)),
        ("state", Arc::new(state_array)),
        ("name", Arc::new(name_array)),
    ];

    // Create RecordBatch
    let batch = RecordBatch::try_from_iter(columns).expect("Failed to create record batch");

    writer.write(&batch)?;

    writer.close()?;

    Ok(())
}
