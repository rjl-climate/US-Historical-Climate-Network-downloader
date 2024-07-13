use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use rusqlite::{params, Connection, OpenFlags};
use tokio::task;

use crate::reading::Reading;

pub async fn process_readings(readings: Vec<Reading>, db_path: PathBuf) -> Result<()> {
    let number_of_readings = readings.len() * 31;
    let pb = make_progress_bar(number_of_readings);

    reset_db(db_path.clone())?;

    let db_path = Arc::new(db_path.to_string_lossy().to_string());

    let tasks: Vec<_> = readings
        .chunks(100) // Process in chunks to reduce overhead
        .map(|chunk| {
            let db_path = Arc::clone(&db_path);
            let pb = Arc::clone(&pb);
            let chunk = chunk.to_vec();
            task::spawn(async move {
                insert_readings(chunk, db_path, pb)
                    .await
                    .unwrap_or_else(|err| {
                        eprintln!("Error inserting readings: {}", err);
                    });
            })
        })
        .collect();

    for task in tasks {
        task.await?;
    }

    Ok(())
}

async fn insert_readings(
    readings: Vec<Reading>,
    db_path: Arc<String>,
    pb: Arc<Mutex<ProgressBar>>,
) -> Result<()> {
    let mut conn = Connection::open(db_path.as_ref())?;
    conn.pragma_update(None, "journal_mode", &"WAL")?;

    let tx = conn.transaction()?;

    // Execute batch insertion
    for r in readings {
        // Prepare the batch insert statement
        let mut stmt = tx.prepare(
            r#"
                INSERT INTO readings (station_id, year, month, day, element, value)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
        )?;

        for (idx, value) in r.values.iter().enumerate() {
            stmt.execute(params![
                &r.id,
                &r.year,
                &r.month,
                &(idx + 1), // day
                &r.element,
                value,
            ])?;
        }

        {
            let pb = pb.lock().unwrap();
            pb.inc(r.values.len() as u64);
        }
    }

    // Commit the transaction
    tx.commit()?;
    Ok(())
}

fn reset_db(db_path: PathBuf) -> Result<()> {
    // Reset the database
    let conn = Connection::open(db_path.clone())?;

    conn.execute("DROP TABLE IF EXISTS readings", [])?;
    conn.execute(
        "CREATE TABLE readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            element TEXT NOT NULL,
            value REAL
        )",
        (),
    )?;

    Ok(())
}

fn make_progress_bar(number_of_readings: usize) -> Arc<Mutex<ProgressBar>> {
    let progress_bar = ProgressBar::new(number_of_readings as u64).with_message("Saving database");
    progress_bar.set_style(
        ProgressStyle::with_template("[{eta_precise}] {bar:40.cyan/blue} {pos:>10}/{len:10} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );
    Arc::new(Mutex::new(progress_bar))
}
