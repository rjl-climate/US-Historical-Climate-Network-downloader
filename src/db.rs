use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use rusqlite::Connection;
use tokio::task;

use crate::reading::Reading;

pub async fn write_readings_to_sqlite(readings: Vec<Reading>, db_path: PathBuf) -> Result<()> {
    let number_of_readings = readings.len() * 31;
    let progress_bar = Arc::new(Mutex::new(
        ProgressBar::new(number_of_readings as u64).with_message("Saving to database"),
    ));
    progress_bar.lock().unwrap().set_style(
        ProgressStyle::with_template("[{eta_precise}] {bar:40.cyan/blue} {pos:>10}/{len:10} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

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

    let db_path = Arc::new(db_path);
    let pb = Arc::clone(&progress_bar);
    let mut tasks = Vec::new();

    for r in readings {
        let db_path = Arc::clone(&db_path);
        let pb = Arc::clone(&pb);
        tasks.push(task::spawn(async move {
            let mut conn = Connection::open(db_path.as_ref())?;
            let tx = conn.transaction()?;

            for (idx, value) in r.values.iter().enumerate() {
                tx.execute(
                    r#"
                        INSERT INTO readings (station_id, year, month, day, element, value)
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    (&r.id, &r.year, &r.month, idx + 1, &r.element, value),
                )?;
                {
                    let pb = pb.lock().unwrap();
                    pb.inc(1);
                }
            }

            tx.commit()?;
            Ok::<(), rusqlite::Error>(())
        }));
    }

    let results = futures::future::join_all(tasks).await;
    for result in results {
        result??; // Unwrap and propagate any errors
    }

    progress_bar
        .lock()
        .unwrap()
        .finish_with_message("Processing complete");

    Ok(())
}
