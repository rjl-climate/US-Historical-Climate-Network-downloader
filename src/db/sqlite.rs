use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::{migrate::MigrateDatabase, QueryBuilder, Sqlite, SqlitePool};

use crate::reading::Reading;

pub async fn persist(readings: Vec<Reading>, db_name: &str) -> Result<()> {
    // Initialise the progress bar
    let records = readings.len() * 31;
    let progress_bar = ProgressBar::new(records as u64).with_message("Updating database");
    progress_bar.set_style(
        ProgressStyle::with_template("[{eta_precise}] {bar:40.cyan/blue} {pos:>10}/{len:10} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    // Initialise the database
    let database_url = create_db(db_name).await?;
    let pool = SqlitePool::connect(&database_url).await?;

    sqlx::query("PRAGMA synchronous = NORMAL")
        .execute(&pool)
        .await?;

    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&pool)
        .await?;

    let mut transaction = pool.begin().await?;

    // Chunk size for batch processing
    let chunk_size = 100;

    for chunk in readings.chunks(chunk_size) {
        let mut qb = QueryBuilder::<Sqlite>::new(
            "INSERT INTO readings (station_id, year, month, day, element, value) VALUES ",
        );

        for (i, reading) in chunk.iter().enumerate() {
            for (day, value) in reading.values.iter().enumerate() {
                if i != 0 || day != 0 {
                    qb.push(", ");
                }
                qb.push("(");
                qb.push_bind(&reading.id);
                qb.push(", ");
                qb.push_bind(reading.year as i32); // Cast to i32 for SQL compatibility
                qb.push(", ");
                qb.push_bind(reading.month as i32); // Cast to i32 for SQL compatibility
                qb.push(", ");
                qb.push_bind((day + 1) as i32); // Day starts from 1, so add 1 to index
                qb.push(", ");
                qb.push_bind(&reading.element);
                qb.push(", ");
                qb.push_bind(value);
                qb.push(")");
            }
            progress_bar.inc(1);
        }

        // Build and execute the full batch of inserts for this chunk
        let query = qb.build();
        // let sql = query.sql();
        // println!("->> {}", sql);
        query.execute(&mut *transaction).await?;
    }

    // Commit the transaction after all chunks have been processed
    transaction.commit().await?;

    progress_bar.finish_with_message("Database updated");

    Ok(())
}

async fn create_db(database_name: &str) -> Result<String> {
    let database_url = format!("sqlite://{database_name}.sqlite");

    if !Sqlite::database_exists(&database_url)
        .await
        .unwrap_or(false)
    {
        match Sqlite::create_database(&database_url).await {
            Ok(_) => {}
            Err(error) => panic!("error: {}", error),
        }
    }

    let db = SqlitePool::connect(&database_url).await.unwrap();
    sqlx::query("DROP TABLE IF EXISTS readings")
        .execute(&db)
        .await?;

    sqlx::query(
        "CREATE TABLE readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            element TEXT NOT NULL,
            value REAL)",
    )
    .execute(&db)
    .await?;

    Ok(database_url)
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::reading::Reading;

    #[tokio::test]
    #[ignore]
    async fn should_persist_readings() {
        let readings = vec![
            Reading {
                id: "USW00094728".to_string(),
                year: 2019,
                month: 1,
                element: "TMAX".to_string(),
                values: vec![Some(1.0), Some(2.0), Some(3.0)],
            },
            Reading {
                id: "USW00094728".to_string(),
                year: 2019,
                month: 1,
                element: "TMIN".to_string(),
                values: vec![Some(1.0), Some(2.0), Some(3.0)],
            },
        ];

        persist(readings, "test_db").await.unwrap();
    }
}
