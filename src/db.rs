use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::{migrate::MigrateDatabase, QueryBuilder, Sqlite, SqlitePool};

use crate::reading::Reading;

pub async fn insert_readings(readings: Vec<Reading>, db_name: &str) -> Result<()> {
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

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new("");

    for reading in readings.iter() {
        for (day, value) in reading.values.iter().enumerate() {
            qb.push("INSERT INTO readings (station_id, year, month, day, element, value) VALUES (");
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
            qb.push("); ");
            progress_bar.inc(1);
        }
    }

    qb.build().execute(&pool).await?;

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
    async fn should_insert_readings() {
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

        insert_readings(readings, "test_db").await.unwrap();
    }
}
