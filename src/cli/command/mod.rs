pub mod daily;
pub mod monthly;

use std::path::PathBuf;

use chrono::{Datelike, Local};
pub use daily::daily;
pub use monthly::monthly;

pub fn make_parquet_file_name(period: &str) -> PathBuf {
    let today = Local::now();
    let file_name = format!(
        "ushcn-{}-{}-{:02}-{:02}.parquet",
        period,
        today.year(),
        today.month(),
        today.day()
    );

    dirs::home_dir().unwrap().join(file_name)
}
