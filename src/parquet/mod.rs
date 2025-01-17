//! Handles serialising and saving data to disk in the _parquet_ file format.

pub mod daily;
pub mod monthly;
pub mod stations;

pub use daily::save_daily;
pub use monthly::save_monthly;
pub use stations::save_stations;
