//! Handles serialising and saving data to disk in the _parquet_ file format.

pub mod daily;
pub mod monthly;

pub use daily::save_daily;
pub use monthly::save_monthly;
