pub mod daily;
pub mod file_properties;
pub mod monthly;

use anyhow::Result;

pub use daily::DailyReading;
pub use file_properties::{Dataset, Element, FileProperties};
pub use monthly::MonthlyReading;

// Define a trait for deserializing a line into a reading
pub trait Reading: Sized {
    fn from_line(line: &str, file_name: &str) -> Result<Self>;
    fn is_valid(&self) -> bool;
}
