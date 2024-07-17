pub mod daily;
pub mod file_properties;
pub mod monthly;

use anyhow::Result;

pub use daily::DailyReading;
pub use file_properties::{Dataset, Element, FileProperties};
pub use monthly::MonthlyReading;
