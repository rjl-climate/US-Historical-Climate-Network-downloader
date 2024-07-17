//! Monthly reading dataset and element.

use anyhow::{anyhow, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Element {
    Max,
    Min,
    Avg,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Dataset {
    Fls52,
    Raw,
    Tob,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct FileProperties {
    pub dataset: Dataset,
    pub element: Element,
}

impl FileProperties {
    pub fn from_file(file_name: &str) -> Result<Self> {
        let parts: Vec<&str> = file_name.split('.').collect();

        match parts.len() {
            3 => {
                let dataset = match parts[1] {
                    "raw" => Dataset::Raw,
                    "tob" => Dataset::Tob,
                    _ => Dataset::Unknown,
                };
                let element = match parts[2] {
                    "tmax" => Element::Max,
                    "tmin" => Element::Min,
                    "tavg" => Element::Avg,
                    _ => Element::Unknown,
                };

                Ok(FileProperties { element, dataset })
            }
            4 => {
                let dataset = Dataset::Fls52;
                let element = match parts[3] {
                    "tmax" => Element::Max,
                    "tmin" => Element::Min,
                    "tavg" => Element::Avg,
                    _ => Element::Unknown,
                };

                Ok(FileProperties { dataset, element })
            }
            _ => Err(anyhow!("Invalid file format: {}", file_name)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn should_get_file_property() {
        let p1 = FileProperties::from_file("USH00297610.tob.tmax").unwrap();
        assert_eq!(p1.dataset, Dataset::Tob);
        assert_eq!(p1.element, Element::Max);

        let p2 = FileProperties::from_file("USH00118916.FLs.52j.tmin").unwrap();
        assert_eq!(p2.dataset, Dataset::Fls52);
        assert_eq!(p2.element, Element::Min);
    }
}
