//! Reading dataset and element.

use anyhow::Result;

#[derive(Debug, Clone)]
/// Represents the type of dataset and the measurement type.
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
            _ => Ok(FileProperties {
                dataset: Dataset::Unknown,
                element: Element::Unknown,
            }),
        }
    }

    pub fn from_element(element: &str) -> Result<Self> {
        let element = Element::from_str(element);
        let dataset = Dataset::Unknown;

        Ok(FileProperties { element, dataset })
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Represents the type of dataset. See the [NOAA documentation](https://www1.ncdc.noaa.gov/pub/data/cdo/documentation/gsom-gsoy_documentation.pdf)
/// for more information.
pub enum Dataset {
    Fls52,
    Raw,
    Tob,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
/// Represents the type of measurement. See the [NOAA documentation](https://www1.ncdc.noaa.gov/pub/data/cdo/documentation/gsom-gsoy_documentation.pdf)
/// for more information.
pub enum Element {
    Max,
    Min,
    Avg,
    Prcp,
    Tmax,
    Tmin,
    Unknown,
}

impl Element {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "prcp" => Element::Prcp,
            "tmax" => Element::Tmax,
            "tmin" => Element::Tmin,
            _ => Element::Unknown,
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

    #[test]
    fn should_get_none_for_unknown_file() {
        let p = FileProperties::from_file("unknown.file").unwrap();
        assert_eq!(p.dataset, Dataset::Unknown);
        assert_eq!(p.element, Element::Unknown);
    }
}
