//! Daily reading data structure and parsing logic.

use anyhow::Result;

use super::{Element, FileProperties, Reading};

#[derive(Debug, Clone)]
/// Represents a daily reading.
pub struct DailyReading {
    /// station identifier
    pub id: String,
    /// station latitude
    pub lat: Option<f32>,
    /// station longitude
    pub lon: Option<f32>,
    /// year of the reading
    pub year: u16,
    /// month of the reading
    pub month: Option<u16>,
    /// properties of the reading
    pub properties: FileProperties,
    /// reading values
    pub values: Vec<Option<f32>>,
}

impl Reading for DailyReading {
    fn from_line(line: &str, _file_name: &str) -> Result<Self> {
        let id = line[0..11].to_string();
        let lat = None; // FIXME
        let lon = None; // FIXME
        let year = line[11..15].parse()?;
        let month = Some(line[15..17].parse()?);
        let element = line[17..21].to_string();
        let properties = FileProperties::from_element(&element)?;
        let values = parse_daily_values(line);

        Ok(DailyReading {
            id,
            lat,
            lon,
            year,
            month,
            properties,
            values,
        })
    }

    fn is_valid(&self) -> bool {
        [Element::Prcp, Element::Tmax, Element::Tmin].contains(&self.properties.element)
    }
}

/// Parses the daily values from a line.
fn parse_daily_values(line: &str) -> Vec<Option<f32>> {
    let start_pos = 21;
    let chunk_length = 8;
    let num_chunks = 31;

    let values: Vec<Option<f32>> = (0..num_chunks)
        .map(|i| {
            let chunk = &line[start_pos + i * chunk_length..start_pos + (i + 1) * chunk_length];
            let first_five = &chunk[..5].trim();
            match first_five.parse::<i32>() {
                Ok(v) if v != -9999 => Some((v as f32) / 10.0),
                _ => None,
            }
        })
        .collect();

    values
}

// -- Tests ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::reading::Element;

    use super::*;

    #[test]
    fn should_parse_line() {
        let line = "USC00011084192601TOBS-9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999     217  6   28  6   39  6   44  6  100  6  106  6  117  6  106  6  128  6   94  6  189  6";
        let reading = DailyReading::from_line(line, "").unwrap();

        assert_eq!(reading.id, "USC00011084");
        assert_eq!(reading.year, 1926);
        assert_eq!(reading.month, Some(1));
        assert_eq!(reading.properties.element, Element::Unknown);
        assert_eq!(reading.values.len(), 31);
        assert_eq!(reading.values[0], None);
        assert_eq!(reading.values[30], Some(18.9));
    }
}
