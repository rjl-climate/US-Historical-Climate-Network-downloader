//! Monthly reading data structure and parsing logic.

use anyhow::Result;

use super::FileProperties;

#[derive(Debug)]
pub struct MonthlyReading {
    pub id: String,
    pub year: u16,
    pub month: Option<u16>,
    pub properties: FileProperties,
    pub values: Vec<Option<f32>>,
}

impl MonthlyReading {
    pub fn from_line(line: &str, file_name: &str) -> Result<Self> {
        let id = line[0..11].to_string();
        let year = line[12..16].parse()?;
        let month = None;
        let properties = FileProperties::from_file(file_name)?;
        let values = parse_monthly_values(line);

        Ok(MonthlyReading {
            id,
            year,
            month,
            properties,
            values,
        })
    }
}

fn parse_monthly_values(line: &str) -> Vec<Option<f32>> {
    // Pad the line with extra spaces to ensure we can extract the expected number of chunks
    let mut padded_line = line.to_string();
    padded_line.push_str("  ");

    let start_pos = 17;
    let chunk_length = 9;
    let num_chunks = 12;
    let line_length = padded_line.len();

    // Ensure the line is long enough to contain the expected number of chunks
    if line_length < start_pos + num_chunks * chunk_length {
        panic!(
            "Line length is too short: expected at least {}, got {}",
            start_pos + num_chunks * chunk_length,
            line_length
        );
    }

    let values: Vec<Option<f32>> = (0..num_chunks)
        .map(|i| {
            let chunk_start = start_pos + i * chunk_length;
            let chunk_end = start_pos + (i + 1) * chunk_length;

            let chunk = &padded_line[chunk_start..chunk_end];
            let first_five = &chunk[..5].trim();
            match first_five.parse::<i32>() {
                Ok(v) if v != -9999 => Some((v as f32) / 100.0),
                _ => None,
            }
        })
        .collect();

    values
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn should_parse_line() {
        let line = "USH0048961511894   517a     377a    1096d    1640b    2231     2485a   -9999     2938    -9999    -9999    -9999    -9999    -9999   ";

        let filename = "USH00297610.raw.tmax";

        let reading = MonthlyReading::from_line(line, filename).unwrap();

        assert_eq!(reading.id, "USH00489615");
        assert_eq!(reading.year, 1894);
        assert_eq!(reading.values.len(), 12);
        assert_eq!(reading.values[0], Some(5.17));
        assert_eq!(reading.values[7], Some(29.38));
        assert_eq!(reading.values[11], None);

        println!("{:#?}", reading.values);
    }

    #[test]
    fn should_parse_short_line() {
        let line = "USH0045726711892 -9999      532    -9999    -9999 Q   1869b    2209     2481     2734     2233     1711      777  3   -50  3";

        let filename = "USH00297610.raw.tmax";

        let reading = MonthlyReading::from_line(line, filename).unwrap();

        assert_eq!(reading.id, "USH00457267");
        assert_eq!(reading.year, 1892);
        assert_eq!(reading.values.len(), 12);
        assert_eq!(reading.values[0], None);
        assert_eq!(reading.values[7], Some(27.34));
        assert_eq!(reading.values[11], Some(-0.5));

        println!("{:#?}", reading.values);
    }
}
