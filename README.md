# US Historical Climate Network data downloader

[NOAA](https://www.ncei.noaa.gov/products/land-based-station/us-historical-climatology-network) maintains datasets of daily and monthly climate data for the US from 1875 to present. Data includes maximum and minimum temperatures, precipitation, and other climate variables from 1,200+ weather stations with complete geographic coordinates.

This tool downloads and processes two complementary datasets:

- **Daily data**: From the Global Historical Climatology Network (GHCN) with measurements from 1875-present
- **Monthly data**: From the US Historical Climatology Network (USHCN) in three quality-controlled variants:
  - **Raw**: Original unadjusted data as received from stations
  - **TOB**: Time-of-observation bias adjusted data
  - **FLS52**: Fully corrected and homogenized data

The data is distributed as fixed-width text files that require processing for analysis:

```text
USC00011084192601TMAX-9999   -9999   -9999   -9999   -9999   -9999 ...
USC00011084192602TMIN   33  6   22  6   67  6    0  6   11  6   17 ...
USC00011084192602PRCP    0  6  381  6    0  6    0  6    0  6    0 ...
...
```

This repository provides a Rust binary that downloads the daily and monthly datasets, processes them, injects lat/lon coordinate data, and saves them as optimized [Apache Parquet files](https://parquet.apache.org/):

```text
# Daily data (long format with all measurements)
        id       date element  value     lat      lon
0  USC00324418 1898-06-14    TMAX   12.3  34.428  -86.2467
1  USC00324418 1898-06-14    TMIN    6.7  34.428  -86.2467
2  USC00324418 1898-06-14    PRCP    1.2  34.428  -86.2467
3  USC00324418 1898-06-15    TMAX   14.4  34.428  -86.2467
...

# Monthly data (separate files by quality level)
# ushcn-monthly-raw-2025-06-27.parquet     (original data)
# ushcn-monthly-tob-2025-06-27.parquet     (time-adjusted)
# ushcn-monthly-fls52-2025-06-27.parquet   (fully corrected)
```

## Usage

The binary downloads and processes all data types automatically:

```bash
# Download to temporary directory (default)
> ushcn
Downloading and processing US Historical Climate Network data...

Downloading USHCN stations data...
USHCN Stations: /Users/richardlyon/ushcn-stations-2025-06-27.parquet

Downloading GHCN stations data...
GHCN Stations: /Users/richardlyon/ghcnd-stations-2025-06-27.parquet

Processing daily data...
✓ Created daily parquet file with 1,268,938 readings
Daily: Created 1 daily file: /Users/richardlyon/ushcn-daily-2025-06-27.parquet

Processing monthly data...
✓ Created RAW monthly parquet file with 443,135 readings
✓ Created TOB monthly parquet file with 443,117 readings
✓ Created FLS52 monthly parquet file with 491,028 readings
Monthly: Created 3 monthly dataset files: ushcn-monthly-raw-2025-06-27.parquet, ushcn-monthly-tob-2025-06-27.parquet, ushcn-monthly-fls52-2025-06-27.parquet

# Use persistent cache (faster on subsequent runs)
> ushcn --cache
```

## Output Files

The tool generates multiple parquet files optimized for analysis with complete coordinate data:

- **Daily data**: `ushcn-daily-{date}.parquet` - Long format with one row per measurement (~37M rows with 100% lat/lon coverage)
- **Monthly data**: `ushcn-monthly-{dataset}-{date}.parquet` - Separate files for raw, time-adjusted, and fully corrected data (~5M rows each with 100% lat/lon coverage)
- **Station metadata**:
  - `ushcn-stations-{date}.parquet` - USHCN station coordinates (1,218 stations)
  - `ghcnd-stations-{date}.parquet` - GHCN station coordinates (129,000+ stations)

## Python Analysis Example

The optimized parquet files work seamlessly with pandas and other Python data analysis tools:

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load daily data (long format with coordinates)
daily_df = pd.read_parquet("ushcn-daily-2025-06-27.parquet")
daily_df['date'] = pd.to_datetime(daily_df['date'])

print(f"Daily data: {len(daily_df):,} rows with {daily_df['lat'].notna().sum():,} coordinate pairs")
# Output: Daily data: 37,874,655 rows with 37,874,655 coordinate pairs

# Filter for temperature data and plot
tmax_data = daily_df[daily_df['element'] == 'TMAX'].set_index('date')
tmax_monthly = tmax_data.groupby(pd.Grouper(freq='M'))['value'].mean()
tmax_monthly.plot(title="Average Monthly Maximum Temperature")

# Compare raw vs. corrected monthly data (both with full coordinates)
raw_monthly = pd.read_parquet("ushcn-monthly-raw-2025-06-27.parquet")
corrected_monthly = pd.read_parquet("ushcn-monthly-fls52-2025-06-27.parquet")

print(f"Monthly coverage: {raw_monthly['lat'].notna().sum() / len(raw_monthly) * 100:.1f}%")
# Output: Monthly coverage: 100.0%

# Geospatial analysis with complete coordinate data
import geopandas as gpd
station_coords = daily_df[['id', 'lat', 'lon']].drop_duplicates()
gdf = gpd.GeoDataFrame(station_coords,
                      geometry=gpd.points_from_xy(station_coords.lon, station_coords.lat))
```

![max_temp](max_temp.png)

## Features

- **Complete coordinate coverage**: All data includes precise latitude/longitude coordinates (100% coverage)
- **Dual dataset integration**: Combines GHCN daily and USHCN monthly data with appropriate station metadata
- **Multi-dataset support**: Automatically generates separate files for raw, time-adjusted, and fully corrected monthly data
- **Optimized format**: Uses ZSTD compression and dictionary encoding for efficient storage and fast Python loading
- **Long format**: Daily data uses a tidy long format optimal for analysis (one row per measurement)
- **Caching**: Optional persistent caching to speed up subsequent runs
- **Error handling**: Robust parsing with bounds checking for malformed data records
- **Parallel processing**: Concurrent downloads and processing for improved performance
- **Geospatial ready**: Perfect for spatial analysis, mapping, and climate research

## Change log

- 0.2.4 - Complete coordinate injection fix: 100% lat/lon coverage for both daily and monthly data
- 0.2.3 - Major refactor: Multi-parquet output, optimized compression, simplified CLI, parsing fixes
- 0.2.2 - Add lat/lon to daily readings
- 0.2.1 - Fix bug in monthly data download
