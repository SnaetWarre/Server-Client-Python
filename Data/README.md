# Arrest Data Analysis and Preprocessing

This directory contains the files and scripts used for analyzing and preprocessing the "Arrest Data from 2020 to Present" dataset.

## Dataset

The dataset `Arrest_Data_from_2020_to_Present.csv` contains records of arrests from 2020 to the present, including information about:
- Report and arrest details (ID, type, date, time)
- Geographic information (area, district, address, coordinates)
- Arrestee demographics (age, sex, descent)
- Charge information (group, description, type)
- Processing information (booking details, disposition)

## Analysis Process

The analysis is performed in the `data_analysis.py` script, which conducts the following steps:

### 1. Data Loading and Exploration
- Load the raw CSV data
- Display basic information (dimensions, data types, summary statistics)
- Check for missing values

### 2. Data Cleaning - Dates and Times
- Convert date and time strings to proper datetime objects
- Extract additional temporal features (year, month, day, weekday, hour)

### 3. Missing Values and Outliers
- Remove rows with missing values in critical columns
- Identify and handle age outliers (restricting to 10-100 years)

### 4. Feature Engineering and Encoding
- One-hot encode categorical variables (Sex Code, Descent Code, etc.)
- Create location clusters from latitude and longitude data
- Generate weekend arrest flags and weekday numerical mapping
- Create area-specific feature flags for top areas

### 5. Data Visualization
- **Time Series Analysis**: Arrests over time by month and year-month trends
- **Demographic Analysis**: Age distribution overall and by gender
- **Geographic Analysis**: Arrest distribution by area
- **Temporal Patterns**: Heatmap of arrests by day of week and hour
- **Charge Analysis**: Top charge groups and charge types by area
- **Feature Correlation**: Improved heatmap of correlations between meaningful features

## Files

- `Arrest_Data_from_2020_to_Present.csv`: Original dataset
- `data_analysis.py`: Python script for analysis and preprocessing
- `processed_arrest_data.csv`: Cleaned and processed dataset
- `plots/`: Directory containing visualization outputs:
  - `arrests_over_time.png`: Time series of arrests by month
  - `age_distribution.png`: Distribution of ages
  - `top_charges.png`: Top 10 charge groups
  - `arrests_by_area.png`: Arrests by geographic area
  - `improved_correlation_heatmap.png`: Heatmap showing correlations between meaningful features
  - `arrests_by_day_and_hour.png`: Heatmap showing arrest patterns by day and hour
  - `age_distribution_by_gender.png`: Violin plot of age distribution by gender
  - `charge_types_by_area.png`: Stacked bar chart of charge types by area
  - `monthly_arrest_trends.png`: Time series of monthly arrest trends

## Preprocessing Steps

The script performs the following preprocessing steps to prepare the data for AI model training:

1. **Date/Time Conversion**: Converting string representations to proper datetime objects
2. **Missing Value Handling**: Removing rows with missing critical information
3. **Outlier Removal**: Filtering out invalid age values outside the 10-100 range
4. **Feature Engineering**: Creating new features from existing data:
   - Temporal features (year, month, day, weekday, hour)
   - Location clustering based on coordinates
   - Weekend/weekday indicators
   - Area-specific indicator variables
5. **Categorical Encoding**: One-hot encoding categorical variables with manageable cardinality

## Running the Analysis

To run the analysis script:

```bash
cd /path/to/project
python Data/data_analysis.py
```

This will generate all visualizations in the `Data/plots` directory and save the processed dataset to `Data/processed_arrest_data.csv`. 