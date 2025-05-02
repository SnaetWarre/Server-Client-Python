#!/usr/bin/env python3
# Data analysis and preprocessing of arrest data
# This script performs exploratory data analysis and preprocessing on the arrest dataset
# to prepare it for potential AI model training.

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os

# Set the style for visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette('viridis')

# Define and create the output directory for plots
plots_dir = 'Data/plots'
os.makedirs(plots_dir, exist_ok=True)
print(f"Plots will be saved in: {plots_dir}")


print("Starting data analysis and preprocessing...")

# Step 1: Load the dataset
# ------------------------------------------------------------
print("\n1. Loading the dataset...")
file_path = 'Data/Arrest_Data_from_2020_to_Present.csv'
df = pd.read_csv(file_path, low_memory=False)

# Display basic information about the dataset
print(f"Dataset dimensions: {df.shape}")
print("\nFirst few rows:")
print(df.head())
print("\nData types:")
print(df.dtypes)
print("\nSummary statistics:")
print(df.describe())

# Check for missing values
print("\nMissing values per column:")
missing_values = df.isnull().sum()
missing_percent = (missing_values / len(df)) * 100
missing_info = pd.DataFrame({
    'Missing Values': missing_values,
    'Percentage': missing_percent
})
print(missing_info[missing_info['Missing Values'] > 0])

# Step 2: Data Cleaning - Handle dates and times
# ------------------------------------------------------------
print("\n2. Cleaning dates and times...")

# Convert date strings to datetime objects
date_columns = [col for col in df.columns if 'Date' in col or 'Time' in col]
print(f"Date and time columns: {date_columns}")

# Function to safely convert date columns
def safe_date_conversion(df, column):
    try:
        if column in df.columns:
            if 'Time' in column and 'Date' not in column:
                # Handle time-only columns
                df[column] = pd.to_datetime(df[column], errors='coerce', format='%H%M')
            else:
                # Handle date or datetime columns
                df[column] = pd.to_datetime(df[column], errors='coerce')
            return True
    except Exception as e:
        print(f"Error converting {column}: {e}")
        return False
    return False

# Convert date columns
for col in date_columns:
    if safe_date_conversion(df, col):
        print(f"Converted {col} to datetime")

# Extract additional temporal features if 'Arrest Date' was successfully converted
if pd.api.types.is_datetime64_dtype(df['Arrest Date']):
    df['Arrest Year'] = df['Arrest Date'].dt.year
    df['Arrest Month'] = df['Arrest Date'].dt.month
    df['Arrest Day'] = df['Arrest Date'].dt.day
    df['Arrest Weekday'] = df['Arrest Date'].dt.day_name()
    df['Arrest Hour'] = pd.to_datetime(df['Time'], format='%H%M', errors='coerce').dt.hour
    print("Created temporal features: Year, Month, Day, Weekday, Hour")

# Step 3: Handle missing values and incorrect data
# ------------------------------------------------------------
print("\n3. Handling missing values and incorrect data...")

# For simplicity, we'll drop rows with missing values in critical columns
critical_columns = ['Report ID', 'Arrest Date', 'Area Name', 'Charge Group Description']
df_clean = df.dropna(subset=critical_columns)
print(f"Rows before cleaning: {len(df)}")
print(f"Rows after cleaning critical columns: {len(df_clean)}")

# Check for age outliers and fix them
print("\nAge statistics before cleaning:")
print(df_clean['Age'].describe())

# Fix age outliers (e.g., assume valid age range is 10-100)
df_clean = df_clean[(df_clean['Age'] >= 10) & (df_clean['Age'] <= 100)]
print("\nAge statistics after cleaning:")
print(df_clean['Age'].describe())

# Step 4: Feature Engineering and Encoding
# ------------------------------------------------------------
print("\n4. Feature engineering and encoding...")

# Encode categorical variables
categorical_cols = ['Sex Code', 'Descent Code', 'Charge Group Code', 'Arrest Type Code']

for col in categorical_cols:
    if col in df_clean.columns:
        # Get value counts and display the distribution
        print(f"\nDistribution of {col}:")
        value_counts = df_clean[col].value_counts()
        print(value_counts)
        
        # Create dummy variables for categorical columns with few unique values
        if len(value_counts) < 20:  # Only one-hot encode if there are few categories
            dummies = pd.get_dummies(df_clean[col], prefix=col)
            df_clean = pd.concat([df_clean, dummies], axis=1)
            print(f"Created {len(dummies.columns)} dummy variables for {col}")

# Create a more general location feature using LAT and LON
if 'LAT' in df_clean.columns and 'LON' in df_clean.columns:
    # Round coordinates to group nearby locations
    df_clean['Location_Cluster'] = df_clean.apply(
        lambda row: f"{round(row['LAT'], 2)}_{round(row['LON'], 2)}" 
        if pd.notnull(row['LAT']) and pd.notnull(row['LON']) else "Unknown", 
        axis=1
    )
    print(f"Created Location_Cluster feature with {df_clean['Location_Cluster'].nunique()} unique areas")

# Step 5: Data Visualization
# ------------------------------------------------------------
print("\n5. Creating visualizations...")

# Plot 1: Arrests over time (by month)
if 'Arrest Date' in df_clean.columns and pd.api.types.is_datetime64_dtype(df_clean['Arrest Date']):
    plt.figure(figsize=(14, 7))
    df_clean.groupby(df_clean['Arrest Date'].dt.to_period('M')).size().plot(kind='line')
    plt.title('Number of Arrests Over Time (Monthly)')
    plt.xlabel('Month')
    plt.ylabel('Number of Arrests')
    plt.grid(True)
    save_path = os.path.join(plots_dir, 'arrests_over_time.png')
    plt.savefig(save_path)
    plt.close()
    print(f"Created plot: {save_path}")

# Plot 2: Age distribution
plt.figure(figsize=(12, 6))
sns.histplot(df_clean['Age'], bins=30, kde=True)
plt.title('Age Distribution of Arrested Individuals')
plt.xlabel('Age')
plt.ylabel('Count')
save_path = os.path.join(plots_dir, 'age_distribution.png')
plt.savefig(save_path)
plt.close()
print(f"Created plot: {save_path}")

# Plot 3: Top 10 charge groups
plt.figure(figsize=(14, 8))
top_charges = df_clean['Charge Group Description'].value_counts().head(10)
sns.barplot(x=top_charges.index, y=top_charges.values)
plt.title('Top 10 Charge Groups')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
save_path = os.path.join(plots_dir, 'top_charges.png')
plt.savefig(save_path)
plt.close()
print(f"Created plot: {save_path}")

# Plot 4: Arrest distribution by area
plt.figure(figsize=(14, 8))
area_counts = df_clean['Area Name'].value_counts()
sns.barplot(x=area_counts.index, y=area_counts.values)
plt.title('Arrests by Area')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
save_path = os.path.join(plots_dir, 'arrests_by_area.png')
plt.savefig(save_path)
plt.close()
print(f"Created plot: {save_path}")

# Plot 5: Improved Heatmap of correlations between meaningful features
plt.figure(figsize=(16, 14))
print("\nCreating improved correlation heatmap...")

# Select more meaningful features for correlation analysis
# Remove ID columns and select a mix of numerical and encoded categorical features
meaningful_cols = ['Age', 'Arrest Year', 'Arrest Month', 'Arrest Day', 'Arrest Hour', 'LAT', 'LON']

# Add dummy variables for categorical columns that might have meaningful correlations
for col in ['Sex Code', 'Descent Code', 'Charge Group Code', 'Arrest Type Code']:
    # Get top 3 most common categories for each categorical variable
    if col in df_clean.columns:
        top_categories = df_clean[col].value_counts().head(3).index.tolist()
        # Add the dummy variables for these top categories
        for category in top_categories:
            dummy_col = f"{col}_{category}"
            if dummy_col in df_clean.columns:
                meaningful_cols.append(dummy_col)

# Add the Area Name dummies (top 5 areas)
for area in df_clean['Area Name'].value_counts().head(5).index.tolist():
    area_col = f"Area_{area.replace(' ', '_')}"
    df_clean[area_col] = (df_clean['Area Name'] == area).astype(int)
    meaningful_cols.append(area_col)

# Add weekday information (convert to numerical - Monday=0, Sunday=6)
if 'Arrest Weekday' in df_clean.columns:
    weekday_map = {
        'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 
        'Friday': 4, 'Saturday': 5, 'Sunday': 6
    }
    df_clean['Weekday_Num'] = df_clean['Arrest Weekday'].map(weekday_map)
    meaningful_cols.append('Weekday_Num')

# Create a flag for weekend arrests
df_clean['Is_Weekend'] = df_clean['Arrest Weekday'].isin(['Saturday', 'Sunday']).astype(int)
meaningful_cols.append('Is_Weekend')

# Generate an improved correlation matrix with only meaningful columns
# Filter to include only columns that exist in the dataframe
existing_cols = [col for col in meaningful_cols if col in df_clean.columns]
print(f"Calculating correlations for {len(existing_cols)} meaningful features")

if len(existing_cols) > 1:  # Need at least 2 columns for correlation
    correlation_matrix = df_clean[existing_cols].corr()
    
    # Create a mask for the upper triangle
    mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
    
    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(230, 20, as_cmap=True)
    
    # Draw the heatmap with the mask and correct aspect ratio
    sns.heatmap(
        correlation_matrix, 
        mask=mask,
        annot=True,      # Show the correlation values 
        fmt=".2f",       # Format as 2 decimal places
        cmap=cmap,       # Use the custom colormap
        center=0,        # Center the colormap at 0
        square=True,     # Make the cells square
        linewidths=.5,   # Width of cell borders
        cbar_kws={"shrink": .8},  # Shrink the colorbar
        vmin=-0.5,       # Custom min value to avoid extreme colors
        vmax=0.5         # Custom max value to avoid extreme colors
    )
    
    plt.title('Correlation Heatmap of Meaningful Features', fontsize=16)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=10)
    plt.tight_layout()
    save_path = os.path.join(plots_dir, 'improved_correlation_heatmap.png')
    plt.savefig(save_path)
    plt.close()
    print(f"Created plot: {save_path}")
else:
    print("Not enough meaningful columns found for correlation analysis")

# Additional visualization 1: Arrests by day of week and time of day
if 'Arrest Weekday' in df_clean.columns and 'Arrest Hour' in df_clean.columns:
    print("\nCreating heatmap of arrests by day of week and hour...")
    plt.figure(figsize=(14, 8))
    
    # Create a crosstab of weekday and hour
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    arrests_by_time = pd.crosstab(
        index=pd.Categorical(df_clean['Arrest Weekday'], categories=weekday_order, ordered=True),
        columns=df_clean['Arrest Hour']
    )
    
    # Plot the heatmap
    sns.heatmap(arrests_by_time, cmap='YlOrRd', annot=False, fmt="d", cbar_kws={'label': 'Number of Arrests'})
    plt.title('Arrests by Day of Week and Hour', fontsize=16)
    plt.xlabel('Hour of Day (24h)', fontsize=12)
    plt.ylabel('Day of Week', fontsize=12)
    plt.tight_layout()
    save_path = os.path.join(plots_dir, 'arrests_by_day_and_hour.png')
    plt.savefig(save_path)
    plt.close()
    print(f"Created plot: {save_path}")

# Additional visualization 2: Age distribution by gender
if 'Sex Code' in df_clean.columns and 'Age' in df_clean.columns:
    print("\nCreating age distribution by gender...")
    plt.figure(figsize=(14, 8))
    
    # Filter for the main gender categories
    gender_map = {'M': 'Male', 'F': 'Female'}
    df_gender = df_clean[df_clean['Sex Code'].isin(['M', 'F'])].copy()
    df_gender['Gender'] = df_gender['Sex Code'].map(gender_map)
    
    # Create the violin plot
    sns.violinplot(x='Gender', y='Age', data=df_gender, palette='Set1', inner='quartile')
    plt.title('Age Distribution by Gender', fontsize=16)
    plt.xlabel('Gender', fontsize=12)
    plt.ylabel('Age', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.close()
    print("Created plot: age_distribution_by_gender.png")

# Additional visualization 3: Top charge types by area (stacked bar chart)
if 'Area Name' in df_clean.columns and 'Charge Group Description' in df_clean.columns:
    print("\nCreating top charge types by area...")
    plt.figure(figsize=(16, 10))
    
    # Get top 5 areas and top 5 charge types
    top_areas = df_clean['Area Name'].value_counts().head(5).index.tolist()
    top_charges = df_clean['Charge Group Description'].value_counts().head(5).index.tolist()
    
    # Filter data for these top areas and charges
    df_filtered = df_clean[
        (df_clean['Area Name'].isin(top_areas)) & 
        (df_clean['Charge Group Description'].isin(top_charges))
    ]
    
    # Create a pivot table
    pivot_data = pd.crosstab(
        index=df_filtered['Area Name'],
        columns=df_filtered['Charge Group Description'],
        normalize='index'  # Normalize by row (area) to show percentages
    ) * 100  # Convert to percentage
    
    # Plot stacked bar chart
    pivot_data.plot(kind='bar', stacked=True, figsize=(16, 10), colormap='tab10')
    plt.title('Top 5 Charge Types by Top 5 Areas (Percentage)', fontsize=16)
    plt.xlabel('Area', fontsize=12)
    plt.ylabel('Percentage', fontsize=12)
    plt.legend(title='Charge Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.close()
    print("Created plot: charge_types_by_area.png")

# Additional visualization 4: Arrests trend by year and month (time series)
if 'Arrest Year' in df_clean.columns and 'Arrest Month' in df_clean.columns:
    print("\nCreating arrests trend by year and month...")
    plt.figure(figsize=(16, 8))
    
    # Create year-month column for proper time series ordering
    df_clean['Year_Month'] = df_clean['Arrest Year'].astype(str) + '-' + df_clean['Arrest Month'].astype(str).str.zfill(2)
    
    # Count arrests by year-month
    monthly_counts = df_clean['Year_Month'].value_counts().sort_index()
    
    # Plot the time series
    plt.plot(monthly_counts.index, monthly_counts.values, marker='o', linestyle='-', linewidth=2, markersize=8)
    plt.title('Monthly Arrest Trends', fontsize=16)
    plt.xlabel('Year-Month', fontsize=12)
    plt.ylabel('Number of Arrests', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.close()
    print("Created plot: monthly_arrest_trends.png")

# Save the processed dataset
df_clean.to_csv('Data/processed_arrest_data.csv', index=False)
print("\nSaved processed dataset to Data/processed_arrest_data.csv")

print("\nData analysis and preprocessing complete!") 