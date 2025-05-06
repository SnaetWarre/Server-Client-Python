#!/usr/bin/env python3
# Data processor for server-side queries

# --- Force Matplotlib backend BEFORE importing pyplot or seaborn ---
import matplotlib
matplotlib.use('Agg') # Use the Agg backend for non-interactive plotting in threads
# -------------------------------------------------------------

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # No longer needed for Query 4 plot
import seaborn as sns # No longer needed for Query 4 plot
import io
from matplotlib.figure import Figure # No longer needed
import folium
from folium.plugins import MarkerCluster
import logging
from datetime import datetime
import urllib.request # No longer needed for OSM background
from math import log, tan, pi, cos, sinh, atan
import uuid # For unique filenames
import tempfile # <-- ADD IMPORT
import json # <-- ADD IMPORT FOR GEOJSON PARSING

# Set the style for visualizations
plt.style.use('seaborn-v0_8-darkgrid') # Keep for other plots
sns.set_palette('viridis') # Keep for other plots

logger = logging.getLogger('data_processor')

# Import the mapping
from shared.constants import DESCENT_CODE_MAP, ARREST_TYPE_CODE_MAP

class DataProcessor:
    """Data processor for handling queries on the dataset"""
    
    def __init__(self, dataset_path='Data/processed_arrest_data.csv'):
        """Initialize data processor with dataset"""
        self.dataset_path = dataset_path
        self.df = None
        self.load_data()
    
    def load_data(self):
        """Load dataset"""
        try:
            self.df = pd.read_csv(self.dataset_path, low_memory=False)
            print(f"Dataset loaded with {len(self.df)} rows and {len(self.df.columns)} columns")
            
            # Convert date columns to datetime if they exist
            for col in self.df.columns:
                if 'Date' in col and self.df[col].dtype == 'object':
                    try:
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                    except:
                        pass # Keep original if conversion fails
            
            # Ensure 'Arrest Date' is datetime
            if 'Arrest Date' in self.df.columns:
                self.df['Arrest Date'] = pd.to_datetime(self.df['Arrest Date'], errors='coerce')
                # Drop rows where Arrest Date couldn't be parsed if necessary
                self.df.dropna(subset=['Arrest Date'], inplace=True)

            # --- Try to parse Location_GeoJSON if it exists ---
            if 'Location_GeoJSON' in self.df.columns and self.df['Location_GeoJSON'].dtype == 'object':
                logger.info("Attempting to parse 'Location_GeoJSON' column...")
                def parse_geojson_str(geojson_str):
                    if pd.isna(geojson_str) or not isinstance(geojson_str, str):
                        return None
                    try:
                        # Replace single quotes with double quotes for valid JSON
                        valid_json_str = geojson_str.replace("'", "\"")
                        return json.loads(valid_json_str)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Could not parse GeoJSON string: {geojson_str[:100]}... Error: {e}")
                        return None
                
                self.df['Location_GeoJSON_Parsed'] = self.df['Location_GeoJSON'].apply(parse_geojson_str)
                parsed_count = self.df['Location_GeoJSON_Parsed'].notna().sum()
                total_geojson_entries = len(self.df[self.df['Location_GeoJSON'].notna()])
                logger.info(f"Parsed {parsed_count} out of {total_geojson_entries} non-null entries in 'Location_GeoJSON'.")
            # --- End GeoJSON parsing ---
            
            return True
        except Exception as e:
            print(f"Error loading dataset: {e}")
            return False
    
    def process_query(self, query_type, parameters=None):
        """
        Process a query based on query type and parameters
        
        Returns:
        - A dictionary with results, which may include dataframes and/or figures
        """
        if self.df is None:
            return {'status': 'error', 'message': 'Dataset not loaded'}
        
        if parameters is None:
            parameters = {}
        
        try:
            if query_type == 'age_distribution':
                return self.get_age_distribution(parameters)
            elif query_type == 'top_charge_groups':
                return self.get_top_charge_groups(parameters)
            elif query_type == 'arrests_by_area':
                return self.get_arrests_by_area(parameters)
            elif query_type == 'arrests_by_time':
                return self.get_arrests_by_time(parameters)
            elif query_type == 'arrests_by_month':
                return self.get_arrests_by_month(parameters)
            elif query_type == 'charge_types_by_area':
                return self.get_charge_types_by_area(parameters)
            elif query_type == 'arrests_by_gender':
                return self.get_arrests_by_gender(parameters)
            elif query_type == 'arrests_by_age_range':
                return self.get_arrests_by_age_range(parameters)
            elif query_type == 'arrests_by_weekday':
                return self.get_arrests_by_weekday(parameters)
            elif query_type == 'correlation_analysis':
                return self.get_correlation_analysis(parameters)
            else:
                return {'status': 'error', 'message': f'Unknown query type: {query_type}'}
        except Exception as e:
            import traceback
            return {
                'status': 'error', 
                'message': f'Error processing query: {str(e)}',
                'traceback': traceback.format_exc()
            }
    
    def get_age_distribution(self, parameters):
        """Get age distribution of arrested individuals"""
        age_counts = self.df['Age'].value_counts().sort_index().reset_index()
        age_counts.columns = ['Age', 'Count']
        
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.histplot(self.df['Age'], bins=30, kde=True, ax=ax)
        ax.set_title('Age Distribution of Arrested Individuals')
        ax.set_xlabel('Age')
        ax.set_ylabel('Count')
        ax.grid(True)
        
        return {
            'status': 'ok',
            'data': age_counts,
            'figure': fig,
            'title': 'Age Distribution of Arrested Individuals'
        }
    
    def get_top_charge_groups(self, parameters):
        """Get top charge groups by frequency"""
        # Get number of top groups to return (default 10)
        n = parameters.get('n', 10)
        n = int(n)
        
        top_charges = self.df['Charge Group Description'].value_counts().head(n).reset_index()
        top_charges.columns = ['Charge Group', 'Count']
        
        fig, ax = plt.subplots(figsize=(12, 8))
        sns.barplot(x='Count', y='Charge Group', data=top_charges, ax=ax)
        ax.set_title(f'Top {n} Charge Groups')
        ax.set_xlabel('Count')
        ax.set_ylabel('Charge Group')
        ax.grid(True, axis='x')
        
        return {
            'status': 'ok',
            'data': top_charges,
            'figure': fig,
            'title': f'Top {n} Charge Groups'
        }
    
    def get_arrests_by_area(self, parameters):
        """Get arrests by geographic area"""
        # Get number of top areas to return (default 15)
        n = parameters.get('n', 15)
        n = int(n)
        
        area_counts = self.df['Area Name'].value_counts().head(n).reset_index()
        area_counts.columns = ['Area', 'Count']
        
        fig, ax = plt.subplots(figsize=(12, 8))
        sns.barplot(x='Count', y='Area', data=area_counts, ax=ax)
        ax.set_title(f'Top {n} Areas by Number of Arrests')
        ax.set_xlabel('Number of Arrests')
        ax.set_ylabel('Area')
        ax.grid(True, axis='x')
        
        return {
            'status': 'ok',
            'data': area_counts,
            'figure': fig,
            'title': f'Top {n} Areas by Number of Arrests'
        }
    
    def get_arrests_by_time(self, parameters):
        """Get arrests by time of day"""
        if 'Arrest Hour' not in self.df.columns:
            return {'status': 'error', 'message': 'Arrest Hour column not found in dataset'}
        
        hour_counts = self.df['Arrest Hour'].value_counts().sort_index().reset_index()
        hour_counts.columns = ['Hour', 'Count']
        
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.barplot(x='Hour', y='Count', data=hour_counts, ax=ax)
        ax.set_title('Arrests by Hour of Day')
        ax.set_xlabel('Hour (24-hour format)')
        ax.set_ylabel('Number of Arrests')
        ax.set_xticks(range(len(hour_counts)))
        ax.set_xticklabels(hour_counts['Hour'])
        ax.grid(True, axis='y')
        
        return {
            'status': 'ok',
            'data': hour_counts,
            'figure': fig,
            'title': 'Arrests by Hour of Day'
        }
    
    def get_arrests_by_month(self, parameters):
        """Get arrests by month"""
        # Get year filter if provided
        year = parameters.get('year', None)
        
        # Filter by year if specified
        if year:
            year = int(year)
            df_filtered = self.df[self.df['Arrest Year'] == year]
        else:
            df_filtered = self.df
        
        # Check if month column exists
        if 'Arrest Month' not in df_filtered.columns:
            return {'status': 'error', 'message': 'Arrest Month column not found in dataset'}
        
        # Create table result
        month_counts = df_filtered['Arrest Month'].value_counts().sort_index().reset_index()
        month_counts.columns = ['Month', 'Count']
        
        # Map month numbers to names
        month_names = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
            7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }
        month_counts['Month Name'] = month_counts['Month'].map(month_names)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.barplot(x='Month Name', y='Count', data=month_counts, ax=ax)
        
        title = 'Arrests by Month' if not year else f'Arrests by Month in {year}'
        ax.set_title(title)
        ax.set_xlabel('Month')
        ax.set_ylabel('Number of Arrests')
        plt.xticks(rotation=45)
        ax.grid(True, axis='y')
        plt.tight_layout()
        
        return {
            'status': 'ok',
            'data': month_counts,
            'figure': fig,
            'title': title
        }
    
    def get_charge_types_by_area(self, parameters):
        """Get charge types by area"""
        # Get number of top areas and charges to include
        n_areas = int(parameters.get('n_areas', 5))
        n_charges = int(parameters.get('n_charges', 5))
        
        top_areas = self.df['Area Name'].value_counts().head(n_areas).index.tolist()
        top_charges = self.df['Charge Group Description'].value_counts().head(n_charges).index.tolist()
        
        # Filter data for these top areas and charges
        df_filtered = self.df[
            (self.df['Area Name'].isin(top_areas)) & 
            (self.df['Charge Group Description'].isin(top_charges))
        ]
        
        # Create a pivot table
        pivot_data = pd.crosstab(
            index=df_filtered['Area Name'],
            columns=df_filtered['Charge Group Description'],
            normalize='index'  # Normalize by row (area) to show percentages
        ) * 100  # Convert to percentage
        
        fig, ax = plt.subplots(figsize=(14, 10))
        pivot_data.plot(kind='bar', stacked=True, figsize=(14, 10), colormap='tab10', ax=ax)
        ax.set_title(f'Top {n_charges} Charge Types by Top {n_areas} Areas (Percentage)')
        ax.set_xlabel('Area')
        ax.set_ylabel('Percentage')
        ax.legend(title='Charge Type', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Reset index for the data to be sent
        pivot_table_reset = pivot_data.reset_index()
        
        return {
            'status': 'ok',
            'data': pivot_table_reset,
            'figure': fig,
            'title': f'Top {n_charges} Charge Types by Top {n_areas} Areas'
        }
    
    def get_arrests_by_gender(self, parameters):
        """Get arrests by gender"""
        # Get gender parameter if provided
        selected_gender = parameters.get('gender')
        
        # Filter out invalid gender codes and map to full names
        gender_map = {'M': 'Male', 'F': 'Female'}
        df_gender = self.df[self.df['Sex Code'].isin(['M', 'F'])].copy()
        df_gender['Gender'] = df_gender['Sex Code'].map(gender_map)
        
        # If a specific gender is selected, filter the data
        title = 'Arrests by Gender'
        if selected_gender in ['M', 'F']:
            df_gender = df_gender[df_gender['Sex Code'] == selected_gender]
            selected_gender_name = gender_map[selected_gender]
            title = f'Arrests for {selected_gender_name} Gender'
        
        # Create table result - if specific gender is selected, show age breakdown
        if selected_gender in ['M', 'F']:
            # For a specific gender, show age distribution
            age_bins = [0, 17, 25, 35, 45, 55, 65, 100]
            age_labels = ['0-17', '18-25', '26-35', '36-45', '46-55', '56-65', '66+']
            
            df_gender['Age Group'] = pd.cut(df_gender['Age'], bins=age_bins, labels=age_labels, right=False)
            age_counts = df_gender['Age Group'].value_counts().sort_index().reset_index()
            age_counts.columns = ['Age Group', 'Count']
            
            fig, ax = plt.subplots(figsize=(10, 8))
            sns.barplot(x='Age Group', y='Count', data=age_counts, ax=ax)
            ax.set_title(f'Age Distribution for {selected_gender_name} Arrests')
            ax.set_xlabel('Age Group')
            ax.set_ylabel('Number of Arrests')
            ax.grid(True, axis='y')
            
            fig2, ax2 = plt.subplots(figsize=(12, 8))
            sns.histplot(df_gender['Age'], bins=30, kde=True, ax=ax2)
            ax2.set_title(f'Age Distribution Histogram for {selected_gender_name} Arrests')
            ax2.set_xlabel('Age')
            ax2.set_ylabel('Number of Arrests')
            ax2.grid(True, axis='y')
            
            return {
                'status': 'ok',
                'data': age_counts,
                'figure': fig,
                'figure2': fig2,
                'title': title
            }
        else:
            # For all genders, show gender comparison
            gender_counts = df_gender['Gender'].value_counts().reset_index()
            gender_counts.columns = ['Gender', 'Count']
            
            # Calculate percentages
            total = gender_counts['Count'].sum()
            gender_counts['Percentage'] = (gender_counts['Count'] / total * 100).round(2)
            
            # Create figure - pie chart
            fig, ax = plt.subplots(figsize=(10, 8))
            ax.pie(gender_counts['Count'], labels=gender_counts['Gender'], autopct='%1.1f%%',
                  startangle=90, colors=['#ff9999','#66b3ff'])
            ax.set_title('Arrests by Gender')
            ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
            
            # Create figure 2 - bar chart with age distribution by gender
            fig2, ax2 = plt.subplots(figsize=(12, 8))
            sns.violinplot(x='Gender', y='Age', data=df_gender, ax=ax2, inner='quartile')
            ax2.set_title('Age Distribution by Gender')
            ax2.set_xlabel('Gender')
            ax2.set_ylabel('Age')
            ax2.grid(True, axis='y', linestyle='--', alpha=0.7)
            
            return {
                'status': 'ok',
                'data': gender_counts,
                'figure': fig,
                'figure2': fig2,
                'title': title
            }
    
    def get_arrests_by_age_range(self, parameters):
        """Get arrests by age range"""
        # Check if min_age and max_age parameters are provided
        min_age = parameters.get('min_age')
        max_age = parameters.get('max_age')
        
        if min_age is not None and max_age is not None:
            # Use the specific min_age and max_age parameters from the client
            min_age = int(min_age)
            max_age = int(max_age)
            
            # Create a custom age range
            custom_range = [(min_age, max_age)]
            
            # Filter dataframe by this specific age range
            filtered_df = self.df[(self.df['Age'] >= min_age) & (self.df['Age'] <= max_age)]
            
            # Create histogram of ages within the specified range
            fig, ax = plt.subplots(figsize=(12, 6))
            sns.histplot(filtered_df['Age'], bins=min(30, max_age - min_age + 1), kde=True, ax=ax)
            ax.set_title(f'Age Distribution between {min_age} and {max_age}')
            ax.set_xlabel('Age')
            ax.set_ylabel('Number of Arrests')
            ax.grid(True, axis='y')
            
            # Create a detailed summary by individual ages
            age_counts = filtered_df['Age'].value_counts().sort_index().reset_index()
            age_counts.columns = ['Age', 'Count']
            
            return {
                'status': 'ok',
                'data': age_counts,
                'figure': fig,
                'title': f'Arrests for Ages {min_age}-{max_age}'
            }
        else:
            # Fall back to predefined age ranges for the general case
            age_ranges = [(10, 17), (18, 25), (26, 35), (36, 45), (46, 55), (56, 65), (66, 100)]
            
            # Create results
            results = []
            for age_min, age_max in age_ranges:
                count = len(self.df[(self.df['Age'] >= age_min) & (self.df['Age'] <= age_max)])
                results.append({
                    'Age Range': f'{age_min}-{age_max}',
                    'Count': count
                })
            
            # Create dataframe from results
            age_range_df = pd.DataFrame(results)
            
            # Create figure
            fig, ax = plt.subplots(figsize=(12, 6))
            sns.barplot(x='Age Range', y='Count', data=age_range_df, ax=ax)
            ax.set_title('Arrests by Age Range')
            ax.set_xlabel('Age Range')
            ax.set_ylabel('Number of Arrests')
            ax.grid(True, axis='y')
            
            return {
                'status': 'ok',
                'data': age_range_df,
                'figure': fig,
                'title': 'Arrests by Age Range'
            }
    
    def get_arrests_by_weekday(self, parameters):
        """Get arrests by day of week"""
        # Check if the weekday column exists
        if 'Arrest Weekday' not in self.df.columns:
            return {'status': 'error', 'message': 'Arrest Weekday column not found in dataset'}
        
        # Define weekday order
        weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        # Create table result with proper ordering
        weekday_counts = pd.DataFrame({'Count': self.df['Arrest Weekday'].value_counts()}).reset_index()
        weekday_counts.columns = ['Weekday', 'Count']
        weekday_counts['Weekday'] = pd.Categorical(weekday_counts['Weekday'], categories=weekday_order, ordered=True)
        weekday_counts = weekday_counts.sort_values('Weekday')
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.barplot(x='Weekday', y='Count', data=weekday_counts, ax=ax)
        ax.set_title('Arrests by Day of Week')
        ax.set_xlabel('Day of Week')
        ax.set_ylabel('Number of Arrests')
        ax.grid(True, axis='y')
        
        # Create a more detailed heatmap by hour and day
        if 'Arrest Hour' in self.df.columns:
            fig2, ax2 = plt.subplots(figsize=(14, 8))
            
            # Create a crosstab of weekday and hour
            arrests_by_time = pd.crosstab(
                index=pd.Categorical(self.df['Arrest Weekday'], categories=weekday_order, ordered=True),
                columns=self.df['Arrest Hour']
            )
            
            # Plot the heatmap
            sns.heatmap(arrests_by_time, cmap='YlOrRd', annot=False, fmt="d", 
                       cbar_kws={'label': 'Number of Arrests'}, ax=ax2)
            ax2.set_title('Arrests by Day of Week and Hour')
            ax2.set_xlabel('Hour of Day (24h)')
            ax2.set_ylabel('Day of Week')
            
            return {
                'status': 'ok',
                'data': weekday_counts,
                'figure': fig,
                'figure2': fig2,
                'title': 'Arrests by Day of Week'
            }
        
        return {
            'status': 'ok',
            'data': weekday_counts,
            'figure': fig,
            'title': 'Arrests by Day of Week'
        }
    
    def get_correlation_analysis(self, parameters):
        """Get correlation analysis between features"""
        # Get selected features or use defaults
        default_features = ['Age', 'Arrest Year', 'Arrest Month', 'Arrest Day', 'Arrest Hour']
        
        # Get features list
        features = parameters.get('features', default_features)
        
        # Filter to include only columns that exist in the dataframe
        available_features = [f for f in features if f in self.df.columns]
        
        if len(available_features) < 2:
            return {'status': 'error', 'message': 'At least 2 valid features are required for correlation analysis'}
        
        # Calculate correlation matrix
        correlation_matrix = self.df[available_features].corr()
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 10))
        
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
            ax=ax
        )
        
        ax.set_title('Correlation Analysis')
        plt.tight_layout()
        
        # Create a reset index version of the correlation matrix for the data
        correlation_reset = correlation_matrix.reset_index()
        correlation_reset = correlation_reset.rename(columns={'index': 'Feature'})
        
        return {
            'status': 'ok',
            'data': correlation_reset,
            'figure': fig,
            'title': 'Correlation Analysis'
        }

    # --- Metadata Methods ---

    def get_unique_areas(self):
        """Returns a sorted list of unique area names."""
        if self.df.empty: return []
        try:
            areas = self.df['Area Name'].dropna().unique().tolist()
            return sorted(areas)
        except KeyError:
            logger.warning("Column 'Area Name' not found for get_unique_areas.")
            return []

    def get_unique_charge_groups(self):
        """Returns a sorted list of unique charge group descriptions."""
        if self.df.empty: return []
        try:
            groups = self.df['Charge Group Description'].dropna().unique().tolist()
            return sorted([g for g in groups if isinstance(g, str)]) # Filter out non-strings if any
        except KeyError:
             logger.warning("Column 'Charge Group Description' not found for get_unique_charge_groups.")
             return []

    def get_unique_descent_codes(self):
        """Returns a sorted list of unique descent codes with descriptions.
        Each item in the list is a dictionary: {'code': str, 'description': str}
        """
        if self.df.empty: return []
        try:
            unique_codes = self.df['Descent Code'].dropna().unique().tolist()
            # Filter out non-strings and sort the codes
            valid_codes = sorted([c for c in unique_codes if isinstance(c, str)])

            # Create list of dictionaries with descriptions
            descent_list = []
            for code in valid_codes:
                description = DESCENT_CODE_MAP.get(code, f"Unknown Code ({code})") # Fallback for unmapped codes
                descent_list.append({'code': code, 'description': description})

            # Sort the final list by description for user-friendliness
            descent_list.sort(key=lambda item: item['description'])

            return descent_list
        except KeyError:
             logger.warning("Column 'Descent Code' not found for get_unique_descent_codes.")
             return []
        except Exception as e:
             logger.error(f"Error getting unique descent codes: {e}", exc_info=True)
             return [] # Return empty list on other errors

    def get_date_range(self):
        """Returns the minimum and maximum arrest dates."""
        if self.df.empty: return None, None
        try:
            min_date = self.df['Arrest Date'].min()
            max_date = self.df['Arrest Date'].max()
            # Convert pandas Timestamp to standard datetime if needed, though usually compatible
            return min_date, max_date
        except KeyError:
             logger.warning("Column 'Arrest Date' not found for get_date_range.")
             return None, None

    def get_unique_arrest_type_codes(self):
        """Returns a sorted list of unique arrest type codes with descriptions.
        Each item in the list is a dictionary: {'code': str, 'description': str}
        """
        if self.df.empty: return []
        try:
            unique_codes = self.df['Arrest Type Code'].dropna().unique().tolist()
            # Filter out non-strings and sort the codes
            valid_codes = sorted([c for c in unique_codes if isinstance(c, str)])

            # Create list of dictionaries with descriptions
            arrest_type_list = []
            for code in valid_codes:
                description = ARREST_TYPE_CODE_MAP.get(code, f"Unknown Code ({code})") # Fallback
                arrest_type_list.append({'code': code, 'description': description})

            # Sort the final list by description
            arrest_type_list.sort(key=lambda item: item['description'])
            return arrest_type_list
        except KeyError:
             logger.warning("Column 'Arrest Type Code' not found for get_unique_arrest_type_codes.")
             return []
        except Exception as e:
             logger.error(f"Error getting unique arrest type codes: {e}", exc_info=True)
             return []

    # --- Query Processing Methods ---

    def process_query1(self, params):
        """
        Query 1: Arrestaties per Gebied en Tijdsperiode
        params: {'area_name': str, 'start_date': str(ISO), 'end_date': str(ISO), 'min_age': int, 'max_age': int}
        """
        logger.info(f"Processing Query 1 with params: {params}")
        if self.df.empty: return {'status': 'error', 'message': 'Dataset not loaded'}

        try:
            area = params['area_name']
            start_date = pd.to_datetime(params['start_date'])
            end_date = pd.to_datetime(params['end_date'])
            min_age = params['min_age']
            max_age = params['max_age']

            # Filter data
            filtered_df = self.df[
                (self.df['Area Name'] == area) &
                (self.df['Arrest Date'] >= start_date) &
                (self.df['Arrest Date'] <= end_date) &
                (self.df['Age'] >= min_age) &
                (self.df['Age'] <= max_age)
            ]

            # Select relevant columns for output (adjust as needed)
            output_cols = ['Report ID', 'Arrest Date', 'Area Name', 'Charge Group Description', 'Age', 'Sex Code', 'Descent Code']
            result_df = filtered_df[output_cols].copy()
            result_df['Arrest Date'] = result_df['Arrest Date'].dt.strftime('%Y-%m-%d') # Format date for display

            # Prepare results
            data = result_df.to_dict(orient='records')
            headers = output_cols
            return {'status': 'OK', 'data': data, 'headers': headers, 'title': f'Arrests in {area}'}

        except KeyError as ke:
             logger.error(f"Query 1 failed - Missing column: {ke}")
             return {'status': 'error', 'message': f"Query failed: Server data missing expected column '{ke}'."}
        except Exception as e:
            logger.error(f"Error processing Query 1: {e}", exc_info=True)
            return {'status': 'error', 'message': f"Error processing query: {e}"}

    def process_query2(self, params):
        """
        Query 2: Trend van Specifieke Overtreding over Tijd
        params: {'charge_group': str, 'granularity': str ('daily', 'weekly', 'monthly', 'yearly')}
        """
        logger.info(f"Processing Query 2 with params: {params}")
        if self.df.empty: return {'status': 'error', 'message': 'Dataset not loaded'}

        try:
            charge_group = params['charge_group']
            granularity = params.get('granularity', 'monthly') # Default to monthly

            # Filter by charge group
            filtered_df = self.df[self.df['Charge Group Description'] == charge_group].copy()

            if filtered_df.empty:
                 return {'status': 'OK', 'data': [], 'headers': [], 'title': f'Trend for {charge_group} ({granularity.capitalize()}) (No Data)'}

            # Set index to Arrest Date for resampling
            filtered_df.set_index('Arrest Date', inplace=True)

            # Resample based on granularity
            resample_code = 'D' # Daily
            if granularity == 'weekly':
                resample_code = 'W-Mon' # Weekly, starting Monday
            elif granularity == 'monthly':
                resample_code = 'M' # Month End
            elif granularity == 'yearly':
                resample_code = 'A' # Year End

            trend_data = filtered_df.resample(resample_code).size().reset_index()
            trend_data.columns = ['Date', 'Arrest Count']

            # Format date column
            if granularity == 'daily':
                 trend_data['Date'] = trend_data['Date'].dt.strftime('%Y-%m-%d')
            elif granularity == 'weekly':
                 trend_data['Date'] = trend_data['Date'].dt.strftime('%Y-%W') # Year-WeekNumber
            elif granularity == 'monthly':
                 trend_data['Date'] = trend_data['Date'].dt.strftime('%Y-%m')
            elif granularity == 'yearly':
                 trend_data['Date'] = trend_data['Date'].dt.strftime('%Y')

            # Prepare results
            data = trend_data.to_dict(orient='records')
            headers = ['Date', 'Arrest Count']
            title = f'Trend for {charge_group} ({granularity.capitalize()})'

            return {'status': 'OK', 'data': data, 'headers': headers, 'title': title}

        except KeyError as ke:
             logger.error(f"Query 2 failed - Missing column: {ke}")
             return {'status': 'error', 'message': f"Query failed: Server data missing expected column '{ke}'."}
        except Exception as e:
            logger.error(f"Error processing Query 2: {e}", exc_info=True)
            return {'status': 'error', 'message': f"Error processing query: {e}"}


    def process_query3(self, params):
        """
        Query 3: Demografische Analyse van Arrestaties (Graph)
        params: {'sex_codes': list[str], 'descent_codes': list[str], 'charge_group': str | None, 'arrest_type_code': str | None, 'generate_plot': bool}
        """
        logger.info(f"Processing Query 3 with params: {params}")
        if self.df.empty: return {'status': 'error', 'message': 'Dataset not loaded'}

        try:
            sex_codes = params['sex_codes']
            descent_codes = params['descent_codes']
            charge_group = params.get('charge_group') # Optional
            arrest_type_code = params.get('arrest_type_code') # <-- ADDED: Get arrest_type_code

            # Filter data
            filtered_df = self.df[
                self.df['Sex Code'].isin(sex_codes) &
                self.df['Descent Code'].isin(descent_codes)
            ]

            if charge_group:
                filtered_df = filtered_df[filtered_df['Charge Group Description'] == charge_group]

            # --- ADDED: Filter by arrest_type_code if provided ---
            if arrest_type_code and 'Arrest Type Code' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['Arrest Type Code'] == arrest_type_code]
            # -----------------------------------------------------

            if filtered_df.empty:
                 descent_names = [DESCENT_CODE_MAP.get(dc, dc) for dc in descent_codes]
                 sex_names = ["Male" if sc == 'M' else "Female" if sc == 'F' else sc for sc in sex_codes]
                 title = f'Arrests by Descent ({", ".join(descent_names)}) and Sex ({", ".join(sex_names)}) - No Data'
                 if charge_group:
                      title += f' for {charge_group}'
                 # --- ADDED: Append arrest type to "No Data" title ---
                 if arrest_type_code:
                     arrest_type_desc = ARREST_TYPE_CODE_MAP.get(arrest_type_code, arrest_type_code)
                     title += f' (Type: {arrest_type_desc})'
                 # -------------------------------------------------
                 return {'status': 'OK', 'data': [], 'headers': [], 'plot': None, 'title': title}

            # --- NEW Plotting Logic --- 
            
            # 1. Calculate counts grouped by Descent and Sex
            summary_data = filtered_df.groupby(['Descent Code', 'Sex Code']).size().reset_index(name='Count')
            
            # 2. Map Descent Code to Description
            summary_data['Descent'] = summary_data['Descent Code'].map(DESCENT_CODE_MAP)
            # Handle any codes not in the map (though get_unique_descent_codes should have description)
            summary_data['Descent'] = summary_data['Descent'].fillna(summary_data['Descent Code'].apply(lambda x: f"Unknown ({x})"))
            
            # 3. Create the plot using object-oriented approach
            fig, ax = plt.subplots(figsize=(12, 7))
            
            plot_title = 'Arrests by Descent'
            if len(sex_codes) == 1:
                 sex_name = "Male" if sex_codes[0] == 'M' else "Female" if sex_codes[0] == 'F' else sex_codes[0]
                 plot_title += f' ({sex_name})'
                 # Plot single bars using the created axes object `ax`
                 # Assign x to hue and hide legend to satisfy future seaborn requirements
                 sns.barplot(data=summary_data, x='Descent', y='Count', hue='Descent', palette='viridis', ax=ax, legend=False)
            else: # Both sexes selected
                 plot_title += ' (Male vs Female)'
                 # Plot grouped bars using hue and the created axes object `ax`
                 sns.barplot(data=summary_data, x='Descent', y='Count', hue='Sex Code', palette='coolwarm', ax=ax)
                 ax.legend(title='Sex Code')
                 
            # Add charge group to title if specified
            if charge_group:
                 plot_title += f'\nCharge Group: {charge_group}'
            
            # --- ADDED: Append arrest type to plot_title ---
            if arrest_type_code:
                arrest_type_desc = ARREST_TYPE_CODE_MAP.get(arrest_type_code, arrest_type_code)
                plot_title += f'\nArrest Type: {arrest_type_desc}'
            # ----------------------------------------------
                 
            ax.set_title(plot_title)
            ax.set_xlabel('Descent')
            ax.set_ylabel('Number of Arrests')
            # Use ax.tick_params for label rotation
            ax.tick_params(axis='x', rotation=45, labelsize='medium') 
            # Set horizontal alignment manually if needed after rotation
            plt.setp(ax.get_xticklabels(), ha="right", rotation_mode="anchor")
            ax.grid(True, axis='y', linestyle='--', alpha=0.7)
            fig.tight_layout() # Call tight_layout on the figure object
            
            # --- End Plotting Logic ---

            # # Get the figure object << No longer needed, we have `fig`
            # fig = plt.gcf()

            # Prepare return data (return figure object, not bytes)
            data_for_table = summary_data.to_dict(orient='records')
            headers_for_table = ['Descent', 'Sex Code', 'Count']

            return {
                'status': 'OK',
                'data': data_for_table,
                'headers': headers_for_table,
                'plot': fig,                  # Return the figure object
                'title': plot_title
             }

        except KeyError as ke:
             logger.error(f"Query 3 failed - Missing column: {ke}")
             return {'status': 'error', 'message': f"Query failed: Server data missing expected column '{ke}'."}
        except Exception as e:
            logger.error(f"Error processing Query 3: {e}", exc_info=True)
            return {'status': 'error', 'message': f"Error processing query: {e}"}

    def _calculate_center(self, area_name):
        """Helper to calculate the central lat/lon for an area using median."""
        if 'Area Name' not in self.df.columns or 'LAT' not in self.df.columns or 'LON' not in self.df.columns:
            logger.warning(f"Cannot calculate center for '{area_name}': Missing required columns.")
            return None, None

        # --- Ensure coordinates are numeric and filter out invalid/zero coordinates ---
        # (Assuming 0,0 or similar might be placeholders for missing data)
        area_data = self.df[
            (self.df['Area Name'] == area_name) &
            pd.to_numeric(self.df['LAT'], errors='coerce').notna() &
            pd.to_numeric(self.df['LON'], errors='coerce').notna() &
            (pd.to_numeric(self.df['LAT'], errors='coerce') != 0) &
            (pd.to_numeric(self.df['LON'], errors='coerce') != 0)
        ].copy() # Create a copy to safely convert types

        if area_data.empty:
            logger.warning(f"No valid, non-zero coordinate data found for area '{area_name}'.")
            return None, None

        # Convert to numeric just for calculation (if not already)
        area_data['LAT'] = pd.to_numeric(area_data['LAT'])
        area_data['LON'] = pd.to_numeric(area_data['LON'])

        # --- Calculate MEDIAN latitude and longitude ---
        center_lat = area_data['LAT'].median()
        center_lon = area_data['LON'].median()
        # -------------------------------------------

        logger.info(f"Calculated MEDIAN center for '{area_name}': Lat={center_lat}, Lon={center_lon}")
        return center_lat, center_lon

    def _haversine(self, lat1, lon1, lat2, lon2):
        """Calculate the great-circle distance between two points on the earth."""
        # Convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        r = 6371 # Radius of Earth in kilometers
        return c * r

    def process_query4(self, params):
        """
        Query 4: Geografische Hotspots van Arrestaties
        params: {'center_lat': float, 'center_lon': float, 'radius_km': float, 'start_date': str(ISO), 'end_date': str(ISO), 'arrest_type_code': str | None}

        Returns a dict with 'data' (list of arrests), 'headers', 'title', and 'map_filepath' (absolute path to saved HTML map).
        """
        logger.info(f"Processing Query 4 with params: {params}")
        if self.df.empty: return {'status': 'error', 'message': 'Dataset not loaded'}
        if 'LAT' not in self.df.columns or 'LON' not in self.df.columns: return {'status': 'error', 'message': 'Latitude/Longitude data not available.'}

        try:
            center_lat = params['center_lat']
            center_lon = params['center_lon']
            radius_km = params['radius_km']
            start_date = pd.to_datetime(params['start_date'])
            end_date = pd.to_datetime(params['end_date']).replace(hour=23, minute=59, second=59)
            arrest_type_code = params.get('arrest_type_code')

            # --- Filter Data --- 
            df_working = self.df.copy() # Work with a copy

            # --- Extract LAT/LON, prioritizing parsed GeoJSON ---
            has_geojson_col = 'Location_GeoJSON_Parsed' in df_working.columns

            if has_geojson_col:
                logger.info("Query 4: Using 'Location_GeoJSON_Parsed' for coordinates.")
                df_working['extracted_LON'] = df_working['Location_GeoJSON_Parsed'].apply(
                    lambda x: x['geometry']['coordinates'][0] if isinstance(x, dict) and x.get('geometry') and x['geometry'].get('type') == 'Point' and len(x['geometry']['coordinates']) == 2 else None
                )
                df_working['extracted_LAT'] = df_working['Location_GeoJSON_Parsed'].apply(
                    lambda x: x['geometry']['coordinates'][1] if isinstance(x, dict) and x.get('geometry') and x['geometry'].get('type') == 'Point' and len(x['geometry']['coordinates']) == 2 else None
                )
            elif 'LON' in df_working.columns and 'LAT' in df_working.columns:
                logger.info("Query 4: Using existing 'LON' and 'LAT' columns.")
                df_working['extracted_LON'] = df_working['LON']
                df_working['extracted_LAT'] = df_working['LAT']
            else:
                logger.error("Query 4: Coordinate columns ('Location_GeoJSON_Parsed' or 'LON'/'LAT') not found.")
                return {'status': 'error', 'message': 'Coordinate data is missing or could not be processed.'}

            df_working['extracted_LAT'] = pd.to_numeric(df_working['extracted_LAT'], errors='coerce')
            df_working['extracted_LON'] = pd.to_numeric(df_working['extracted_LON'], errors='coerce')
            # --- End Coordinate Extraction ---


            # (Keep the efficient bounding box + precise radius filter)
            lat_degrees_delta = radius_km / 111.0
            lon_degrees_delta = radius_km / (111.0 * np.cos(np.radians(center_lat)))
            min_lat, max_lat = center_lat - lat_degrees_delta, center_lat + lat_degrees_delta
            min_lon, max_lon = center_lon - lon_degrees_delta, center_lon + lon_degrees_delta

            df_filtered = df_working[
                (df_working['Arrest Date'] >= start_date) &
                (df_working['Arrest Date'] <= end_date) &
                df_working['extracted_LAT'].notna() & df_working['extracted_LON'].notna() &
                (df_working['extracted_LAT'] >= min_lat) & (df_working['extracted_LAT'] <= max_lat) &
                (df_working['extracted_LON'] >= min_lon) & (df_working['extracted_LON'] <= max_lon)
            ]

            if arrest_type_code and 'Arrest Type Code' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['Arrest Type Code'] == arrest_type_code]
            
            map_filepath_abs = None # Initialize
            if not df_filtered.empty:
                # Use extracted_LAT and extracted_LON for Haversine and plotting
                distances = self._haversine(center_lat, center_lon, df_filtered['extracted_LAT'].values, df_filtered['extracted_LON'].values)
                df_filtered = df_filtered[distances <= radius_km].copy() # Copy results after final filter
                df_filtered['distance_km'] = distances[distances <= radius_km]
                logger.info(f"Query 4: Found {len(df_filtered)} points within radius.")

                # --- Generate Folium Map with Marker Clustering --- 
                if not df_filtered.empty:
                    try:
                        m = folium.Map(location=[center_lat, center_lon], zoom_start=13)

                        # Add Center Marker (directly to map)
                        folium.Marker(
                            [center_lat, center_lon],
                            popup=f"Center ({center_lat:.4f}, {center_lon:.4f})",
                            tooltip="Query Center",
                            icon=folium.Icon(color='red', icon='info-sign')
                        ).add_to(m)

                        # --- Create Marker Cluster --- 
                        marker_cluster = MarkerCluster().add_to(m)
                        # ----------------------------

                        df_to_plot = df_filtered # Plot all points

                        for idx, row in df_to_plot.iterrows():
                            popup_text = f"<b>Arrest:</b> {row.get('Report ID', 'N/A')}<br>"
                            popup_text += f"<b>Date:</b> {row.get('Arrest Date', pd.NaT).strftime('%Y-%m-%d') if pd.notna(row.get('Arrest Date')) else 'N/A'}<br>"
                            popup_text += f"<b>Address:</b> {row.get('Address', 'N/A')}<br>"
                            popup_text += f"<b>Charge:</b> {row.get('Charge Group Description', 'N/A')}<br>"
                            # Use extracted coordinates for popup as well
                            popup_text += f"<b>Coords:</b> ({row['extracted_LAT']:.4f}, {row['extracted_LON']:.4f})"
                            
                            # --- Add marker TO THE CLUSTER --- 
                            folium.CircleMarker(
                                location=[row['extracted_LAT'], row['extracted_LON']], # Use extracted coords
                                radius=5,
                                popup=folium.Popup(popup_text, max_width=300),
                                tooltip=f"Arrest {row.get('Report ID', '')}",
                                color='#3186cc',
                                fill=True,
                                fill_color='#3186cc',
                                fill_opacity=0.7
                            ).add_to(marker_cluster) # <<< Add to cluster, not map `m`
                            # --------------------------------

                        # --- Save map to a unique temporary file in SYSTEM temp dir ---
                        map_basename = f"map-{uuid.uuid4()}.html"
                        # Get the system temporary directory
                        system_temp_dir = tempfile.gettempdir()
                        # Get the absolute path within the system temp dir
                        map_filepath_abs = os.path.abspath(os.path.join(system_temp_dir, map_basename))
                        m.save(map_filepath_abs)
                        # -----------------------------------------------------------
                        
                        logger.info(f"Query 4: Folium map with {len(df_to_plot)} markers saved to {map_filepath_abs}.")

                    except Exception as map_err:
                        logger.error(f"Query 4: Failed to generate or save Folium map: {map_err}", exc_info=True)
                        map_filepath_abs = None 
            
            # --- Prepare Results --- 
            final_title = f'Arrests within {radius_km}km of ({center_lat:.4f}, {center_lon:.4f})'
            if arrest_type_code:
                 arrest_type_desc = ARREST_TYPE_CODE_MAP.get(arrest_type_code, arrest_type_code) # Get description
                 final_title += f' (Type: {arrest_type_desc})' # Use description
                 
            if df_filtered.empty:
                 # Return empty data but indicate no results in title
                 final_title += " (No Results)"
                 return {'status': 'OK', 'data': [], 'headers': [], 'map_filepath': None, 'title': final_title}

            # Prepare tabular data as before, using extracted_LAT/LON for output if original LAT/LON are now supplemental
            # Ensure 'LAT' and 'LON' in output_cols refer to the consistent, cleaned coordinates
            df_filtered.rename(columns={'extracted_LAT': 'LAT', 'extracted_LON': 'LON'}, inplace=True)

            output_cols = ['Report ID', 'Arrest Date', 'Area Name', 'Address', 'LAT', 'LON', 'Charge Group Description', 'Arrest Type Code', 'distance_km']
            output_cols = [col for col in output_cols if col in df_filtered.columns] # Keep only existing columns
            result_df = df_filtered[output_cols].sort_values(by='distance_km')
            if 'Arrest Date' in result_df.columns: result_df['Arrest Date'] = result_df['Arrest Date'].dt.strftime('%Y-%m-%d')
            if 'distance_km' in result_df.columns: result_df['distance_km'] = result_df['distance_km'].round(2)
            data = result_df.to_dict(orient='records')
            headers = output_cols

            # --- ADD LOGGING BEFORE RETURN ---
            logger.info(f"PROCESSOR_QUERY4: Returning map_filepath: {map_filepath_abs}")
            # ----------------------------------

            # Return data AND map FILEPATH
            return {'status': 'OK', 'data': data, 'headers': headers, 'map_filepath': map_filepath_abs, 'title': final_title}

        except KeyError as ke:
             logger.error(f"Query 4 failed - Missing parameter/column: {ke}")
             return {'status': 'error', 'message': f"Query failed: Missing expected parameter '{ke}' or data column."}
        except Exception as e:
            logger.error(f"Error processing Query 4: {e}", exc_info=True)
            return {'status': 'error', 'message': f"Error processing query: {e}"}

    # Add other methods if needed (e.g., for loading data, preprocessing) 