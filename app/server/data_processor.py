#!/usr/bin/env python3
# Data processor for server-side queries

# --- Force Matplotlib backend BEFORE importing pyplot or seaborn ---
import matplotlib
matplotlib.use('Agg') # Use the Agg backend for non-interactive plotting in threads
# -------------------------------------------------------------

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from matplotlib.figure import Figure
import logging
from datetime import datetime
import base64
import urllib.request
from math import log, tan, pi, cos, sinh, atan

# Set the style for visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette('viridis')

logger = logging.getLogger('data_processor')

# Import the mapping
from shared.constants import DESCENT_CODE_MAP

def add_osm_background(ax, bbox, zoom=13):
    """Add OpenStreetMap background to plot using matplotlib"""
    
    def deg2num(lat_deg, lon_deg, zoom):
        lat_rad = lat_deg * pi / 180.0
        n = 2.0 ** zoom
        xtile = int((lon_deg + 180.0) / 360.0 * n)
        ytile = int((1.0 - log(tan(lat_rad) + (1 / cos(lat_rad))) / pi) / 2.0 * n)
        return (xtile, ytile)
    
    xmin, ymin, xmax, ymax = bbox
    
    try:
        x1, y1 = deg2num(ymin, xmin, zoom)
        
        # Download the OSM tile with proper headers
        url = f"https://tile.openstreetmap.org/{zoom}/{x1}/{y1}.png"
        
        # Create a proper request with User-Agent header
        headers = {
            'User-Agent': 'ArrestDataViewer/1.0 (educational project; warresnaet@icloud.com)'
        }
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req) as response:
            img_data = response.read()
        
        img = plt.imread(io.BytesIO(img_data), format='png')
        ax.imshow(img, extent=[xmin, xmax, ymin, ymax], aspect='equal', alpha=0.7)
        logger.info("Successfully added OSM background map")
        return True
    except Exception as e:
        logger.error(f"Error loading map background: {e}")
        return False

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
                        pass
            
            # Ensure 'Arrest Date' is datetime
            self.df['Arrest Date'] = pd.to_datetime(self.df['Arrest Date'], errors='coerce')
            # Drop rows where Arrest Date couldn't be parsed if necessary
            self.df.dropna(subset=['Arrest Date'], inplace=True)
            
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
        """Returns a sorted list of unique arrest type codes."""
        if self.df.empty: return []
        try:
            # Use 'Arrest Type Code' column
            codes = self.df['Arrest Type Code'].dropna().unique().tolist()
            return sorted([c for c in codes if isinstance(c, str)]) # Filter out non-strings
        except KeyError:
             logger.warning("Column 'Arrest Type Code' not found for get_unique_arrest_type_codes.")
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
        params: {'sex_codes': list[str], 'descent_codes': list[str], 'charge_group': str | None, 'generate_plot': bool}
        """
        logger.info(f"Processing Query 3 with params: {params}")
        if self.df.empty: return {'status': 'error', 'message': 'Dataset not loaded'}

        try:
            sex_codes = params['sex_codes']
            descent_codes = params['descent_codes']
            charge_group = params.get('charge_group') # Optional

            # Filter data
            filtered_df = self.df[
                self.df['Sex Code'].isin(sex_codes) &
                self.df['Descent Code'].isin(descent_codes)
            ]

            if charge_group:
                filtered_df = filtered_df[filtered_df['Charge Group Description'] == charge_group]

            if filtered_df.empty:
                 # Use description in title if possible
                 descent_names = [DESCENT_CODE_MAP.get(dc, dc) for dc in descent_codes]
                 sex_names = ["Male" if sc == 'M' else "Female" if sc == 'F' else sc for sc in sex_codes]
                 title = f'Arrests by Descent ({", ".join(descent_names)}) and Sex ({", ".join(sex_names)}) - No Data'
                 if charge_group:
                      title += f' for {charge_group}'
                 return {'status': 'OK', 'data': [], 'headers': [], 'plot': None, 'title': title}

            # --- NEW Plotting Logic --- 
            
            # 1. Calculate counts grouped by Descent and Sex
            summary_data = filtered_df.groupby(['Descent Code', 'Sex Code']).size().reset_index(name='Count')
            
            # 2. Map Descent Code to Description
            summary_data['Descent'] = summary_data['Descent Code'].map(DESCENT_CODE_MAP)
            # Handle any codes not in the map (though get_unique_descent_codes should have description)
            summary_data['Descent'] = summary_data['Descent'].fillna(summary_data['Descent Code'].apply(lambda x: f"Unknown ({x})"))
            
            # 3. Create the plot
            plt.figure(figsize=(12, 7)) # Adjusted figure size
            
            plot_title = 'Arrests by Descent'
            if len(sex_codes) == 1:
                 sex_name = "Male" if sex_codes[0] == 'M' else "Female" if sex_codes[0] == 'F' else sex_codes[0]
                 plot_title += f' ({sex_name})'
                 # Plot single bars
                 ax = sns.barplot(data=summary_data, x='Descent', y='Count', palette='viridis')
            else: # Both sexes selected
                 plot_title += ' (Male vs Female)'
                 # Plot grouped bars using hue
                 ax = sns.barplot(data=summary_data, x='Descent', y='Count', hue='Sex Code', palette='coolwarm')
                 ax.legend(title='Sex Code')
                 
            # Add charge group to title if specified
            if charge_group:
                 plot_title += f'\nCharge Group: {charge_group}'
                 
            ax.set_title(plot_title)
            ax.set_xlabel('Descent')
            ax.set_ylabel('Number of Arrests')
            plt.xticks(rotation=45, ha='right') # Rotate labels for better readability
            ax.grid(True, axis='y', linestyle='--', alpha=0.7)
            plt.tight_layout() 
            
            # --- End NEW Plotting Logic ---

            # Save plot to buffer
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150) # Increased DPI slightly
            plt.close() # Close the figure
            buf.seek(0)
            plot_bytes = buf.read()
            buf.close()

            # Prepare return data (now includes summary data and plot)
            data_for_table = summary_data.to_dict(orient='records')
            headers_for_table = ['Descent', 'Sex Code', 'Count'] # Headers for optional table view

            return {
                'status': 'OK',
                'data': data_for_table,        # Return summary data for potential table display
                'headers': headers_for_table,  
                'plot': plot_bytes,           # Raw bytes of the plot PNG
                'title': plot_title           # Use the generated plot title
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

        Returns a dict with 'data' (list of arrests), 'headers', 'title', and potentially 'plot' (heatmap bytes).
        """
        logger.info(f"Processing Query 4 with params: {params}")
        if self.df.empty:
            return {'status': 'error', 'message': 'Dataset not loaded'}
        if 'LAT' not in self.df.columns or 'LON' not in self.df.columns:
            return {'status': 'error', 'message': 'Latitude/Longitude data not available in dataset.'}

        try:
            center_lat = params['center_lat']
            center_lon = params['center_lon']
            radius_km = params['radius_km']
            start_date = pd.to_datetime(params['start_date'])
            end_date = pd.to_datetime(params['end_date']).replace(hour=23, minute=59, second=59)
            arrest_type_code = params.get('arrest_type_code')

            # --- Bounding Box Calculation ---
            # Rough conversion: 1 degree latitude ~= 111 km
            # 1 degree longitude ~= 111 km * cos(latitude)
            lat_degrees_delta = radius_km / 111.0
            lon_degrees_delta = radius_km / (111.0 * np.cos(np.radians(center_lat)))

            min_lat = center_lat - lat_degrees_delta
            max_lat = center_lat + lat_degrees_delta
            min_lon = center_lon - lon_degrees_delta
            max_lon = center_lon + lon_degrees_delta
            logger.info(f"Query 4: Bounding Box - LAT ({min_lat:.4f} to {max_lat:.4f}), LON ({min_lon:.4f} to {max_lon:.4f})")
            # --- End Bounding Box Calculation ---

            # --- Filter Data (Date, Type, Coords, *Bounding Box*) ---
            # Ensure LAT/LON are numeric *before* bounding box filter for efficiency
            df_filtered = self.df.copy() # Start with a copy
            df_filtered['LAT'] = pd.to_numeric(df_filtered['LAT'], errors='coerce')
            df_filtered['LON'] = pd.to_numeric(df_filtered['LON'], errors='coerce')

            df_filtered = df_filtered[
                (df_filtered['Arrest Date'] >= start_date) &
                (df_filtered['Arrest Date'] <= end_date) &
                df_filtered['LAT'].notna() & # Check after numeric conversion
                df_filtered['LON'].notna() & # Check after numeric conversion
                # Add bounding box filter here
                (df_filtered['LAT'] >= min_lat) &
                (df_filtered['LAT'] <= max_lat) &
                (df_filtered['LON'] >= min_lon) &
                (df_filtered['LON'] <= max_lon)
            ]
            logger.info(f"Query 4: After date and bounding box filter, size: {len(df_filtered)}")

            # Optional: Filter by Arrest Type Code (apply *after* bounding box)
            if arrest_type_code and 'Arrest Type Code' in df_filtered.columns:
                logger.info(f"Query 4: Filtering by Arrest Type Code = '{arrest_type_code}'. Available codes in current data: {df_filtered['Arrest Type Code'].unique()}")
                df_filtered = df_filtered[df_filtered['Arrest Type Code'] == arrest_type_code]
                logger.info(f"Query 4: After arrest type filter ('{arrest_type_code}'), size: {len(df_filtered)}")
            elif arrest_type_code:
                 logger.warning(f"Query 4: Arrest type code '{arrest_type_code}' provided, but 'Arrest Type Code' column not found in filtered data.")

            # --- Calculate *precise* distance and filter by radius (only on bounding box results) ---
            plot_bytes = None
            if not df_filtered.empty:
                 # LAT/LON are already numeric and non-null from previous steps
                 distances = self._haversine(center_lat, center_lon, df_filtered['LAT'].values, df_filtered['LON'].values)
                 df_filtered['distance_km'] = distances
                 # Apply final radius filter
                 df_filtered = df_filtered[df_filtered['distance_km'] <= radius_km]
                 logger.info(f"Query 4: After precise radius ({radius_km}km) filter, size: {len(df_filtered)}")

                 # --- Generate Heatmap if results exist ---
                 if not df_filtered.empty:
                     try:
                         logger.info(f"Query 4: Generating heatmap for {len(df_filtered)} points.")
                         
                         # --- CREATE PLOT WITH MAP BACKGROUND ---
                         fig = plt.figure(figsize=(10, 10))
                         ax = fig.add_subplot(111)
                         
                         # Create the KDE plot with some transparency
                         sns.kdeplot(
                             data=df_filtered, x='LON', y='LAT',
                             fill=True, thresh=0.05, levels=10, 
                             cmap="viridis", alpha=0.7, # Add transparency
                             ax=ax
                         )
                         
                         # Add center point
                         ax.scatter(center_lon, center_lat, color='red', s=80, marker='x', 
                                   linewidth=2, label='Center')
                         
                         # Set bounds slightly larger than data points
                         buffer = max(0.005, radius_km/100)  # At least 0.005 degrees buffer
                         ax.set_xlim(center_lon - lon_degrees_delta - buffer, 
                                    center_lon + lon_degrees_delta + buffer)
                         ax.set_ylim(center_lat - lat_degrees_delta - buffer, 
                                    center_lat + lat_degrees_delta + buffer)
                         
                         # Set aspect ratio to equal for correct geography
                         ax.set_aspect('equal')
                         
                         # Add the base map from OpenStreetMap
                         try:
                             add_osm_background(ax, [center_lon-lon_degrees_delta-buffer, 
                                                    center_lat-lat_degrees_delta-buffer,
                                                    center_lon+lon_degrees_delta+buffer, 
                                                    center_lat+lat_degrees_delta+buffer])
                         except Exception as map_err:
                             logger.warning(f"Failed to add map basemap: {map_err}. Falling back to basic plot.")
                         
                         # Add minimal styling
                         ax.set_xlabel('Longitude')
                         ax.set_ylabel('Latitude')
                         ax.legend(loc='upper right')
                         
                         # --- Save with small padding ---
                         buf = io.BytesIO()
                         fig.savefig(buf, format='png', dpi=200, 
                                    bbox_inches='tight', pad_inches=0.1)
                         # ---------------------------------
                         
                         plt.close(fig)
                         buf.seek(0)
                         plot_bytes = buf.read()
                         buf.close()
                         
                         logger.info("Query 4: Heatmap with map background generated.")
                     except Exception as plot_err:
                         logger.error(f"Query 4: Failed to generate heatmap: {plot_err}", exc_info=True)
                         plot_bytes = None
            else:
                 logger.info("Query 4: DataFrame empty after bounding box/type filter.")

            # --- Prepare Results ---
            if df_filtered.empty:
                return {'status': 'OK', 'data': [], 'headers': [], 'plot': None, 'title': f'Arrests within {radius_km}km of ({center_lat:.4f}, {center_lon:.4f}) (No Results)'}

            output_cols = ['Report ID', 'Arrest Date', 'Area Name', 'Address', 'LAT', 'LON', 'Charge Group Description', 'Arrest Type Code', 'distance_km']
            output_cols = [col for col in output_cols if col in df_filtered.columns]
            result_df = df_filtered[output_cols].sort_values(by='distance_km')

            if 'Arrest Date' in result_df.columns:
                 result_df['Arrest Date'] = result_df['Arrest Date'].dt.strftime('%Y-%m-%d')
            if 'distance_km' in result_df.columns:
                 result_df['distance_km'] = result_df['distance_km'].round(2)

            data = result_df.to_dict(orient='records')
            headers = output_cols
            logger.info(f"Query 4: Found {len(data)} results near ({center_lat:.4f}, {center_lon:.4f}). Preparing to send...")

            title = f'Arrests within {radius_km}km of ({center_lat:.4f}, {center_lon:.4f})'
            if arrest_type_code:
                 title += f' (Type: {arrest_type_code})'

            # --- Add plot_bytes to the return dictionary ---
            return {'status': 'OK', 'data': data, 'headers': headers, 'plot': plot_bytes, 'title': title}
            # ---------------------------------------------

        except KeyError as ke:
            logger.error(f"Query 4 failed - Missing parameter/column: {ke}")
            return {'status': 'error', 'message': f"Query failed: Missing expected parameter '{ke}' or data column."}
        except Exception as e:
            logger.error(f"Error processing Query 4: {e}", exc_info=True)
            return {'status': 'error', 'message': f"Error processing query: {e}"}

    # Add other methods if needed (e.g., for loading data, preprocessing) 