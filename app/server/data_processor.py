#!/usr/bin/env python3
# Data processor for server-side queries

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

# Set the style for visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette('viridis')


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
        
        # Set default parameters if not provided
        if parameters is None:
            parameters = {}
        
        try:
            # Choose the appropriate query handler
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
        # Create table result
        age_counts = self.df['Age'].value_counts().sort_index().reset_index()
        age_counts.columns = ['Age', 'Count']
        
        # Create figure
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
        
        # Create table result
        top_charges = self.df['Charge Group Description'].value_counts().head(n).reset_index()
        top_charges.columns = ['Charge Group', 'Count']
        
        # Create figure
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
        
        # Create table result
        area_counts = self.df['Area Name'].value_counts().head(n).reset_index()
        area_counts.columns = ['Area', 'Count']
        
        # Create figure
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
        # Check if the hour column exists
        if 'Arrest Hour' not in self.df.columns:
            return {'status': 'error', 'message': 'Arrest Hour column not found in dataset'}
        
        # Create table result
        hour_counts = self.df['Arrest Hour'].value_counts().sort_index().reset_index()
        hour_counts.columns = ['Hour', 'Count']
        
        # Create figure
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
        
        # Create figure
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
        
        # Get top areas and charges
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
        
        # Create figure
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
            
            # Create figure - age distribution for selected gender
            fig, ax = plt.subplots(figsize=(10, 8))
            sns.barplot(x='Age Group', y='Count', data=age_counts, ax=ax)
            ax.set_title(f'Age Distribution for {selected_gender_name} Arrests')
            ax.set_xlabel('Age Group')
            ax.set_ylabel('Number of Arrests')
            ax.grid(True, axis='y')
            
            # Create figure 2 - histogram of exact ages
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