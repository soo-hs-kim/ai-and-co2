# ========================================================
# Python Script: Interactive Choropleth Map for U.S. Emissions
# ========================================================
# Purpose:
# This script generates an interactive choropleth map visualizing 
# U.S. state-level commercial sector emissions from 1970–2022.
#
# Data Source: 
# "Commercial energy-related carbon dioxide emissions by state" dataset 
# from https://www.eia.gov/environment/emissions/state/
#
# Key Features:
# - Dynamic visualization with year-by-year data using a slider.
# - Uses Plotly for interactive mapping and visualization.
# - Outputs the map to an HTML file for browser viewing.
#
# Libraries Used: pandas, plotly, os, webbrowser, requests
# --------------------------------------------------------

# ==========================
# 1. Import Libraries
# ==========================
import pandas as pd
import plotly.graph_objects as go
import webbrowser
import os

# ==========================
# 2. Download and Load Data
# ==========================
# Define file location and GitHub repository
github_url = "https://raw.githubusercontent.com/soo-hs-kim/ai-and-co2"
file_name = "eia_emissions_commercial.xlsx"

# Check if the file exists locally, otherwise download it
if not os.path.exists(file_name):
    response = requests.get(github_url)
    with open(file_name, "wb") as file:
        file.write(response.content)

# Load data into a pandas DataFrame
df = pd.read_excel(file_name)

# ==========================
# 3. Data Preparation
# ==========================
# Map full state names to state abbreviations
state_abbreviation_mapping = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "District of Columbia": "DC",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA",
    "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI",
    "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
    "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND",
    "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY"
}

df['state'] = df['state'].map(state_abbreviation_mapping)

# Filter data for the desired year range
df = df[df['year'].between(1970, 2022)]  
df = df.dropna(subset=['state', 'emissions'])  # Remove rows with missing values

# ==========================
# 4. Visualization Setup
# ==========================
fig = go.Figure()

# Extract unique years for slider functionality
years = sorted(df['year'].unique())

# Add a choropleth trace for each year
for year in years:
    year_data = df[df['year'] == year]
    fig.add_trace(
        go.Choropleth(
            locations=year_data['state'],  
            z=year_data['emissions'],  
            locationmode="USA-states",  
            colorscale="Reds",  
            zmin=df['emissions'].min(),
            zmax=df['emissions'].max(),
            colorbar_title="Emissions",
            hoverinfo="location+z",
            visible=(year == years[0])  # Only the first year is visible initially
        )
    )

# ==========================
# 5. Create Slider for Years
# ==========================
steps = []
for i, year in enumerate(years):
    step = dict(
        method="update",
        args=[
            {"visible": [False] * len(fig.data)},  # Hide all traces
            {"title": f"State-Level Emissions in the U.S. - Year {year}"}
        ],
    )
    step["args"][0]["visible"][i] = True  # Show trace for the current year
    step["label"] = str(year)  
    steps.append(step)

sliders = [dict(
    active=0,
    currentvalue={"prefix": "Year: "},
    pad={"t": 50},
    steps=steps
)]

# ==========================
# 6. Layout Configuration
# ==========================
fig.update_layout(
    sliders=sliders,
    title="Interactive State-Level Emissions in the U.S. (1970–2022)",
    geo=dict(
        scope="usa",
        projection_type="albers usa",
        showframe=False,
        showcoastlines=True
    )
)

# ==========================
# 7. Save and Open the Map
# ==========================
output_path = r"state_emissions_map.html"
fig.write_html(output_path)

print(f"Visualization has been saved to {output_path}. Open this file in a browser to view the interactive map.")

