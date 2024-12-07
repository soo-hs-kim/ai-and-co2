# Sample code for visualization

# Brief description
# This sample code is a Python script for generating an interactive choropleth map.
# This code visualizes U.S. state-level commercial emissions for each year.

# Original data source: https://www.eia.gov/environment/emissions/state/
# The excel file under "Commercial energy-related carbon dioxide emissions by state" was downloaded and saved as 'eia_emissions_commercial.xlsx' in the github repository

import pandas as pd
import plotly.graph_objects as go
import webbrowser
import os

# Download the data file from GitHub repository
github_url = "https://raw.githubusercontent.com/soo-hs-kim/ai-and-co2"
file_name = "eia_emissions_commercial.xlsx"

if not os.path.exists(file_name):
    response = requests.get(github_url)
    with open(file_name, "wb") as file:
        file.write(response.content)

# Load the data
df = pd.read_excel(file_name)

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

df = df[df['year'].between(1970, 2022)]  # Ensure the year range (1970–2022)
df = df.dropna(subset=['state', 'emissions'])  # Drop rows with missing state or emissions

fig = go.Figure()

years = sorted(df['year'].unique())

# Add a trace for each year
for year in years:
    year_data = df[df['year'] == year]
    fig.add_trace(
        go.Choropleth(
            locations=year_data['state'],  # State abbreviations
            z=year_data['emissions'],  # Emissions values
            locationmode="USA-states",  # Map U.S. states
            colorscale="Reds",  # Use Reds color scale
            zmin=df['emissions'].min(),
            zmax=df['emissions'].max(),
            colorbar_title="Emissions",
            hoverinfo="location+z",
            visible=(year == years[0])  # Show only the first year's data initially
        )
    )

# Create slider steps for each year
steps = []
for i, year in enumerate(years):
    step = dict(
        method="update",
        args=[
            {"visible": [False] * len(fig.data)},  # Hide all traces
            {"title": f"State-Level Emissions in the U.S. - Year {year}"}
        ],
    )
    step["args"][0]["visible"][i] = True  # Show only the trace for the current year
    step["label"] = str(year)  # Label the step with the year
    steps.append(step)

# Add the slider to the layout
sliders = [dict(
    active=0,
    currentvalue={"prefix": "Year: "},
    pad={"t": 50},
    steps=steps
)]

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

#Save the figure as an HTML file and open it in a new Chrome tab
output_path = r"state_emissions_map.html"
fig.write_html(output_path)

print(f"Visualization has been saved to {output_path}. Open this file in a browser to view the interactive map.")
