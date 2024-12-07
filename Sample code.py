# Sample code for visualization

# Brief description
# This sample code is a python code for generating an interactive choropleth map.
# This code draws an interactive choropleth map for U.S. state-level commercial emissions in each year
# Original data source: https://www.eia.gov/environment/emissions/state/

import pandas as pd
import plotly.graph_objects as go
import webbrowser
import os

file_path = r"~\state_level_emissions_commercial.dta"
df = pd.read_stata(file_path)

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
output_path = r"~\state_emissions_map.html"
fig.write_html(output_path)

# Open the file in a new Chrome tab
chrome_path = r"~\chrome.exe"  
webbrowser.register('chrome', None, webbrowser.BackgroundBrowser(chrome_path))
webbrowser.get('chrome').open_new_tab(f"file://{os.path.abspath(output_path)}")

