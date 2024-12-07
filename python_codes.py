# Last update: Oct 29, 2024

# Brief description
# This Python script prepares a dataset containing information on the AI workforce at the firm-year level.

# This script is composed of three sub-parts: 
## (Part 1) Identify individuals having AI-related skills using Revelio Lab's data.
## (Part 2) Refine the existing dataset using data from Part 1.
## (Part 3) Generate firm-year level variables using the refined dataset from Part 2.
## (Part 4) Export a .csv file where each row represents a unique firm-year record.

#####################################################################################
#####################################################################################
## (Part 1) Identify individuals having AI-related skills using Revelio Lab's data.
## - A list of AI-related skills are stored in AI_related_skills.xlsx
## - This list is created using Alekseeva L, Azar J, Giné M, Samila S, Taska B. 2021. The demand for AI skills in the labor market. Labour Economics
#####################################################################################
#####################################################################################

import pandas as pd
import os
import gc  # Garbage collector
from tqdm import tqdm

# Paths
excel_path = r"~\AI_related_skills.xlsx"
input_directory = r"~\academic_individual_user_skill"
interim_output_path = r"~\interim"
final_output_path = r"~\final_combined_skills.csv"

# Load the Excel file to extract the list of AI-related skills
ai_skills = pd.read_excel(excel_path)
skills_list = ai_skills['skill'].str.strip().str.lower().tolist()

# Check if the correct number of skills (81) is identified
assert len(skills_list) == 81, f"Expected 81 skills, but found {len(skills_list)}"

# Ensure the interim and final output directories exist
os.makedirs(interim_output_path, exist_ok=True)
os.makedirs(os.path.dirname(final_output_path), exist_ok=True)

# Create an empty list to store interim DataFrames
all_filtered_data = []

# Loop over the expected range of file numbers (0000 to 0405)
for i in tqdm(range(406), desc="Processing Files", ncols=100):
    filename = f"individual_user_skill_{i:04d}_part_00.parquet"
    input_file = os.path.join(input_directory, filename)

    # Check if the file exists before processing
    if not os.path.exists(input_file):
        print(f"File not found: {filename}, skipping...")
        continue

    # Load the parquet file
    df = pd.read_parquet(input_file)

    # Count the total number of individuals in the file
    total_individuals = df['user_id'].nunique()

    # Ensure 'skill_raw' and 'skill_mapped' are lowercase for matching
    df['skill_raw'] = df['skill_raw'].str.strip().str.lower()
    df['skill_mapped'] = df['skill_mapped'].str.strip().str.lower()

    # Filter individuals with skills matching the AI-related skills list
    filtered_df = df[
        df['skill_raw'].isin(skills_list) | df['skill_mapped'].isin(skills_list)
    ]

    # Count the number of filtered individuals
    filtered_individuals = filtered_df['user_id'].nunique()

    # Save the interim result as a CSV file
    interim_file = os.path.join(interim_output_path, f"interim_{filename.replace('.parquet', '.csv')}")
    filtered_df.to_csv(interim_file, index=False)

    # Append the filtered DataFrame to the list
    all_filtered_data.append(filtered_df)

    # Print progress with counts
    print(f"[{i + 1}/406] Processed: {filename} -> "
          f"Total Individuals: {total_individuals}, Filtered Individuals: {filtered_individuals}")

    # Clear variables and free memory
    del df, filtered_df  # Delete DataFrames
    gc.collect()  # Run garbage collection

# Combine all filtered DataFrames into a single DataFrame
if all_filtered_data:
    final_combined_df = pd.concat(all_filtered_data, ignore_index=True)
    final_combined_df.to_csv(final_output_path, index=False)
    print(f"Final combined file saved at: {final_output_path}")
else:
    print("No matching data found across all files.")

print("All files have been processed and combined.")



#####################################################################################
#####################################################################################
## (Part 2) Refine the existing dataset using data from Part 1
## - To combined files (files with firms of interest), attach id_parat (by rcid) and tag (equals 1 if the individual's role in the firm is related to AI, as determined by the ETO Report). 
## - Also, attach information on AI-skilled individuals (by user_id).
## - Then, all interim files (i.e., interim_combined_0###_0###.csv) have information on user_id, whether the user_id's role is AI-related based on ETO report (tag), 
##   and whether the user_id has AI-related skills (labor_with_AI_skill).
## - Finally, using 'startdate' and 'enddate', generate panel data (unique in company user_id year)
## - Remove observations whose startdate is missing
## - For observations whose enddate is missing, fill the missing value as 2024, assuming that the user_id is still working in the firm. 
#####################################################################################
#####################################################################################

import pandas as pd
import numpy as np
import gc  # For garbage collection
import os

# Paths to input and output directories
input_directory = r"~\combined"
interim_output_directory = r"~\interim"
user_skill_file = r"~\final_combined_skills.csv"  

# Ensure the output directory exists
os.makedirs(interim_output_directory, exist_ok=True)

# Step 1: Load the user skills data 
ai_skilled_users = pd.read_csv(user_skill_file)['user_id'].unique()

# Helper function to generate filenames dynamically
def generate_filenames():
    for i in range(1, 1000, 50):  # Step by 50 for file ranges (0001 to 0999)
        start = f"{i:04d}"
        end = f"{min(i + 49, 999):04d}"
        yield f"combined_{start}_{end}.csv"

# Step 2: Process each file one by one and save interim results
for filename in generate_filenames():
    input_file = os.path.join(input_directory, filename)

    if not os.path.exists(input_file):
        print(f"File not found: {filename}, skipping...")
        continue

    # Show the name of the file being processed
    print(f"Processing file: {filename}")

    # Load the current CSV file
    df1 = pd.read_csv(input_file)

    # Remove rows with missing 'startdate'
    df1 = df1.dropna(subset=['startdate'])

    # Extract 'start_year' and 'end_year'
    df1['start_year'] = df1['startdate'].astype(str).str[:4]
    df1['end_year'] = df1['enddate'].astype(str).str[:4].fillna('2024').replace('nan', '2024') 
    df1['start_year'] = df1['start_year'].astype(int)
    df1['end_year'] = df1['end_year'].astype(int)

    df2 = pd.read_stata(r"~\[Revelio - Parat] rcid - id_parat.dta")        # Dataset that matched firms in Revelio labs' data (rcid) and ETO parat data (id_parat)
    df1 = pd.merge(df1, df2[['rcid', 'id_parat']], on='rcid', how='left') 

    df3 = pd.read_stata(r"~\[Position] Technical Team roles.dta")          # Dataset that tagged AI-related roles based on CSET report (https://cset.georgetown.edu/publication/the-race-for-us-technical-talent/)
    df1 = pd.merge(df1, df3[['role_k1000', 'tag']], on='role_k1000', how='left')

    # Swap start and end years if start_year > end_year
    df1['start_year'], df1['end_year'] = np.where(
        df1['start_year'] > df1['end_year'],
        [df1['end_year'], df1['start_year']],
        [df1['start_year'], df1['end_year']]
    )

    # Expand rows by year range
    df1_expanded = pd.DataFrame({
        col: np.repeat(df1[col].values, df1['end_year'] - df1['start_year'] + 1)
        for col in df1.columns
    })
    df1_expanded['year'] = [
        year for start, end in zip(df1['start_year'], df1['end_year'])
        for year in range(start, end + 1)
    ]

    # Tag observations with AI skills
    df1_expanded['labor_with_AI_skill'] = df1_expanded['user_id'].isin(ai_skilled_users).astype(int)

    # Save the interim result
    interim_file = os.path.join(interim_output_directory, f"interim_{filename}")
    df1_expanded.to_csv(interim_file, index=False)
    print(f"Processed and saved: {interim_file}")

    # Perform garbage collection
    del df1, df1_expanded, df2, df3
    gc.collect()

print("All files have been processed and saved as interim files.")



#####################################################################################
#####################################################################################
## (Part 3) Generate firm-year level variables using the refined dataset from Part 2.
#####################################################################################
#####################################################################################

import pandas as pd
import numpy as np
import os
import gc 

# Paths to directories
interim_input_directory = r"~\interim"
output_directory = r"~\interim"

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

# Helper function to generate filenames dynamically
def generate_interim_filenames():
    for i in range(1, 1000, 50):  # Step by 50 for file ranges (0001 to 0999)
        start = f"{i:04d}"
        end = f"{min(i + 49, 999):04d}"
        yield f"interim_combined_{start}_{end}.csv"

# Function to validate 'labor_with_AI_skill' values
def validate_labor_with_AI_skill(df):
    if not df['labor_with_AI_skill'].isin([0, 1]).all():
        raise ValueError("labor_with_AI_skill values are not 1 or 0")

# Step 1: Process each interim file
for filename in generate_interim_filenames():
    input_file = os.path.join(interim_input_directory, filename)
    output_file = os.path.join(output_directory, f"interim_filtered_{filename}")

    if not os.path.exists(input_file):
        print(f"File not found: {filename}, skipping...")
        continue

    # Load the interim file
    print(f"Processing file: {filename}")
    df = pd.read_csv(input_file)

    # Step 2: Validate 'labor_with_AI_skill'
    try:
        validate_labor_with_AI_skill(df)
    except ValueError as e:
        print(e)
        break  # Stop processing if validation fails

    # Step 3: Create the eight new variables
    df['rcid_total'] = df.groupby(['rcid', 'year'])['user_id'].transform('nunique')
    df['rcid_tag'] = df[df['tag'] == 1].groupby(['rcid', 'year'])['user_id'].transform('nunique').fillna(0)

    df['rcid_AI_skill'] = df[df['labor_with_AI_skill'] == 1].groupby(['rcid', 'year'])['user_id'].transform('nunique').fillna(0)
    df['rcid_tag_AI_skill'] = df[(df['labor_with_AI_skill'] == 1) & (df['tag'] == 1)].groupby(['rcid', 'year'])['user_id'].transform('nunique').fillna(0)

    df['id_parat_total'] = df.groupby(['id_parat', 'year'])['user_id'].transform('nunique')
    df['id_parat_tag'] = df[df['tag'] == 1].groupby(['id_parat', 'year'])['user_id'].transform('nunique').fillna(0)

    df['id_parat_AI_skill'] = df[df['labor_with_AI_skill'] == 1].groupby(['id_parat', 'year'])['user_id'].transform('nunique').fillna(0)
    df['id_parat_tag_AI_skill'] = df[(df['labor_with_AI_skill'] == 1) & (df['tag'] == 1)].groupby(['id_parat', 'year'])['user_id'].transform('nunique').fillna(0)

    # Step 4: Save the result as an interim filtered file
    df.to_csv(output_file, index=False)
    print(f"Processed and saved: {output_file}")

    # Step 5: Perform garbage collection
    del df
    gc.collect()

print("All files have been processed and saved as interim filtered files.")


##############################################################################################
##############################################################################################
## (Part 4) Export a .csv file where each row represents a unique firm-year record.
##############################################################################################
##############################################################################################

import pandas as pd
import os
import gc  # For garbage collection

# Paths to input and output directories
input_directory = r"H:\Revelio\academic_individual_position\stata\interim\interim_filtered"
output_directory = r"H:\Revelio\academic_individual_position\stata\interim\interim_filtered"
final_output_path = r"H:\Revelio\academic_individual_position\stata\interim\interim_filtered\[FIRM YEAR LEVEL] FINAL.csv"

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

# Columns to keep
columns_to_keep = [
    'id_parat', 'rcid', 'year', 'rcid_total', 'rcid_tag', 
    'id_parat_total', 'id_parat_tag', 'rcid_AI_skill', 
    'rcid_tag_AI_skill', 'id_parat_AI_skill', 'id_parat_tag_AI_skill'
]

# Helper function to generate filenames dynamically
def generate_interim_filenames():
    for filename in os.listdir(input_directory):
        if filename.startswith("interim_filtered") and filename.endswith(".csv"):
            yield filename

# List to store dataframes for final concatenation
dataframes = []

# Step 1: Process each file, keep selected columns, and save as new files
for filename in generate_interim_filenames():
    input_file = os.path.join(input_directory, filename)
    output_file = os.path.join(output_directory, f"[FIRM YEAR LEVEL] {filename}")

    # Indicate which file is being processed
    print(f"Processing file: {filename}")

    # Load the current CSV file
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error processing {filename}: {e}")
        continue  # Skip to the next file if an error occurs

    # Keep only the selected columns and remove duplicates
    df_filtered = df[columns_to_keep].drop_duplicates()

    # Save the filtered result
    df_filtered.to_csv(output_file, index=False)
    print(f"Processed and saved: {output_file}")

    # Store the dataframe for final concatenation
    dataframes.append(df_filtered)

    # Perform garbage collection
    del df, df_filtered
    gc.collect()

# Step 2: Concatenate all filtered dataframes and save the final file
if dataframes:
    print("Concatenating all filtered files...")
    final_df = pd.concat(dataframes, ignore_index=True)
    final_df.to_csv(final_output_path, index=False)
    print(f"Final file saved: {final_output_path}")
else:
    print("No files to process.")

print("All files have been processed and saved.")



