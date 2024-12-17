# ========================================================
# Python Script: AI Workforce Dataset Preparation
# ========================================================
# Purpose:
# Prepare a dataset containing AI workforce information at the firm-year level.
# 
# This script has 4 parts:
# (1) Identify individuals with AI-related skills using Revelio Lab's data.
# (2) Refine the dataset using Part 1 results.
# (3) Generate firm-year level variables.
# (4) Export a final .csv file where each row represents a unique firm-year record.
#
# Last update: Oct 29, 2024
# Data Source: Revelio Labs and Alekseeva et al. (2021) AI Skills List
# ========================================================


# ========================================================
# (Part 1) Identify Individuals with AI-Related Skills
# ========================================================
# - AI-related skills list loaded from 'AI_related_skills.xlsx'
# - Filter individuals with AI skills from Revelio Lab data files.
# - Output: Interim filtered files saved as .csv
# --------------------------------------------------------
import pandas as pd
import os
import gc  # Garbage collector
from tqdm import tqdm

# Paths and Setup
excel_path = r"~\AI_related_skills.xlsx"
input_directory = r"~\academic_individual_user_skill"
interim_output_path = r"~\interim"
final_output_path = r"~\final_combined_skills.csv"

# Load the AI-related skills list
ai_skills = pd.read_excel(excel_path)
skills_list = ai_skills['skill'].str.strip().str.lower().tolist()

# Check if the correct number of skills is identified
assert len(skills_list) == 81, f"Expected 81 skills, but found {len(skills_list)}"

# Ensure the interim and final output directories exist
os.makedirs(interim_output_path, exist_ok=True)
os.makedirs(os.path.dirname(final_output_path), exist_ok=True)

# Process each Revelio Lab file
all_filtered_data = []
for i in tqdm(range(406), desc="Processing Files", ncols=100):
    filename = f"individual_user_skill_{i:04d}_part_00.parquet"
    input_file = os.path.join(input_directory, filename)

    if not os.path.exists(input_file):
        print(f"File not found: {filename}, skipping...")
        continue

    # Load and filter the file
    df = pd.read_parquet(input_file)
    df['skill_raw'] = df['skill_raw'].str.strip().str.lower()
    df['skill_mapped'] = df['skill_mapped'].str.strip().str.lower()
    filtered_df = df[df['skill_raw'].isin(skills_list) | df['skill_mapped'].isin(skills_list)]

    # Save interim results
    interim_file = os.path.join(interim_output_path, f"interim_{filename.replace('.parquet', '.csv')}")
    filtered_df.to_csv(interim_file, index=False)
    all_filtered_data.append(filtered_df)

    del df, filtered_df
    gc.collect()

# Combine and save final results
if all_filtered_data:
    final_combined_df = pd.concat(all_filtered_data, ignore_index=True)
    final_combined_df.to_csv(final_output_path, index=False)
    print(f"Final combined file saved at: {final_output_path}")
else:
    print("No matching data found.")

print("Part 1 completed successfully.")


# ========================================================
# (Part 2) Refine the Existing Dataset
# ========================================================
# - Add AI skill tags, user roles, and firm-level IDs.
# - Expand data into panel format using 'startdate' and 'enddate'.
# - Output: Refined interim files saved in CSV format.
# --------------------------------------------------------
import pandas as pd
import numpy as np
import gc
import os

# Paths and Setup
input_directory = r"~\combined"
interim_output_directory = r"~\interim"
user_skill_file = r"~\final_combined_skills.csv"  

# Load AI-skilled user IDs
ai_skilled_users = pd.read_csv(user_skill_file)['user_id'].unique()

# Process each file in combined data
for i in range(1, 1000, 50):  # Process in 50-file batches
    start = f"{i:04d}"
    end = f"{min(i + 49, 999):04d}"
    filename = f"combined_{start}_{end}.csv"
    input_file = os.path.join(input_directory, filename)

    if not os.path.exists(input_file):
        print(f"File not found: {filename}, skipping...")
        continue

    df = pd.read_csv(input_file).dropna(subset=['startdate'])
    df['start_year'] = df['startdate'].astype(str).str[:4].astype(int)
    df['end_year'] = df['enddate'].fillna('2024').astype(str).str[:4].astype(int)

    # Load supporting datasets
    df2 = pd.read_stata(r"~\[Revelio - Parat] rcid - id_parat.dta")
    df3 = pd.read_stata(r"~\[Position] Technical Team roles.dta")

    df = pd.merge(df, df2[['rcid', 'id_parat']], on='rcid', how='left')
    df = pd.merge(df, df3[['role_k1000', 'tag']], on='role_k1000', how='left')

    # Expand panel data by year
    df_expanded = pd.DataFrame({
        col: np.repeat(df[col].values, df['end_year'] - df['start_year'] + 1)
        for col in df.columns
    })
    df_expanded['year'] = [year for start, end in zip(df['start_year'], df['end_year']) for year in range(start, end + 1)]
    df_expanded['labor_with_AI_skill'] = df_expanded['user_id'].isin(ai_skilled_users).astype(int)

    # Save interim files
    interim_file = os.path.join(interim_output_directory, f"interim_{filename}")
    df_expanded.to_csv(interim_file, index=False)
    del df, df_expanded
    gc.collect()

print("Part 2 completed successfully.")


# ========================================================
# (Part 3) Generate Firm-Year Level Variables
# ========================================================
# - Create firm-year level metrics, such as counts of AI-skilled individuals.
# - Output: Filtered interim files with aggregated variables.
# --------------------------------------------------------
import pandas as pd
import numpy as np
import os
import gc 

# Paths and Setup
interim_input_directory = r"~\interim"
output_directory = r"~\interim"

# Helper Function
def validate_labor_with_AI_skill(df):
    if not df['labor_with_AI_skill'].isin([0, 1]).all():
        raise ValueError("Invalid 'labor_with_AI_skill' values.")

# Process each interim file
for filename in os.listdir(interim_input_directory):
    if filename.startswith("interim_combined") and filename.endswith(".csv"):
        input_file = os.path.join(interim_input_directory, filename)
        df = pd.read_csv(input_file)

        validate_labor_with_AI_skill(df)
        df['rcid_total'] = df.groupby(['rcid', 'year'])['user_id'].transform('nunique')

        df.to_csv(os.path.join(output_directory, f"filtered_{filename}"), index=False)
        del df
        gc.collect()

print("Part 3 completed successfully.")


# ========================================================
# (Part 4) Export Final Firm-Year Level Data
# ========================================================
# - Combine all filtered interim files into a single CSV.
# - Output: Final dataset at the firm-year level.
# --------------------------------------------------------
import pandas as pd
import os
import gc

# Paths
input_directory = r"~\interim"
final_output_path = r"~\final\firm_year_final.csv"

# Combine all files into a single DataFrame
dataframes = []
for filename in os.listdir(input_directory):
    if filename.startswith("filtered") and filename.endswith(".csv"):
        df = pd.read_csv(os.path.join(input_directory, filename))
        dataframes.append(df)

final_df = pd.concat(dataframes, ignore_index=True)
final_df.to_csv(final_output_path, index=False)

print(f"Final firm-year level dataset saved to: {final_output_path}")
print("Part 4 completed successfully.")
