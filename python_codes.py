##### To combined files (files with firms of interest), attach id_parat (by rcid) and tag (eqauls to 1 if the individual's role in the firm is related to AI (from ETO Report)). //
##### Then, attach AI-skilled-individuals (by user_id).
##### Thus, all interim files (i.e., interim_combined_0###_0###.csv) have 
##### information on user_id, whether the user_id's role is AI-related based on ETO report (tag),
##### whether the user_id has AI-related skills (labor_with_AI_skill), for firms of interest.
##### Finally, using startdate and enddate, generate panel data (unique in company user_id year)
##### Dropped observations whose startdate is missing
##### For observations whose enddate is missing, fill the missing value as 2024, assuming that the user_id is still working in the firm. 

# import pandas as pd
# import numpy as np
# import gc  # For garbage collection
# import os

# # Paths to input and output directories
# input_directory = r"H:\Revelio\academic_individual_position\stata\combined"
# interim_output_directory = r"H:\Revelio\academic_individual_position\stata\interim"
# user_skill_file = r"H:\Revelio\academic_individual_user_skill\final\final_combined_skills.csv"

# # Ensure the interim output directory exists
# os.makedirs(interim_output_directory, exist_ok=True)

# # Step 1: Load the user skills data to identify AI-skilled individuals
# ai_skilled_users = pd.read_csv(user_skill_file)['user_id'].unique()

# # Helper function to generate filenames dynamically
# def generate_filenames():
#     for i in range(1, 1000, 50):  # Step by 50 for file ranges (0001 to 0999)
#         start = f"{i:04d}"
#         end = f"{min(i + 49, 999):04d}"
#         yield f"combined_{start}_{end}.csv"

# # Step 2: Process each file one by one and save interim results
# for filename in generate_filenames():
#     input_file = os.path.join(input_directory, filename)

#     if not os.path.exists(input_file):
#         print(f"File not found: {filename}, skipping...")
#         continue

#     # Show the name of the file being processed
#     print(f"Processing file: {filename}")

#     # Load the current CSV file
#     df1 = pd.read_csv(input_file)

#     # Remove rows with missing 'startdate'
#     df1 = df1.dropna(subset=['startdate'])

#     # Extract 'start_year' and 'end_year'
#     df1['start_year'] = df1['startdate'].astype(str).str[:4]
#     df1['end_year'] = df1['enddate'].astype(str).str[:4].fillna('2024').replace('nan', '2024')
#     df1['start_year'] = df1['start_year'].astype(int)
#     df1['end_year'] = df1['end_year'].astype(int)

#     # Merge with Stata files
#     df2 = pd.read_stata(r"H:\Revelio\[Revelio - Parat] rcid - id_parat.dta")
#     df1 = pd.merge(df1, df2[['rcid', 'id_parat']], on='rcid', how='left')

#     df3 = pd.read_stata(r"H:\Revelio\[Position] Technical Team roles (only tag 1).dta")
#     df1 = pd.merge(df1, df3[['role_k1000', 'tag']], on='role_k1000', how='left')

#     # Swap start and end years if start_year > end_year
#     df1['start_year'], df1['end_year'] = np.where(
#         df1['start_year'] > df1['end_year'],
#         [df1['end_year'], df1['start_year']],
#         [df1['start_year'], df1['end_year']]
#     )

#     # Expand rows by year range
#     df1_expanded = pd.DataFrame({
#         col: np.repeat(df1[col].values, df1['end_year'] - df1['start_year'] + 1)
#         for col in df1.columns
#     })
#     df1_expanded['year'] = [
#         year for start, end in zip(df1['start_year'], df1['end_year'])
#         for year in range(start, end + 1)
#     ]

#     # Tag observations with AI skills
#     df1_expanded['labor_with_AI_skill'] = df1_expanded['user_id'].isin(ai_skilled_users).astype(int)

#     # Save the interim result
#     interim_file = os.path.join(interim_output_directory, f"interim_{filename}")
#     df1_expanded.to_csv(interim_file, index=False)
#     print(f"Processed and saved: {interim_file}")

#     # Perform garbage collection
#     del df1, df1_expanded, df2, df3
#     gc.collect()

# print("All files have been processed and saved as interim files.")

##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
#### Generate dataset that has firm-year level variables (by rcid - year; by id_parat - year)

# import pandas as pd
# import numpy as np
# import os
# import gc  # For garbage collection

# # Paths to directories
# interim_input_directory = r"H:\Revelio\academic_individual_position\stata\interim"
# output_directory = r"H:\Revelio\academic_individual_position\stata\interim"

# # Ensure the output directory exists
# os.makedirs(output_directory, exist_ok=True)

# # Helper function to generate filenames dynamically
# def generate_interim_filenames():
#     for i in range(1, 1000, 50):  # Step by 50 for file ranges (0001 to 0999)
#         start = f"{i:04d}"
#         end = f"{min(i + 49, 999):04d}"
#         yield f"interim_combined_{start}_{end}.csv"

# # Function to validate 'labor_with_AI_skill' values
# def validate_labor_with_AI_skill(df):
#     if not df['labor_with_AI_skill'].isin([0, 1]).all():
#         raise ValueError("labor_with_AI_skill values are not 1 or 0")

# # Step 1: Process each interim file
# for filename in generate_interim_filenames():
#     input_file = os.path.join(interim_input_directory, filename)
#     output_file = os.path.join(output_directory, f"interim_filtered_{filename}")

#     if not os.path.exists(input_file):
#         print(f"File not found: {filename}, skipping...")
#         continue

#     # Load the interim file
#     print(f"Processing file: {filename}")
#     df = pd.read_csv(input_file)

#     # Step 2: Validate 'labor_with_AI_skill'
#     try:
#         validate_labor_with_AI_skill(df)
#     except ValueError as e:
#         print(e)
#         break  # Stop processing if validation fails

#     # Step 3: Create the eight new variables
#     df['rcid_total'] = df.groupby(['rcid', 'year'])['user_id'].transform('nunique')
#     df['rcid_tag'] = df[df['tag'] == 1].groupby(['rcid', 'year'])['user_id'].transform('nunique').fillna(0)

#     df['rcid_AI_skill'] = df[df['labor_with_AI_skill'] == 1].groupby(['rcid', 'year'])['user_id'].transform('nunique').fillna(0)
#     df['rcid_tag_AI_skill'] = df[(df['labor_with_AI_skill'] == 1) & (df['tag'] == 1)].groupby(['rcid', 'year'])['user_id'].transform('nunique').fillna(0)

#     df['id_parat_total'] = df.groupby(['id_parat', 'year'])['user_id'].transform('nunique')
#     df['id_parat_tag'] = df[df['tag'] == 1].groupby(['id_parat', 'year'])['user_id'].transform('nunique').fillna(0)

#     df['id_parat_AI_skill'] = df[df['labor_with_AI_skill'] == 1].groupby(['id_parat', 'year'])['user_id'].transform('nunique').fillna(0)
#     df['id_parat_tag_AI_skill'] = df[(df['labor_with_AI_skill'] == 1) & (df['tag'] == 1)].groupby(['id_parat', 'year'])['user_id'].transform('nunique').fillna(0)

#     # Step 4: Save the result as an interim filtered file
#     df.to_csv(output_file, index=False)
#     print(f"Processed and saved: {output_file}")

#     # Step 5: Perform garbage collection
#     del df
#     gc.collect()

# print("All files have been processed and saved as interim filtered files.")


##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
#### Generate firm-year level dataset (by rcid - year; by id_parat - year)

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

