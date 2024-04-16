import os
import pandas as pd

# Find CSV files in all folders of the current directory
csv_files = []
for root, dirs, files in os.walk('.'):
    for file in files:
        if file.endswith('.csv') and file.startswith('cars_'):
            csv_files.append(os.path.join(root, file))

# Read and merge the CSV files
data_frames = []
for file in csv_files:
    data = pd.read_csv(file)
    data_frames.append(data)

merged_data = pd.concat(data_frames, ignore_index=True)

# Remove duplicates based on the "vin" attribute
merged_data.drop_duplicates(subset='vin', inplace=True)

# Write the merged and deduplicated data to a new CSV file
merged_data.to_csv('merged_data.csv', index=False)