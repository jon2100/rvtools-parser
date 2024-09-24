#!/usr/bin/env python3

import pandas as pd
import os
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Conversion factor for MiB to MB
MIB_TO_MB = 1.048576

def process_file(file_path, os_filters, capacity_ranges):
    """Process a single Excel file and return OS counts for all capacity ranges and VMware Photon OS counts."""
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return None, None, None
    
    # Find the OS column and check if it exists
    os_col_candidates = df.columns[df.columns.str.contains("OS according to the configuration file", case=False)]
    if os_col_candidates.empty:
        print(f"Skipping {file_path}: 'OS according to the configuration file' column not found.")
        return None, None, None

    os_col = os_col_candidates.tolist()[0]  # Use the first matching column

    # Try to find the column for capacity (MiB or MB)
    capacity_col_candidates = df.columns[df.columns.str.contains("Total disk capacity MiB|Total disk capacity MB", case=False, regex=True)]
    if capacity_col_candidates.empty:
        print(f"Skipping {file_path}: No capacity column found (MiB or MB).")
        return None, None, None

    capacity_col = capacity_col_candidates.tolist()[0]  # Use the first matching column

    # Filter out rows that contain 'Template', 'SRM Placeholder'
    if 'Template' in df.columns and 'SRM Placeholder' in df.columns:
        df = df[(df['Template'] != True) & (df['SRM Placeholder'] != True)]

    df = df[~df[os_col].astype(str).str.contains('Template|SRM Placeholder', case=False, na=False)]

    # Convert MiB to MB (if necessary)
    if "MiB" in capacity_col:
        df[capacity_col] = df[capacity_col] * MIB_TO_MB

    # Prepare results for each capacity range
    results_by_range = []
    for min_capacity, max_capacity, label in capacity_ranges:
        filtered_df = df[
            (df[capacity_col] >= min_capacity) &
            (df[capacity_col] <= max_capacity)
        ]
        if not filtered_df.empty:
            # Group by OS and sum the counts for the same OS within the capacity range
            grouped_result = filtered_df.groupby(os_col).size().reset_index(name='Count')
            grouped_result['Capacity Range'] = label
            grouped_result['OS according to the configuration file'] = grouped_result[os_col]
            results_by_range.append(grouped_result)

    # Handle VMware Photon OS separately, also grouping by capacity range
    vmware_os_col_candidates = df.columns[df.columns.str.contains("OS according to the VMware Tools", case=False)]
    photon_results_by_range = []
    if not vmware_os_col_candidates.empty:
        vmware_os_col = vmware_os_col_candidates.tolist()[0]
        photon_df = df[df[vmware_os_col] == "VMware Photon OS (64-bit)"]
        
        # For Photon OS, also apply capacity range filtering
        for min_capacity, max_capacity, label in capacity_ranges:
            photon_filtered_df = photon_df[
                (photon_df[capacity_col] >= min_capacity) &
                (photon_df[capacity_col] <= max_capacity)
            ]
            if not photon_filtered_df.empty:
                photon_grouped = photon_filtered_df.groupby(vmware_os_col).size().reset_index(name='Count')
                photon_grouped['Capacity Range'] = label
                photon_grouped['OS according to the configuration file'] = 'VMware Photon OS (64-bit)'
                photon_results_by_range.append(photon_grouped)

    # Return the results for all capacity ranges and Photon OS data
    return results_by_range, photon_results_by_range, df[os_col].dropna().unique()

def parallel_process_files(file_paths, capacity_ranges):
    """Process files in parallel, returning the combined OS results for all capacity ranges and Photon OS results."""
    all_results_by_range = []
    all_photon_results_by_range = []
    all_os_filters = set()  # Dynamically collect all unique OS types

    with ProcessPoolExecutor() as executor:
        future_to_file = {executor.submit(process_file, file_path, [], capacity_ranges): file_path for file_path in file_paths}
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing files in parallel"):
            try:
                results_by_range, photon_results_by_range, unique_os = future.result()
                if results_by_range is None:
                    continue  # Skip the file if there were issues
                
                all_results_by_range.extend(results_by_range)
                all_photon_results_by_range.extend(photon_results_by_range)

                if unique_os is not None:
                    all_os_filters.update(unique_os)
            except Exception as exc:
                print(f"File {future_to_file[future]} generated an exception: {exc}")

    # Combine results for each capacity range
    combined_results_by_range = pd.concat(all_results_by_range, ignore_index=True) if all_results_by_range else pd.DataFrame()

    # Combine Photon OS results for each capacity range
    photon_combined_results_by_range = pd.concat(all_photon_results_by_range, ignore_index=True) if all_photon_results_by_range else pd.DataFrame()

    return combined_results_by_range, photon_combined_results_by_range, all_os_filters

def insert_break_and_sum(df):
    # For each capacity range, group the results by OS, sum the counts, and add a "Disk OS Sum" row
    df_with_sums = pd.DataFrame()

    # Group by capacity range and add a sum row after each group
    for label in df['Capacity Range'].unique():
        grouped_df = df[df['Capacity Range'] == label].copy()
        
        # Group by 'OS according to the configuration file' and sum the counts for each OS
        grouped_df = grouped_df.groupby('OS according to the configuration file', as_index=False).agg({'Count': 'sum'})
        grouped_df['Capacity Range'] = label
        
        # Calculate the total count for this capacity range
        total_count = grouped_df['Count'].sum()
        
        # Add sum row for this capacity range
        break_df = pd.DataFrame({
            'OS according to the configuration file': ['Disk OS Sum'],
            'Count': [total_count],
            'Capacity Range': [label],
            'OS according to the VMware Tools': ['']
        })
        
        # Add a blank row after the "Disk OS Sum" row
        blank_row = pd.DataFrame({
            'OS according to the configuration file': [''],
            'Count': [''],
            'Capacity Range': [''],
            'OS according to the VMware Tools': ['']
        })
        
        # Concatenate the results for this capacity range, followed by the sum row and the blank row
        df_with_sums = pd.concat([df_with_sums, grouped_df, break_df, blank_row], ignore_index=True)
    
    return df_with_sums

def insert_total_machine_count(df):
    """Inserts a row for 'Total Machine Count' before the Photon OS block, followed by a blank row."""
    total_machine_count = df[df['OS according to the configuration file'] == 'Disk OS Sum']['Count'].sum()

    total_row = pd.DataFrame({
        'OS according to the configuration file': ['Total Machine Count'],
        'Count': [total_machine_count],
        'Capacity Range': [''],
        'OS according to the VMware Tools': ['']
    })

    # Insert a blank row after the 'Total Machine Count' row
    blank_row = pd.DataFrame({
        'OS according to the configuration file': [''],
        'Count': [''],
        'Capacity Range': [''],
        'OS according to the VMware Tools': ['']
    })

    return pd.concat([df, total_row, blank_row], ignore_index=True)

def main():
    parser = argparse.ArgumentParser(description="Process Excel files and generate OS disk capacity reports.")
    
    # Optional command-line arguments
    parser.add_argument('-src', '--source', default='./data', help='Source folder containing Excel files (default: ./data)')
    parser.add_argument('-dst', '--destination', default='./output', help='Destination folder for the output CSV file (default: ./output)')
    parser.add_argument('-name', '--name', default='output.csv', help='Name of the output CSV file (default: output.csv)')

    args = parser.parse_args()

    # Resolve paths to absolute paths
    src_folder = os.path.abspath(args.source)
    dst_folder = os.path.abspath(args.destination)

    # Get the list of Excel files from the source folder
    file_paths = [os.path.join(src_folder, f) for f in os.listdir(src_folder) if f.endswith('.xlsx')]

    # Define capacity ranges
    capacity_ranges = [
        (0, 149, '0 MB - 149 MB'),
        (150, 2000000, '150 MB - 2 TB'),
        (2000001, 10000000, '2 TB - 10 TB'),
        (10000001, 20000000, '10 TB - 20 TB'),
        (20000001, 40000000, '20 TB - 40 TB'),
        
    ]

    # Create destination folder if it doesn't exist
    os.makedirs(dst_folder, exist_ok=True)

    # Process files in parallel and gather OS and Photon OS data
    combined_results_by_range, photon_combined_by_range, unique_os_filters = parallel_process_files(file_paths, capacity_ranges)

    # If we have capacity-based results, process them and add sum rows for each capacity range
    if not combined_results_by_range.empty:
        combined_results_with_sum = insert_break_and_sum(combined_results_by_range)
        combined_results_with_total = insert_total_machine_count(combined_results_with_sum)
    else:
        combined_results_with_total = pd.DataFrame()

    # Insert sum row for Photon OS by capacity range if photon_combined_by_range is not empty
    if not photon_combined_by_range.empty:
        photon_combined_with_sum = insert_break_and_sum(photon_combined_by_range)
        combined_results_with_total = pd.concat([combined_results_with_total, photon_combined_with_sum], ignore_index=True)

    # Rearrange columns to have 'OS according to the configuration file' as the first column
    column_order = ['OS according to the configuration file', 'Count', 'Capacity Range', 'OS according to the VMware Tools']
    combined_results_with_total = combined_results_with_total[column_order]

    # Output the combined result to a single CSV file
    output_file = os.path.join(dst_folder, args.name)
    combined_results_with_total.to_csv(output_file, index=False)

    # Debug: Print the columns of the resulting DataFrame to confirm structure
    print(f"Output file: {output_file}")
    print("Columns in final result:", combined_results_with_total.columns.tolist())
    print(f"Unique OS Filters used: {list(unique_os_filters)}")  # Debugging OS Filters

if __name__ == "__main__":
    main()
