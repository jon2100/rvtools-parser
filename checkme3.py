#!/usr/bin/env python3

import pandas as pd
import os
import argparse
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm  # Import the tqdm progress bar

# Conversion factor for MiB to MB
MIB_TO_MB = 1.048576

def load_unique_os_filters(file_paths):
    """Dynamically load unique OS filters from the 'OS according to the configuration file' column in the given Excel files."""
    os_filters = set()
    for file_path in tqdm(file_paths, desc="Loading OS filters from files"):
        df = pd.read_excel(file_path)
        os_col = df.columns[df.columns.str.contains("OS according to the configuration file", case=False)].tolist()
        if os_col:
            os_filters.update(df[os_col[0]].dropna().unique())
    return list(os_filters)

def process_file(file_path, os_filters, min_capacity, max_capacity):
    """Process a single Excel file and return grouped results."""
    df = pd.read_excel(file_path)
    
    # Find the OS column and check if it exists
    os_col_candidates = df.columns[df.columns.str.contains("OS according to the configuration file", case=False)]
    
    if os_col_candidates.empty:
        print(f"Column 'OS according to the configuration file' not found in {file_path}. Skipping file.")
        return None
    
    os_col = os_col_candidates.tolist()[0]  # Use the first matching column

    # Try to find the column for capacity (MiB) in a flexible way
    capacity_col_candidates = df.columns[df.columns.str.contains("Total.*disk.*capacity.*(MiB|MB)", case=False)]
    
    if capacity_col_candidates.empty:
        raise ValueError(f"Could not find the capacity column in file: {file_path}")
    
    capacity_col = capacity_col_candidates.tolist()[0]  # Use the first matching column

    # Filter out rows that contain 'Template', 'SRM Placeholder', or 'AdditionalBackEnd'
    if 'Template' in df.columns and 'SRM Placeholder' in df.columns:
        df = df[(df['Template'] != True) & (df['SRM Placeholder'] != True)]

    df = df[~df[os_col].astype(str).str.contains('Template|SRM Placeholder|AdditionalBackEnd', case=False, na=False)]

    # Convert MiB to MB (if necessary) by checking if the capacity column has 'MiB' in its name
    if "MiB" in capacity_col:
        df[capacity_col] = df[capacity_col] * MIB_TO_MB  # Convert MiB to MB

    # Filter by capacity range and OS filters
    filtered_df = df[
        (df[capacity_col] >= min_capacity) &
        (df[capacity_col] <= max_capacity) &
        (df[os_col].isin(os_filters))
    ]

    # Check if the filtered dataframe is empty; if so, skip this file
    if filtered_df.empty:
        print(f"No matching OS data in {file_path}. Skipping file.")
        return None

    # Group the results by OS
    grouped_result = filtered_df.groupby(os_col).size().reset_index(name='Count')
    return grouped_result

def count_os_by_capacity(file_paths, os_filters, min_capacity, max_capacity):
    results = []
    # Wrap the iteration over files with tqdm for the progress bar
    for file_path in tqdm(file_paths, desc="Processing files by capacity"):
        result = process_file(file_path, os_filters, min_capacity, max_capacity)
        if result is not None:
            results.append(result)  # Only append if the result is not None (i.e., data exists)
    
    # If no results are collected, return an empty DataFrame
    if not results:
        return pd.DataFrame(columns=["OS according to the configuration file", "Count"])
    
    combined_df = pd.concat(results, ignore_index=True)
    return combined_df.groupby(combined_df.columns[0]).sum().reset_index()

def filter_photon_os(file_paths):
    combined_photon_df = pd.DataFrame()

    for file_path in tqdm(file_paths, desc="Filtering VMware Photon OS"):
        df = pd.read_excel(file_path)
        
        vmware_os_col = df.columns[df.columns.str.contains("OS according to the VMware Tools", case=False)].tolist()
        if not vmware_os_col:
            continue
        
        vmware_os_col = vmware_os_col[0]
        photon_df = df[df[vmware_os_col] == "VMware Photon OS (64-bit)"]

        combined_photon_df = pd.concat([combined_photon_df, photon_df])

    photon_grouped = combined_photon_df.groupby(vmware_os_col).size().reset_index(name='Count')
    return photon_grouped

def insert_break_and_sum(df):
    total_count = df['Count'].sum()
    break_df = pd.DataFrame({'OS according to the configuration file': ['Disk OS Sum', ''], 'Count': [total_count, ''], 'Capacity Range': ['', '']})
    return pd.concat([df, break_df], ignore_index=True)

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

    # Dynamically load unique OS filters from the Excel files
    os_filters = load_unique_os_filters(file_paths)
    print(f"Loaded OS filters: {os_filters}")

    # Define capacity ranges
    capacity_ranges = [
        (150, 2000000, '150 MB - 2 TB'),
        (2000001, 10000000, '2 TB - 10 TB'),
        (10000001, 20000000, '10 TB - 20 TB'),
        (20000001, 40000000, '20 TB - 40 TB'),
        (0, 149, '0 MB - 149 MB'),
    ]

    # Create destination folder if it doesn't exist
    os.makedirs(dst_folder, exist_ok=True)

    # Process each capacity range with a progress bar
    combined_results = pd.DataFrame()
    for min_capacity, max_capacity, label in capacity_ranges:
        result = count_os_by_capacity(file_paths, os_filters, min_capacity, max_capacity)
        if not result.empty:  # Ensure to only process non-empty results
            result['Capacity Range'] = label
            result_with_sum = insert_break_and_sum(result)
            combined_results = pd.concat([combined_results, result_with_sum], ignore_index=True)

    # Calculate total sum of all group sums
    total_machine_count = combined_results[combined_results['OS according to the configuration file'] == 'Disk OS Sum']['Count'].sum()

    # Add a row for the total sum at the end
    total_row = pd.DataFrame({'OS according to the configuration file': ['Total Machine Count'], 'Count': [total_machine_count], 'Capacity Range': ['']})
    combined_results = pd.concat([combined_results, total_row], ignore_index=True)

    # Handle VMware Photon OS
    photon_result = filter_photon_os(file_paths)
    photon_result['Capacity Range'] = 'All Capacities'
    photon_result['OS according to the configuration file'] = 'VMware Photon OS (64-bit)'

    # Insert sum row for Photon OS
    photon_count = photon_result['Count'].sum()
    photon_total_row = pd.DataFrame({
        'OS according to the configuration file': ['Disk OS Sum'],
        'Count': [photon_count],
        'Capacity Range': ['All Capacities']
    })

    # Combine Photon OS results with the rest
    combined_results = pd.concat([combined_results, photon_result, photon_total_row], ignore_index=True)

    # Output the combined result to a single CSV file
    output_file = os.path.join(dst_folder, args.name)
    combined_results.to_csv(output_file, index=False)

    print(f"Combined results including VMware Photon OS saved to {output_file}")

if __name__ == "__main__":
    main()
