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
    df = pd.read_excel(file_path)
    
    # Find the OS column and check if it exists
    os_col_candidates = df.columns[df.columns.str.contains("OS according to the configuration file", case=False)]
    if os_col_candidates.empty:
        return None, None, None

    os_col = os_col_candidates.tolist()[0]  # Use the first matching column

    # Try to find the column for capacity (MiB or MB)
    capacity_col_candidates = df.columns[df.columns.str.contains("Total disk capacity MiB|Total disk capacity MB", case=False, regex=True)]
    if capacity_col_candidates.empty:
        return None, None, None

    capacity_col = capacity_col_candidates.tolist()[0]  # Use the first matching column

    # Filter out rows that contain 'Template', 'SRM Placeholder', or 'AdditionalBackEnd'
    if 'Template' in df.columns and 'SRM Placeholder' in df.columns:
        df = df[(df['Template'] != True) & (df['SRM Placeholder'] != True)]

    df = df[~df[os_col].astype(str).str.contains('Template|SRM Placeholder|AdditionalBackEnd', case=False, na=False)]

    # Convert MiB to MB (if necessary)
    if "MiB" in capacity_col:
        df[capacity_col] = df[capacity_col] * MIB_TO_MB

    # Handle VMware Photon OS at the same time
    vmware_os_col_candidates = df.columns[df.columns.str.contains("OS according to the VMware Tools", case=False)]
    photon_result = None
    if not vmware_os_col_candidates.empty:
        vmware_os_col = vmware_os_col_candidates.tolist()[0]
        photon_result = df[df[vmware_os_col] == "VMware Photon OS (64-bit)"]

    # Prepare results for each capacity range
    results_by_range = {}
    for min_capacity, max_capacity, label in capacity_ranges:
        filtered_df = df[
            (df[capacity_col] >= min_capacity) &
            (df[capacity_col] <= max_capacity) &
            (df[os_col].isin(os_filters))
        ]
        if not filtered_df.empty:
            grouped_result = filtered_df.groupby(os_col).size().reset_index(name='Count')
            results_by_range[label] = grouped_result

    # Return the results for all capacity ranges and Photon OS data
    return results_by_range, photon_result, df[os_col].dropna().unique()

def parallel_process_files(file_paths, os_filters, capacity_ranges):
    """Process files in parallel, returning the combined OS results for all capacity ranges and Photon OS results."""
    all_results_by_range = {label: [] for _, _, label in capacity_ranges}
    photon_results = []
    unique_os_filters = set()

    with ProcessPoolExecutor() as executor:
        future_to_file = {executor.submit(process_file, file_path, os_filters, capacity_ranges): file_path for file_path in file_paths}
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing files in parallel"):
            try:
                results_by_range, photon_result, unique_os = future.result()
                for label, result in results_by_range.items():
                    if result is not None:
                        all_results_by_range[label].append(result)

                if photon_result is not None and not photon_result.empty:
                    photon_results.append(photon_result)

                if unique_os is not None:
                    unique_os_filters.update(unique_os)
            except Exception as exc:
                print(f"File {future_to_file[future]} generated an exception: {exc}")

    # Combine results for each capacity range
    combined_results_by_range = {}
    for label, results in all_results_by_range.items():
        if results:
            combined_df = pd.concat(results, ignore_index=True)
            combined_results_by_range[label] = combined_df.groupby(combined_df.columns[0]).sum().reset_index()
        else:
            combined_results_by_range[label] = pd.DataFrame(columns=["OS according to the configuration file", "Count"])

    # Combine Photon OS results
    if photon_results:
        photon_combined_df = pd.concat(photon_results, ignore_index=True)
        photon_summary = photon_combined_df.groupby("OS according to the VMware Tools").size().reset_index(name='Count')
    else:
        photon_summary = pd.DataFrame(columns=["OS according to the VMware Tools", "Count"])

    return combined_results_by_range, photon_summary, unique_os_filters

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

    # Process files in parallel and gather OS and Photon OS data
    combined_results_by_range, photon_combined, unique_os_filters = parallel_process_files(file_paths, [], capacity_ranges)

    combined_results = pd.DataFrame()
    for label, os_summary in combined_results_by_range.items():
        if not os_summary.empty:  # Ensure to only process non-empty results
            os_summary['Capacity Range'] = label
            os_summary_with_sum = insert_break_and_sum(os_summary)
            combined_results = pd.concat([combined_results, os_summary_with_sum], ignore_index=True)

    # Calculate total sum of all group sums
    total_machine_count = combined_results[combined_results['OS according to the configuration file'] == 'Disk OS Sum']['Count'].sum()

    # Add a row for the total sum at the end
    total_row = pd.DataFrame({'OS according to the configuration file': ['Total Machine Count'], 'Count': [total_machine_count], 'Capacity Range': ['']})
    combined_results = pd.concat([combined_results, total_row], ignore_index=True)

    # Insert sum row for Photon OS if photon_combined is not empty
    if not photon_combined.empty:
        photon_combined['Capacity Range'] = 'All Capacities'
        photon_combined['OS according to the configuration file'] = 'VMware Photon OS (64-bit)'
        photon_count = photon_combined['Count'].sum()
        photon_total_row = pd.DataFrame({
            'OS according to the configuration file': ['Disk OS Sum'],
            'Count': [photon_count],
            'Capacity Range': ['All Capacities']
        })
        combined_results = pd.concat([combined_results, photon_combined, photon_total_row], ignore_index=True)

    # Output the combined result to a single CSV file
    output_file = os.path.join(dst_folder, args.name)
    combined_results.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()