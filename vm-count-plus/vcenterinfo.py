#!/usr/bin/env python3

import pandas as pd
import os
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Conversion factors
MIB_TO_MB = 1.048576
MB_TO_GB = 1024
MB_TO_TB = 1024 * 1024

def process_file(file_path, os_filters, capacity_ranges):
    """Process a single Excel file and return OS counts for all capacity ranges, VMware Photon OS counts, and filtered cluster counts."""
    df = pd.read_excel(file_path)

    # Initialize the results dictionary
    results_by_range = {}
    photon_df = pd.DataFrame()  # Initialize an empty DataFrame for VMware Photon OS

    # Find the OS column and check if it exists
    os_col_candidates = df.columns[df.columns.str.contains("OS according to the configuration file", case=False)]
    if os_col_candidates.empty:
        return results_by_range, photon_df, pd.DataFrame()

    os_col = os_col_candidates.tolist()[0]

    # Try to find the column for capacity (MiB or MB)
    capacity_col_candidates = df.columns[df.columns.str.contains("Total disk capacity MiB|Total disk capacity MB", case=False, regex=True)]
    if capacity_col_candidates.empty:
        return results_by_range, photon_df, pd.DataFrame()

    capacity_col = capacity_col_candidates.tolist()[0]

    # Filter out rows that contain 'Template', 'SRM Placeholder'
    if 'Template' in df.columns and 'SRM Placeholder' in df.columns:
        df = df[(df['Template'] != True) & (df['SRM Placeholder'] != True)]

    df = df[~df[os_col].astype(str).str.contains('Template|SRM Placeholder', case=False, na=False)]

    # Isolate "VMware Photon OS (64-bit)" entries
    vmware_os_col_candidates = df.columns[df.columns.str.contains("OS according to the VMware Tools", case=False)]
    if not vmware_os_col_candidates.empty:
        vmware_os_col = vmware_os_col_candidates.tolist()[0]
        photon_df = df[df[vmware_os_col] == "VMware Photon OS (64-bit)"]  # Get only VMware Photon OS rows
        df = df[df[vmware_os_col] != "VMware Photon OS (64-bit)"]  # Remove VMware Photon OS rows from the main DataFrame

    # Convert MiB to MB (if necessary)
    if "MiB" in capacity_col:
        df[capacity_col] = df[capacity_col] * MIB_TO_MB

    # Prepare results for each capacity range
    for min_capacity, max_capacity, label in capacity_ranges:
        filtered_df = df[
            (df[capacity_col] >= min_capacity) &
            (df[capacity_col] <= max_capacity)
        ]
        if not filtered_df.empty:
            grouped_result = filtered_df.groupby(os_col).size().reset_index(name='Count')
            grouped_result['Capacity Range'] = label
            grouped_result['OS according to the configuration file'] = grouped_result[os_col]
            results_by_range[label] = grouped_result

    # Return the DataFrame for cluster counting (excluding VMware Photon OS)
    return results_by_range, photon_df, df

def parallel_process_files(file_paths, capacity_ranges):
    """Process files in parallel, returning the combined OS results for all capacity ranges, VMware Photon OS counts, and filtered cluster counts."""
    all_results_by_range = {label: [] for _, _, label in capacity_ranges}
    photon_dfs = []  # List to collect all VMware Photon OS data
    cluster_dfs = []

    with ProcessPoolExecutor() as executor:
        future_to_file = {executor.submit(process_file, file_path, [], capacity_ranges): file_path for file_path in file_paths}
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing files in parallel"):
            try:
                results_by_range, photon_df, cluster_df = future.result()
                for label, result in results_by_range.items():
                    if result is not None and not result.empty:
                        all_results_by_range[label].append(result)

                # Add VMware Photon OS entries to the list
                if not photon_df.empty:
                    photon_dfs.append(photon_df)

                # Only add to cluster_dfs if it's not empty and contains valid data
                if not cluster_df.empty:
                    cluster_df = cluster_df.dropna(how='all')  # Remove all-NA rows
                    cluster_df = cluster_df.loc[:, cluster_df.notna().any(axis=0)]  # Drop columns with all NaNs

                    if not cluster_df.empty:  # Ensure DataFrame is still not empty
                        cluster_dfs.append(cluster_df)

            except Exception as exc:
                print(f"File {future_to_file[future]} generated an exception: {exc}")

    # Filter cluster_dfs to include only non-empty DataFrames with valid data
    cluster_dfs = [df for df in cluster_dfs if not df.empty and not df.isna().all(axis=None)]

    # Check if cluster_dfs has valid DataFrames before concatenating
    if cluster_dfs:
        combined_cluster_df = pd.concat(cluster_dfs, ignore_index=True)

        # Convert "Total disk capacity MiB" to MB if it's in MiB
        if "Total disk capacity MiB" in combined_cluster_df.columns:
            combined_cluster_df["Total disk capacity MiB"] = combined_cluster_df["Total disk capacity MiB"] * MIB_TO_MB

        # Separate rows where "Total disk capacity MiB" is 0
        zero_capacity_df = combined_cluster_df[combined_cluster_df['Total disk capacity MiB'] == 0]

        # Filter out "VMware Photon OS (64-bit)" in the "OS according to the VMware Tools" column for vCluster VM Count
        if 'OS according to the VMware Tools' in combined_cluster_df.columns:
            combined_cluster_df = combined_cluster_df[(combined_cluster_df['OS according to the VMware Tools'] != "VMware Photon OS (64-bit)") & 
                                                      (combined_cluster_df['Total disk capacity MiB'] != 0)]

        # Group by cluster and sum up CPUs, Memory, and Disk Capacity
        cluster_summary = combined_cluster_df.groupby('Cluster').agg(
            VM_Count=('Cluster', 'size'),
            Total_CPUs=('CPUs', 'sum'),
            Total_Memory_GB=('Memory', lambda x: x.sum() / MB_TO_GB),  # Convert MB to GB
            Total_Disk_Capacity_TB=('Total disk capacity MiB', lambda x: x.sum() / MB_TO_TB)  # Convert MB to TB
        ).reset_index()

        # Format values with thousands separators
        cluster_summary['Total_Memory_GB'] = cluster_summary['Total_Memory_GB'].map('{:,.2f}'.format)
        cluster_summary['Total_Disk_Capacity_TB'] = cluster_summary['Total_Disk_Capacity_TB'].map('{:,.2f}'.format)
        cluster_summary['Total_CPUs'] = cluster_summary['Total_CPUs'].map('{:,}'.format)
        cluster_summary['VM_Count'] = cluster_summary['VM_Count'].map('{:,}'.format)

        # Add total row for the entire cluster summary
        total_row = pd.DataFrame({
            'Cluster': ['Total Machine Count'],
            'VM_Count': ['{:,}'.format(cluster_summary['VM_Count'].replace(',', '', regex=True).astype(int).sum())],
            'Total_CPUs': ['{:,}'.format(cluster_summary['Total_CPUs'].replace(',', '', regex=True).astype(int).sum())],
            'Total_Memory_GB': ['{:,.2f}'.format(cluster_summary['Total_Memory_GB'].replace(',', '', regex=True).astype(float).sum())],
            'Total_Disk_Capacity_TB': ['{:,.2f}'.format(cluster_summary['Total_Disk_Capacity_TB'].replace(',', '', regex=True).astype(float).sum())]
        })
        cluster_summary = pd.concat([cluster_summary, total_row], ignore_index=True)

        # Add two empty rows after "Total Machine Count"
        empty_rows = pd.DataFrame({'Cluster': ['', ''], 'VM_Count': ['', ''], 'Total_CPUs': ['', ''], 'Total_Memory_GB': ['', ''], 'Total_Disk_Capacity_TB': ['', '']})
        cluster_summary = pd.concat([cluster_summary, empty_rows], ignore_index=True)

        # Group and count zero-capacity VMs separately
        zero_capacity_summary = zero_capacity_df.groupby('Cluster').agg(
            VM_Count=('Cluster', 'size'),
            Total_CPUs=('CPUs', 'sum'),
            Total_Memory_GB=('Memory', lambda x: x.sum() / MB_TO_GB),
            Total_Disk_Capacity_TB=('Total disk capacity MiB', lambda x: x.sum() / MB_TO_TB)
        ).reset_index()
        zero_capacity_summary['Cluster'] = zero_capacity_summary['Cluster'] + ' (Zero Capacity)'

        # Format zero capacity values with thousands separators
        zero_capacity_summary['Total_Memory_GB'] = zero_capacity_summary['Total_Memory_GB'].map('{:,.2f}'.format)
        zero_capacity_summary['Total_Disk_Capacity_TB'] = zero_capacity_summary['Total_Disk_Capacity_TB'].map('{:,.2f}'.format)
        zero_capacity_summary['Total_CPUs'] = zero_capacity_summary['Total_CPUs'].map('{:,}'.format)
        zero_capacity_summary['VM_Count'] = zero_capacity_summary['VM_Count'].map('{:,}'.format)

        zero_total_row = pd.DataFrame({
            'Cluster': ['Total Zero Capacity Count'],
            'VM_Count': ['{:,}'.format(zero_capacity_summary['VM_Count'].replace(',', '', regex=True).astype(int).sum())],
            'Total_CPUs': ['{:,}'.format(zero_capacity_summary['Total_CPUs'].replace(',', '', regex=True).astype(int).sum())],
            'Total_Memory_GB': ['{:,.2f}'.format(zero_capacity_summary['Total_Memory_GB'].replace(',', '', regex=True).astype(float).sum())],
            'Total_Disk_Capacity_TB': ['{:,.2f}'.format(zero_capacity_summary['Total_Disk_Capacity_TB'].replace(',', '', regex=True).astype(float).sum())]
        })
        zero_capacity_summary = pd.concat([zero_capacity_summary, zero_total_row], ignore_index=True)

        # Combine both summaries
        cluster_summary = pd.concat([cluster_summary, zero_capacity_summary], ignore_index=True)
    else:
        print("No valid DataFrames found in cluster_dfs for concatenation.")
        cluster_summary = pd.DataFrame(columns=['Cluster', 'VM_Count', 'Total_CPUs', 'Total_Memory_GB', 'Total_Disk_Capacity_TB'])

    # Combine capacity range results for OS_Disk_Count
    combined_results_by_range = {}
    for label, results in all_results_by_range.items():
        if results:
            combined_df = pd.concat(results, ignore_index=True)
            combined_results_by_range[label] = combined_df.groupby(combined_df.columns[0]).sum().reset_index()
        else:
            combined_results_by_range[label] = pd.DataFrame(columns=["OS according to the configuration file", "Count"])

    # Combine VMware Photon OS data into a separate group
    photon_summary = pd.DataFrame()
    if photon_dfs:
        combined_photon_df = pd.concat(photon_dfs, ignore_index=True)
        photon_summary = combined_photon_df.groupby("OS according to the VMware Tools").size().reset_index(name='Count')
        photon_summary['Capacity Range'] = 'VMware Photon OS'
        photon_summary.rename(columns={"OS according to the VMware Tools": "OS according to the configuration file"}, inplace=True)

    return combined_results_by_range, photon_summary, cluster_summary

def insert_break_and_sum(df):
    """Insert sum row and ensure column exists, with a separator row for visual separation."""
    if 'OS according to the configuration file' not in df.columns:
        raise KeyError("'OS according to the configuration file' column missing in data.")

    total_count = pd.to_numeric(df['Count'], errors='coerce').fillna(0).sum()
    break_df = pd.DataFrame({'OS according to the configuration file': ['Disk OS Sum'], 'Count': [total_count], 'Capacity Range': ['']})
    separator_row = pd.DataFrame({'OS according to the configuration file': [''], 'Count': [''], 'Capacity Range': ['']})
    return pd.concat([df, break_df, separator_row], ignore_index=True)

def adjust_column_widths(writer, dataframe, sheet_name):
    """Adjust column widths based on the length of the data in each column."""
    worksheet = writer.sheets[sheet_name]
    for column in dataframe.columns:
        column_length = max(dataframe[column].astype(str).map(len).max(), len(column))
        col_idx = dataframe.columns.get_loc(column)
        worksheet.set_column(col_idx, col_idx, column_length + 2)

def main():
    parser = argparse.ArgumentParser(
        description="Process Excel files and generate OS disk capacity reports.",
        epilog="Example: python3 disk-groupby-capacity.py -s /path/to/source -d /path/to/destination -n os_report"
    )

    parser.add_argument('-s', '--src', default='./data', help='Source folder containing Excel files (default: ./data)')
    parser.add_argument('-d', '--dst', default='./output', help='Destination folder for the output file (default: ./output)')
    parser.add_argument('-n', '--name', default='output', help='Base name for the output file (default: output, will be saved as output.xlsx)')

    args = parser.parse_args()

    # Ensure the output file has .xlsx extension
    output_file_name = f"{args.name}.xlsx" if not args.name.endswith('.xlsx') else args.name

    src_folder = os.path.abspath(args.src)
    dst_folder = os.path.abspath(args.dst)

    file_paths = [os.path.join(src_folder, f) for f in os.listdir(src_folder) if f.endswith('.xlsx')]

    capacity_ranges = [
        (0, 10, '0 MB - 10 MB'),
        (10, 149, '10 MB - 149 MB'),
        (150, 2000000, '150 MB - 2 TB'),
        (2000001, 10000000, '2 TB - 10 TB'),
        (10000001, 20000000, '10 TB - 20 TB'),
        (20000001, 40000000, '20 TB - 40 TB')
    ]

    os.makedirs(dst_folder, exist_ok=True)

    combined_results_by_range, photon_summary, cluster_summary = parallel_process_files(file_paths, capacity_ranges)

    combined_results = pd.DataFrame()
    for label, os_summary in combined_results_by_range.items():
        if not os_summary.empty:
            os_summary['Capacity Range'] = label
            os_summary_with_sum = insert_break_and_sum(os_summary)
            combined_results = pd.concat([combined_results, os_summary_with_sum], ignore_index=True)

    # Calculate the total sum of all "Disk OS Sum" rows
    total_machine_count = pd.to_numeric(
        combined_results[combined_results['OS according to the configuration file'] == 'Disk OS Sum']['Count'],
        errors='coerce'
    ).fillna(0).sum()

    # Add the total row at the bottom
    total_row = pd.DataFrame({
        'OS according to the configuration file': ['Total Machine Count'],
        'Count': [total_machine_count],
        'Capacity Range': ['']
    })
    combined_results = pd.concat([combined_results, total_row], ignore_index=True)

    # Add Photon OS summary to the results (last)
    if not photon_summary.empty:
        combined_results = pd.concat([combined_results, photon_summary], ignore_index=True)

    # Dynamically build columns order
    columns_order = ['OS according to the configuration file', 'Count', 'Capacity Range']
    combined_results = combined_results[[col for col in columns_order if col in combined_results.columns]]

    output_file = os.path.join(dst_folder, output_file_name)
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        # Write the first sheet for OS disk counts
        sheet_name = 'OS_Disk_Count'
        combined_results.to_excel(writer, sheet_name=sheet_name, index=False)
        adjust_column_widths(writer, combined_results, sheet_name)

        # Write the second sheet for cluster VM counts
        cluster_sheet_name = 'vCluster VM Count'
        cluster_summary.to_excel(writer, sheet_name=cluster_sheet_name, index=False)
        adjust_column_widths(writer, cluster_summary, cluster_sheet_name)

    print(f"Combined results and cluster VM counts saved to {output_file}")

if __name__ == "__main__":
    main()
