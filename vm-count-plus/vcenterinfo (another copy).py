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

def load_ignore_patterns(ignore_file):
    """Load ignore patterns from a file, each pattern on a new line."""
    if ignore_file and os.path.isfile(ignore_file):
        with open(ignore_file, 'r') as f:
            patterns = [line.strip() for line in f if line.strip()]
        return patterns
    return []

def process_file(file_path, capacity_ranges, ignore_powered_off, ignore_patterns, ignore_vm_folder):
    """Process a single Excel file and return OS counts for all capacity ranges and cluster statistics."""
    df = pd.read_excel(file_path)

    # Filter out powered-off VMs if requested
    if ignore_powered_off and 'Powerstate' in df.columns:
        df = df[df['Powerstate'] != 'poweredOff']

    # Apply ignore patterns from ignore file to VM Name column
    if 'Name' in df.columns and ignore_patterns:
        regex_pattern = '|'.join(ignore_patterns)
        df = df[~df['Name'].astype(str).str.contains(regex_pattern, case=False, na=False)]

    # Apply --ignore-vm filter to Cluster, Folder, Function, and Annotation columns
    if ignore_vm_folder:
        vm_folder_patterns = ignore_vm_folder.split(',')
        regex_filter = '|'.join([pattern.strip() for pattern in vm_folder_patterns])

        # Filter for each relevant column if it exists in the DataFrame
        columns_to_filter = ['Cluster', 'Folder', 'Function', 'Annotation']
        for col in columns_to_filter:
            if col in df.columns:
                df = df[~df[col].astype(str).str.contains(regex_filter, case=False, na=False)]

    # Identify OS columns
    os_config_col_candidates = df.columns[df.columns.str.contains("OS according to the configuration file", case=False)]
    vmware_os_col_candidates = df.columns[df.columns.str.contains("OS according to the VMware Tools", case=False)]

    if os_config_col_candidates.empty:
        return None, None, None, None, None

    os_config_col = os_config_col_candidates.tolist()[0]

    # Use "OS according to the VMware Tools" if it exists, otherwise fallback to "OS according to the configuration file"
    if not vmware_os_col_candidates.empty:
        vmware_os_col = vmware_os_col_candidates.tolist()[0]
        df['Final OS'] = df[vmware_os_col].where(df[vmware_os_col].notna(), df[os_config_col])
    else:
        df['Final OS'] = df[os_config_col]

    # Identify capacity column (either MiB or MB)
    capacity_col_candidates = df.columns[df.columns.str.contains("Total disk capacity MiB|Total disk capacity MB", case=False, regex=True)]
    if capacity_col_candidates.empty:
        return None, None, None, None, None

    capacity_col = capacity_col_candidates.tolist()[0]

    # Separate VMware Photon OS entries
    photon_df = df[df['Final OS'] == "VMware Photon OS (64-bit)"]
    df = df[df['Final OS'] != "VMware Photon OS (64-bit)"]

    # Exclude rows labeled as templates or placeholders
    df = df[~df['Final OS'].astype(str).str.contains('Template|SRM Placeholder', case=False, na=False)]

    # Convert MiB to MB if necessary
    if "MiB" in capacity_col:
        df[capacity_col] = df[capacity_col] * MIB_TO_MB

    # Filter and count by capacity ranges
    results_by_range = {}
    for min_capacity, max_capacity, label in capacity_ranges:
        filtered_df = df[(df[capacity_col] >= min_capacity) & (df[capacity_col] <= max_capacity)]
        if not filtered_df.empty:
            grouped_result = filtered_df.groupby('Final OS').size().reset_index(name='Count')
            grouped_result['Capacity Range'] = label
            results_by_range[label] = grouped_result

    # Count each OS for the OS Summary tab
    os_summary = df['Final OS'].value_counts().reset_index()
    os_summary.columns = ['Operating System', 'Count']

    # Calculate cluster-level summary statistics
    if 'Cluster' in df.columns:
        cluster_summary = df.groupby('Cluster').agg(
            VM_Count=('Cluster', 'size'),
            Total_CPUs=('CPUs', 'sum'),
            Total_Memory_GB=('Memory', lambda x: x.sum() / MB_TO_GB),
            Total_Disk_Capacity_TB=(capacity_col, lambda x: x.sum() / MB_TO_TB)
        ).reset_index()
    else:
        cluster_summary = pd.DataFrame()

    return results_by_range, cluster_summary, photon_df, os_summary, df

def parallel_process_files(file_paths, capacity_ranges, ignore_powered_off, ignore_patterns, ignore_vm_folder):
    """Process files in parallel, returning the combined OS results for all capacity ranges and cluster statistics."""
    all_results_by_range = {label: [] for _, _, label in capacity_ranges}
    cluster_summaries = []
    photon_dfs = []
    os_summaries = []
    environment_data = pd.DataFrame()  # To store data for Environment tab

    with ProcessPoolExecutor() as executor:
        future_to_file = {
            executor.submit(
                process_file,
                file_path,
                capacity_ranges,
                ignore_powered_off,
                ignore_patterns,
                ignore_vm_folder,
            ): file_path for file_path in file_paths
        }
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing files in parallel"):
            try:
                results = future.result()
                if results is None:
                    print(f"Warning: No results returned for file {future_to_file[future]}")
                    continue
                results_by_range, cluster_summary, photon_df, os_summary, env_data = results
                
                for label, result in results_by_range.items():
                    if result is not None:
                        all_results_by_range[label].append(result)

                if not cluster_summary.empty:
                    cluster_summaries.append(cluster_summary)

                if not photon_df.empty:
                    photon_dfs.append(photon_df)

                if not os_summary.empty:
                    os_summaries.append(os_summary)

                # Collect environment data
                if not env_data.empty:
                    environment_data = pd.concat([environment_data, env_data], ignore_index=True)

            except Exception as exc:
                print(f"File {future_to_file[future]} generated an exception: {exc}")

    # Combine results for each capacity range
    combined_results_by_range = {}
    for label, results in all_results_by_range.items():
        if results:
            combined_df = pd.concat(results, ignore_index=True)
            combined_results_by_range[label] = combined_df.groupby('Final OS').sum().reset_index()
        else:
            combined_results_by_range[label] = pd.DataFrame(columns=["Final OS", "Count"])

    # Combine cluster summaries
    combined_cluster_summary = pd.DataFrame()
    if cluster_summaries:
        combined_cluster_summary = pd.concat(cluster_summaries, ignore_index=True)
        combined_cluster_summary = combined_cluster_summary.groupby('Cluster').sum().reset_index()

    # Combine VMware Photon OS data
    photon_summary = pd.DataFrame()
    if photon_dfs:
        combined_photon_df = pd.concat(photon_dfs, ignore_index=True)
        photon_summary = pd.DataFrame({'Final OS': ["VMware Photon OS (64-bit)"], 'Count': [len(combined_photon_df)]})

    # Combine OS Summary
    combined_os_summary = pd.DataFrame()
    if os_summaries:
        combined_os_summary = pd.concat(os_summaries, ignore_index=True)
        combined_os_summary = combined_os_summary.groupby('Operating System').sum().reset_index()

    return combined_results_by_range, combined_cluster_summary, photon_summary, combined_os_summary, environment_data

def format_os_disk_count_sheet(results_by_range, photon_summary):
    """Format the OS Disk Count sheet with totals for each range and an overall total."""
    full_df = pd.DataFrame()
    overall_total_count = 0

    for label, grouped_result in results_by_range.items():
        if not grouped_result.empty:
            # Set the 'Capacity Range' column for the group only once
            grouped_result = grouped_result.copy()
            grouped_result['Capacity Range'] = label  # Set capacity range once for the group
            group_total = grouped_result['Count'].sum()
            overall_total_count += group_total

            # Add group total row and separate rows for each range
            total_row = pd.DataFrame({'Final OS': ['Group Total'], 'Count': [group_total], 'Capacity Range': ['']})
            separator_row = pd.DataFrame({'Final OS': [''], 'Count': [''], 'Capacity Range': ['']})
            full_df = pd.concat([full_df, grouped_result, total_row, separator_row], ignore_index=True)

    # Append overall total and Photon OS count rows at the end
    overall_total_row = pd.DataFrame({'Final OS': ['Overall Total OS Count'], 'Count': [overall_total_count], 'Capacity Range': ['']})
    photon_count = photon_summary['Count'].iloc[0] if not photon_summary.empty else 0
    photon_row = pd.DataFrame({'Final OS': ['Photon OS Count'], 'Count': [photon_count], 'Capacity Range': ['']})

    # Append the final totals to the main DataFrame
    full_df = pd.concat([full_df, overall_total_row, photon_row], ignore_index=True)
    return full_df

def format_environment_summary(environment_data):
    """Format the Environment tab with each environment's OS breakdown and totals for each environment."""
    full_env_df = pd.DataFrame()  # Initialize the full environment DataFrame
    overall_total_count = 0  # To keep track of the overall total count

    # Group by Environment and Final OS, and count occurrences
    env_summary = environment_data.groupby(['Environment', 'Final OS']).size().reset_index(name='Count')
    
    # Group by environment to get the main environment count summary
    env_total_summary = environment_data['Environment'].value_counts().reset_index()
    env_total_summary.columns = ['Environment', 'Count']

    # Append the environment summary and detailed OS counts
    for environment, total_count in env_total_summary.itertuples(index=False):
        # Add the environment total row
        total_row = pd.DataFrame({'Environment': [environment], 'Count': [total_count], 'Final OS': ['']})
        full_env_df = pd.concat([full_env_df, total_row], ignore_index=True)
        overall_total_count += total_count  # Add to overall total

        # Add each OS breakdown within this environment
        os_breakdown = env_summary[env_summary['Environment'] == environment][['Final OS', 'Count']]
        os_breakdown['Environment'] = ''  # Clear 'Environment' for detailed rows
        full_env_df = pd.concat([full_env_df, os_breakdown], ignore_index=True)

        # Add a separator row after each environment's breakdown
        separator_row = pd.DataFrame({'Environment': [''], 'Count': [''], 'Final OS': ['']})
        full_env_df = pd.concat([full_env_df, separator_row], ignore_index=True)

    # Add the overall total count row at the end
    overall_total_row = pd.DataFrame({'Environment': ['Overall Total'], 'Count': [overall_total_count], 'Final OS': ['']})
    full_env_df = pd.concat([full_env_df, overall_total_row], ignore_index=True)

    return full_env_df[['Environment', 'Final OS', 'Count']]

def adjust_column_widths(writer, dataframe, sheet_name):
    """Adjust column widths based on the length of the data in each column."""
    worksheet = writer.sheets[sheet_name]
    for column in dataframe.columns:
        column_length = max(dataframe[column].astype(str).map(len).max(), len(str(column)))
        col_idx = dataframe.columns.get_loc(column)
        worksheet.set_column(col_idx, col_idx, column_length + 2)

def main():
    parser = argparse.ArgumentParser(
        description="Process Excel files and generate OS disk capacity reports.",
        epilog="Example: python3 disk-groupby-capacity.py -src /path/to/source -dst /path/to/destination -n os_report --ignore-powered-off --ignore-file ignore_patterns.txt --ignore-vm \"Virtual Appliances\""
    )
    
    parser.add_argument('-src', '--source', default='./data', help='Source folder containing Excel files (default: ./data)')
    parser.add_argument('-dst', '--destination', default='./output', help='Destination folder for the output Excel file (default: ./output)')
    parser.add_argument('-name', '--name', default='output_data', help='Name of the output Excel file (default: output_data.xlsx)')
    parser.add_argument('--ignore-powered-off', action='store_true', help='Ignore rows where Powerstate is "poweredOff"')
    parser.add_argument('--ignore-file', help='Path to a file with VM name patterns to ignore')
    parser.add_argument('--ignore-vm', help='Ignore VMs located in specified folders (comma-separated list)')
    parser.add_argument('--group-by', help='Comma-separated list of keywords for grouping by environment based on Cluster column')

    args = parser.parse_args()

    # Check if source and destination directories exist; if not, print help and exit
    if not os.path.isdir(args.source) or not os.path.isdir(args.destination):
        print("Error: The specified source or destination directory does not exist.")
        parser.print_help()
        return

    # Load ignore patterns from file
    ignore_patterns = load_ignore_patterns(args.ignore_file)

    # Ensure output file has .xlsx extension
    output_file_name = args.name if args.name.endswith('.xlsx') else f"{args.name}.xlsx"
    output_file = os.path.join(args.destination, output_file_name)

    # Get the list of Excel files from the source folder
    file_paths = [os.path.join(args.source, f) for f in os.listdir(args.source) if f.endswith('.xlsx')]

    # Define capacity ranges
    capacity_ranges = [
        (0, 149, '0 MB - 149 MB'),  
        (150, 2000000, '150 MB - 2 TB'),
        (2000001, 10000000, '2 TB - 10 TB'),
        (10000001, 20000000, '10 TB - 20 TB'),
        (20000001, 40000000, '20 TB - 40 TB')
    ]

    # Parse group-by patterns if provided
    env_patterns = [pattern.strip().upper() for pattern in args.group_by.split(',')] if args.group_by else []

    # Process files in parallel and gather OS data and cluster statistics
    combined_results_by_range, combined_cluster_summary, photon_summary, combined_os_summary, environment_data = parallel_process_files(
        file_paths, capacity_ranges, args.ignore_powered_off, ignore_patterns, args.ignore_vm
    )

    # Group environment data based on env_patterns
    if not environment_data.empty and env_patterns:
        environment_data['Environment'] = environment_data['Cluster'].apply(
            lambda x: next((pattern for pattern in env_patterns if pattern in x.upper()), 'UNKNOWN')
        )
        env_summary = environment_data.groupby('Environment').size().reset_index(name='Count')
        env_summary = env_summary[env_summary['Count'] > 0]  # Filter out zero counts
    else:
        env_summary = pd.DataFrame(columns=['Environment', 'Count'])

    # Writing to Excel
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        # Write OS_Disk_Count
        os_disk_count_df = format_os_disk_count_sheet(combined_results_by_range, photon_summary)
        os_disk_count_df.to_excel(writer, index=False, sheet_name='OS_Disk_Count')
        adjust_column_widths(writer, os_disk_count_df, 'OS_Disk_Count')

        # Write vCluster VM Count with an aggregated total row
        if not combined_cluster_summary.empty:
            empty_row_cluster = pd.DataFrame({'Cluster': [''], 'VM_Count': [''], 'Total_CPUs': [''], 'Total_Memory_GB': [''], 'Total_Disk_Capacity_TB': ['']})
            total_row = pd.DataFrame({
                'Cluster': ['Total'],
                'VM_Count': [combined_cluster_summary['VM_Count'].sum()],
                'Total_CPUs': [combined_cluster_summary['Total_CPUs'].sum()],
                'Total_Memory_GB': [combined_cluster_summary['Total_Memory_GB'].sum()],
                'Total_Disk_Capacity_TB': [combined_cluster_summary['Total_Disk_Capacity_TB'].sum()]
            })
            combined_cluster_summary = pd.concat([combined_cluster_summary, empty_row_cluster, total_row], ignore_index=True)
            combined_cluster_summary.to_excel(writer, index=False, sheet_name='vCluster VM Count')
            adjust_column_widths(writer, combined_cluster_summary, 'vCluster VM Count')

        # Write OS_Summary
        if not combined_os_summary.empty:
            combined_os_summary.to_excel(writer, index=False, sheet_name='OS_Summary')
            adjust_column_widths(writer, combined_os_summary, 'OS_Summary')

        # Write Environment summary with counts for each OS in each environment
        if not env_summary.empty:
            full_env_summary = format_environment_summary(environment_data)
            full_env_summary.to_excel(writer, index=False, sheet_name='Environment')
            adjust_column_widths(writer, full_env_summary, 'Environment')

    print(f"Combined results saved to {output_file}")

if __name__ == "__main__":
    main()
