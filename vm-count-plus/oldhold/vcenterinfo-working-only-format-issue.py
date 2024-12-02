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

def process_file(file_path, capacity_ranges, ignore_powered_off, ignore_patterns, ignore_vm_list, group_by_patterns):
    """Process a single Excel file and return OS counts for all capacity ranges and cluster statistics."""
    df = pd.read_excel(file_path)

    # Filter out powered-off VMs if requested
    if ignore_powered_off and 'Powerstate' in df.columns:
        df = df[df['Powerstate'] != 'poweredOff']

    # Apply ignore patterns from ignore file to VM Name column
    if 'Name' in df.columns and ignore_patterns:
        regex_pattern = '|'.join(ignore_patterns)
        df = df[~df['Name'].astype(str).str.contains(regex_pattern, case=False, na=False)]

    # Apply --ignore-vm filter to Cluster or Folder columns
    if ignore_vm_list:
        cluster_filter = df['Cluster'].astype(str).str.contains('|'.join(ignore_vm_list), case=False, na=False) if 'Cluster' in df.columns else pd.Series([False] * len(df))
        folder_filter = df['Folder'].astype(str).str.contains('|'.join(ignore_vm_list), case=False, na=False) if 'Folder' in df.columns else pd.Series([False] * len(df))
        df = df[~(cluster_filter | folder_filter)]

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

    # Calculate environment grouping based on `Cluster` and `--group-by` list
    env_summary = pd.DataFrame()
    if group_by_patterns:
        for env in group_by_patterns:
            env_df = df[df['Cluster'].str.contains(env, case=False, na=False)]
            env_count = env_df['Final OS'].value_counts().reset_index()
            env_count.columns = ['Operating System', 'Count']
            env_count['Environment'] = env.upper()

            # Add total row and an empty row after each environment group
            total_count = env_count['Count'].sum()
            total_row = pd.DataFrame({'Operating System': ['Total'], 'Count': [total_count], 'Environment': [env.upper()]})
            empty_row = pd.DataFrame({'Operating System': [''], 'Count': [''], 'Environment': ['']})
            env_count = pd.concat([env_count, total_row, empty_row], ignore_index=True)

            env_summary = pd.concat([env_summary, env_count], ignore_index=True)

    # Calculate cluster-level summary statistics
    cluster_summary = pd.DataFrame()
    if 'Cluster' in df.columns:
        cluster_summary = df.groupby('Cluster').agg(
            VM_Count=('Cluster', 'size'),
            Total_CPUs=('CPUs', 'sum'),
            Total_Memory_GB=('Memory', lambda x: x.sum() / MB_TO_GB),
            Total_Disk_Capacity_TB=(capacity_col, lambda x: x.sum() / MB_TO_TB)
        ).reset_index()

    return results_by_range, cluster_summary, photon_df, os_summary, env_summary

def parallel_process_files(file_paths, capacity_ranges, ignore_powered_off, ignore_patterns, ignore_vm_list, group_by_patterns):
    """Process files in parallel, returning the combined OS results for all capacity ranges and cluster statistics."""
    all_results_by_range = {label: [] for _, _, label in capacity_ranges}
    cluster_summaries = []
    photon_dfs = []
    os_summaries = []
    env_summaries = []

    with ProcessPoolExecutor() as executor:
        future_to_file = {
            executor.submit(
                process_file,
                file_path,
                capacity_ranges,
                ignore_powered_off,
                ignore_patterns,
                ignore_vm_list,
                group_by_patterns,
            ): file_path for file_path in file_paths
        }
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing files in parallel"):
            try:
                results = future.result()
                if results is None:
                    print(f"Warning: No results returned for file {future_to_file[future]}")
                    continue
                results_by_range, cluster_summary, photon_df, os_summary, env_summary = results
                
                for label, result in results_by_range.items():
                    if result is not None:
                        all_results_by_range[label].append(result)

                if not cluster_summary.empty:
                    cluster_summaries.append(cluster_summary)

                if not photon_df.empty:
                    photon_dfs.append(photon_df)

                if not os_summary.empty:
                    os_summaries.append(os_summary)

                if not env_summary.empty:
                    env_summaries.append(env_summary)

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

    # Combine Environment Summary
    combined_env_summary = pd.DataFrame()
    if env_summaries:
        combined_env_summary = pd.concat(env_summaries, ignore_index=True)

    return combined_results_by_range, combined_cluster_summary, photon_summary, combined_os_summary, combined_env_summary

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
    parser.add_argument('--ignore-vm', help='Comma-separated list of patterns to ignore in Cluster/Folder columns')
    parser.add_argument('--group-by', help='Comma-separated list of environment keywords to group by in Cluster column')

    args = parser.parse_args()

    # Check if source and destination directories exist; if not, print help and exit
    if not os.path.isdir(args.source) or not os.path.isdir(args.destination):
        print("Error: The specified source or destination directory does not exist.")
        parser.print_help()
        return

    # Load ignore patterns from file
    ignore_patterns = load_ignore_patterns(args.ignore_file)
    ignore_vm_list = args.ignore_vm.split(",") if args.ignore_vm else []
    group_by_patterns = args.group_by.split(",") if args.group_by else []

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

    # Process files in parallel and gather OS data and cluster statistics
    combined_results_by_range, combined_cluster_summary, photon_summary, combined_os_summary, combined_env_summary = parallel_process_files(
        file_paths, capacity_ranges, args.ignore_powered_off, ignore_patterns, ignore_vm_list, group_by_patterns
    )

    combined_results = pd.DataFrame()
    for label, os_summary in combined_results_by_range.items():
        if not os_summary.empty:
            os_summary['Capacity Range'] = label
            combined_results = pd.concat([combined_results, os_summary], ignore_index=True)

    # Add VMware Photon OS summary to OS Disk Count
    combined_results = pd.concat([combined_results, pd.DataFrame({
        'Final OS': ['VMware Photon OS (64-bit)'],
        'Count': [photon_summary['Count'].sum()],
        'Capacity Range': ['']
    })], ignore_index=True)

    # Output the combined result to a single Excel file
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        combined_results.to_excel(writer, index=False, sheet_name='OS_Disk_Count')
        combined_cluster_summary.to_excel(writer, index=False, sheet_name='vCluster VM Count')
        combined_os_summary.to_excel(writer, index=False, sheet_name='OS_Summary')
        
        if not combined_env_summary.empty:
            combined_env_summary.to_excel(writer, index=False, sheet_name='Environment_Summary')

    print(f"Output successfully saved to {output_file}")

if __name__ == "__main__":
    main()
