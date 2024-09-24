#!/usr/bin/env python3

import pandas as pd
import os
import argparse
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.chart import PieChart, Reference
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Conversion factor for MiB to MB
MIB_TO_MB = 1.048576

def process_file(file_path, os_filters, capacity_ranges):
    """Process a single Excel file and return OS counts for all capacity ranges and VMware Photon OS counts."""
    df = pd.read_excel(file_path)
    
    os_col_candidates = df.columns[df.columns.str.contains("OS according to the configuration file", case=False)]
    if os_col_candidates.empty:
        return None, None, None

    os_col = os_col_candidates.tolist()[0]

    capacity_col_candidates = df.columns[df.columns.str.contains("Total disk capacity MiB|Total disk capacity MB", case=False, regex=True)]
    if capacity_col_candidates.empty:
        return None, None, None

    capacity_col = capacity_col_candidates.tolist()[0]

    if 'Template' in df.columns and 'SRM Placeholder' in df.columns:
        df = df[(df['Template'] != True) & (df['SRM Placeholder'] != True)]

    df = df[~df[os_col].astype(str).str.contains('Template|SRM Placeholder|AdditionalBackEnd', case=False, na=False)]

    if "MiB" in capacity_col:
        df[capacity_col] = df[capacity_col] * MIB_TO_MB

    vmware_os_col_candidates = df.columns[df.columns.str.contains("OS according to the VMware Tools", case=False)]
    photon_result = None
    if not vmware_os_col_candidates.empty:
        vmware_os_col = vmware_os_col_candidates.tolist()[0]
        photon_result = df[df[vmware_os_col] == "VMware Photon OS (64-bit)"]

    results_by_range = {}
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

    return results_by_range, photon_result, df[os_col].dropna().unique()

def parallel_process_files(file_paths, capacity_ranges):
    all_results_by_range = {label: [] for _, _, label in capacity_ranges}
    photon_results = []
    unique_os_filters = set()

    with ProcessPoolExecutor() as executor:
        future_to_file = {executor.submit(process_file, file_path, [], capacity_ranges): file_path for file_path in file_paths}
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

    combined_results_by_range = {}
    for label, results in all_results_by_range.items():
        if results:
            combined_df = pd.concat(results, ignore_index=True)
            combined_results_by_range[label] = combined_df.groupby(combined_df.columns[0]).sum().reset_index()
        else:
            combined_results_by_range[label] = pd.DataFrame(columns=["OS according to the configuration file", "Count"])

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

def create_pivot_table(df):
    filtered_df = df[~df['OS according to the configuration file'].isin(['Total Machine Count', 'VMware Photon OS (64-bit)'])]
    disk_os_sum_df = filtered_df[filtered_df['OS according to the configuration file'] == 'Disk OS Sum']
    total_disk_os_sum = disk_os_sum_df['Count'].sum()

    disk_os_sum_df.loc[:, 'Percentage'] = (disk_os_sum_df['Count'] / total_disk_os_sum) * 100  # Fixed the SettingWithCopyWarning by using .loc[]
    pivot_table = disk_os_sum_df[['Capacity Range', 'Count', 'Percentage']]
    
    return pivot_table

def add_pie_chart(ws, start_row, title, labels_col, values_col, data_len):
    chart = PieChart()
    chart.title = title
    labels_ref = Reference(ws, min_col=labels_col, min_row=start_row, max_row=start_row + data_len - 1)
    data_ref = Reference(ws, min_col=values_col, min_row=start_row, max_row=start_row + data_len - 1)
    chart.add_data(data_ref, titles_from_data=True)
    chart.set_categories(labels_ref)
    ws.add_chart(chart, f"E{start_row}")

def save_with_charts(df, pivot_table, output_file):
    wb = Workbook()
    ws = wb.active
    ws.title = "Disk OS Data"

    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)

    ws.append([])  # Blank row for separation

    # First Pie Chart for "Disk OS Sum"
    disk_os_sum_data_len = len(df[df['OS according to the configuration file'] == 'Disk OS Sum'])
    add_pie_chart(ws, len(df) + 3, "OS Disk Sum for All OSes", labels_col=1, values_col=2, data_len=disk_os_sum_data_len)

    ws.append(['Pivot Table - OS Disk Sum Percentage'])
    for r in dataframe_to_rows(pivot_table, index=False, header=True):
        ws.append(r)

    # Second Pie Chart for Capacity Ranges
    pivot_data_len = len(pivot_table)
    pivot_start_row = len(df) + len(pivot_table) + 5
    add_pie_chart(ws, pivot_start_row, "Capacity Range Percentages", labels_col=1, values_col=2, data_len=pivot_data_len)

    wb.save(output_file)

def main():
    parser = argparse.ArgumentParser(description="Process Excel files and generate OS disk capacity reports.")
    parser.add_argument('-src', '--source', default='./data', help='Source folder containing Excel files (default: ./data)')
    parser.add_argument('-dst', '--destination', default='./output', help='Destination folder for the output Excel file (default: ./output)')
    parser.add_argument('-name', '--name', default='output.xlsx', help='Name of the output Excel file (default: output.xlsx)')
    args = parser.parse_args()

    src_folder = os.path.abspath(args.source)
    dst_folder = os.path.abspath(args.destination)

    file_paths = [os.path.join(src_folder, f) for f in os.listdir(src_folder) if f.endswith('.xlsx')]

    capacity_ranges = [
        (0, 149, '0 MB - 149 MB'),
        (150, 2000000, '150 MB - 2 TB'),
        (2000001, 10000000, '2 TB - 10 TB'),
        (10000001, 20000000, '10 TB - 20 TB'),
        (20000001, 40000000, '20 TB - 40 TB')
    ]

    os.makedirs(dst_folder, exist_ok=True)

    combined_results_by_range, photon_combined, unique_os_filters = parallel_process_files(file_paths, capacity_ranges)

    combined_results = pd.DataFrame()
    for label, os_summary in combined_results_by_range.items():
        if not os_summary.empty:
            os_summary['Capacity Range'] = label
            os_summary_with_sum = insert_break_and_sum(os_summary)
            combined_results = pd.concat([combined_results, os_summary_with_sum], ignore_index=True)

    if 'OS according to the configuration file' in combined_results.columns:
        total_machine_count = combined_results[combined_results['OS according to the configuration file'] == 'Disk OS Sum']['Count'].sum()
        total_row = pd.DataFrame({'OS according to the configuration file': ['Total Machine Count'], 'Count': [total_machine_count], 'Capacity Range': ['']})
        combined_results = pd.concat([combined_results, total_row], ignore_index=True)

        blank_row = pd.DataFrame({'OS according to the configuration file': [''], 'Count': [''], 'Capacity Range': [''], 'OS according to the VMware Tools': ['']})
        combined_results = pd.concat([combined_results, blank_row], ignore_index=True)

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

        blank_row_after_photon = pd.DataFrame({
            'OS according to the configuration file': [''],
            'Count': [''],
            'Capacity Range': [''],
            'OS according to the VMware Tools': ['']
        })
        combined_results = pd.concat([combined_results, blank_row_after_photon], ignore_index=True)

    if 'OS according to the configuration file' in combined_results.columns and 'OS according to the VMware Tools' in combined_results.columns:
        columns_order = ['OS according to the configuration file', 'Count', 'Capacity Range', 'OS according to the VMware Tools']
        combined_results = combined_results[columns_order]

    pivot_table = create_pivot_table(combined_results)
    output_file = os.path.join(dst_folder, args.name)
    save_with_charts(combined_results, pivot_table, output_file)

    print(f"Combined results and charts saved to {output_file}")

if __name__ == "__main__":
    main()
