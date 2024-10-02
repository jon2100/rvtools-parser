# vCluster Count Tool

This tool processes Excel files containing vCluster information and generates an Excel report with detailed summaries. It aggregates VM counts, CPU totals, memory, and disk space usage for vClusters, providing an option to group data by country using a mapping file.

## Features
- Aggregates VM counts and hardware totals (CPUs, Memory, Disk) for vClusters.
- Optionally groups data by country using a mapping file.
- Generates a detailed Excel report with totals, groupings, and formatted data.
- Automatically adjusts column widths and alignment in the output Excel file.

## Installation

1. **Clone the Repository:**
    ```bash
    git clone https://github.com/your-username/vcluster-count-tool.git
    ```
2. **Navigate to the Project Directory:**
    ```bash
    cd vcluster-count-tool
    ```
3. **Install the Required Python Packages:**
    ```bash
    pip install pandas openpyxl argparse
    ```

## Requirements
- **Python Version:** Python 3.6 or later
- **Required Python Packages:** 
  - `pandas` (for data processing)
  - `openpyxl` (for reading and writing Excel files)
  - `argparse` (for command-line argument parsing)
- **Input Files:** Excel files containing vCluster data with the necessary columns:
  - `'Cluster'`, `'VM'`, `'VI SDK Server'`, `'CPUs'`, `'Memory'`, `'Total disk capacity MiB'`, `'OS according to the configuration file'`, `'OS according to the VMware Tools'`.

## Usage

Run the script using the command line:
```bash
python vcluster-count.py -s /path/to/source/directory -d /path/to/output/directory -n output_filename.xlsx
```

### Command-line Arguments
```text
-s, --src            : Source directory containing vCluster Excel files. Default: ./data
-m, --mapping        : (Optional) Path to the Excel file containing Country, vCenter, and vCluster mapping.
-ms, --mapping-sheet : (Optional) Sheet name of the mapping data in the Excel file. Default: vClusterLoc
-d, --dst            : Destination directory for the output file. Default: ./output
-n, --name           : Output file name. Default: output.xlsx
```

### Example Commands

1. **With Mapping File:**
    ```bash
    python vcluster-count.py -s ./data -m ./mapping.xlsx -ms vClusterLoc -d ./output -n vcluster_report.xlsx
    ```

2. **Without Mapping File:**
    ```bash
    python vcluster-count.py -s ./data -d ./output -n vcluster_report.xlsx
    ```

## Output

The script generates an Excel file containing two sheets:
- **vCluster Summary**: A summary of the vClusters with VM counts, CPU totals, memory, and disk space.
- **Standalone_VMs**: A listing of standalone VMs.

### Output Explanation
1. **vCluster Summary:** Includes columns for:
   - Cluster name
   - Total VMs
   - Total CPUs
   - Memory usage in GB
   - Disk capacity in TB
2. **Standalone_VMs:** Lists all VMs that are not part of any cluster.

## Notes
- The source Excel files must contain the following columns:
  - `'Cluster'`, `'VM'`, `'VI SDK Server'`, `'CPUs'`, `'Memory'`, `'Total disk capacity MiB'`, `'OS according to the configuration file'`, `'OS according to the VMware Tools'`.
- If a mapping file is provided, it should include the columns:
  - `'Country'`, `'vCenter'`, `'vCluster'`.
- The script filters out rows containing `'Template'` or `'SRM Placeholder'` in the source data to focus on active VMs.
- The output file is saved in the specified output directory with the name you provide.

## Troubleshooting
- Ensure that the input Excel files are formatted correctly and contain all the necessary columns.
- If the script fails, verify that the specified paths for source and mapping files are correct.
- If you encounter permission issues, try running the script with elevated privileges.

## License

This project is licensed under the BSD License. See the LICENSE file for details.

## Contributing
If you would like to contribute to this tool, please fork the repository and submit a pull request.