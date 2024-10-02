# OS Disk Capacity Report Script

This script processes Excel files to generate OS disk capacity reports. It identifies different VMs, groups them by disk capacity ranges, and includes orphaned VMs and VMware Photon OS in a summary report saved as an Excel file.

## Requirements

To run this script, you need the following components installed:

1. **Python 3.x**: Ensure you have Python 3 installed on your system.
2. **Pandas**: Python library for data manipulation and analysis.
3. **XlsxWriter**: Python module for creating Excel files.
4. **Openpyxl**: For reading Excel files.
5. **tqdm**: For showing progress bars when processing files.
6. **argparse**: Included in the standard library of Python, for handling command-line arguments.
7. **concurrent.futures**: Included in the standard library of Python, for parallel processing.

### Install Required Packages

Use `pip` to install the necessary packages:

```bash
pip install pandas xlsxwriter openpyxl tqdm
```

## How to Run the Script

The script is designed to be run from the command line. Here are the steps to execute the script:

1. **Clone or Download the Script**: Download the script file (`os_disk_capacity_report.py`) to a directory on your local machine.

2. **Prepare the Data**: Ensure that your input Excel files are in a directory (default is \`./data\`).

3. **Open a Terminal**: Navigate to the directory where you saved the script using the terminal or command prompt.

4. **Run the Script**: Execute the script using Python and provide the necessary command-line arguments.

### Command-Line Arguments

The script accepts the following optional command-line arguments:

- \`-s\` or \`--src\`: The source folder containing Excel files. Defaults to \`./data\`.
- \`-d\` or \`--dst\`: The destination folder where the output file will be saved. Defaults to \`./output\`.
- \`-n\` or \`--name\`: The base name for the output file. The extension will automatically be \`.xlsx\`. Defaults to \`output\`.

### Example Usage

```bash
python os_disk_capacity_report.py -s /path/to/data -d /path/to/output -n my_report
```

This command will:
- Read Excel files from \`/path/to/data\`.
- Write the output file named \`my_report.xlsx\` to \`/path/to/output\`.

### Default Usage

If you don't specify any arguments, the script will use the default values:

```bash
python os_disk_capacity_report.py
```

This command will:
- Read Excel files from the \`./data\` directory.
- Write the output file named \`output.xlsx\` to the \`./output\` directory.

## Output

The script generates an Excel file with the following details:
- A worksheet named \`OS_Disk_Count\`.
- Summary of VMs grouped by disk capacity ranges.
- Lists orphaned VMs and VMware Photon OS separately.

## Notes

- Ensure the source directory contains Excel files with the required columns: "OS according to the configuration file," "Connection state," and "Total disk capacity MiB/MB."
- The script will create the destination directory if it doesn't exist.
