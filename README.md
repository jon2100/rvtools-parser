# OS Disk Capacity Report Generator

This script processes Excel files containing OS disk capacity information, groups them by capacity ranges, and generates a detailed report in Excel format (`.xlsx`). The report includes sums of OSes by capacity and highlights any "VMware Photon OS" systems found during the processing.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Command-Line Arguments](#command-line-arguments)
- [Example Usage](#example-usage)
- [Example Output](#example-output)
- [License](#license)

---

### 1. Clone the repository
First, clone the repository to your local machine.   
```bash
git clone git@github.com:jon2100/rvtools-parser.git
cd rvtools-parser
```

### 2. Install dependencies
This script uses Python 3.x and requires some additional libraries for processing Excel files and parallel execution. Install the dependencies using pip.
```bash
pip install pandas openpyxl tqdm
```
* pandas: For data manipulation and Excel file processing.
* openpyxl: For saving the output in .xlsx format and adjusting Excel-specific properties (e.g., column width).
* tqdm: For displaying progress bars when processing files.

## Usage
### Command-Line Arguments  
```
-src / --source: Specify the source directory containing the Excel files you want to process. (Default: ./data)

-dst / --destination: Specify the destination directory where the output file should be saved. (Default: ./output)  

-name / --name: The name of the output Excel file without an extension. (Default: output)
```

### Example Command
```bash
python3 disk-groupby-capacity.py -src /path/to/source -dst /path/to/destination -name os_report
```
This will generate an Excel report named os_report.xlsx in the destination directory.

## Example Output
When running the script, you will see progress updates in the terminal as the files are processed:
```bash
Processing files in parallel: 100%|████████████████████████████████████████| 10/10 [00:10<00:00,  1.02s/it]
Combined results including VMware Photon OS saved to /path/to/destination/os_report.xlsx
```
### Output File: ```os_report.xlsx```  
The output file will contain the following columns:
1. OS according to the configuration file: Lists the OS as identified in the configuration file.
2. Count: Number of occurrences of the OS in the given capacity range.
3. Capacity Range: Shows the capacity range for the OSes (e.g., 150 MB - 2 TB).
4. OS according to the VMware Tools: Only populated if VMware Photon OS (64-bit) is present, showing occurrences of Photon OS.

### Example Output Data
```markdown
| OS according to the configuration file | Count | Capacity Range    | OS according to the VMware Tools |
|----------------------------------------|-------|-------------------|----------------------------------|
| Microsoft Windows Server 2016 (64-bit) |  10	 |  150 MB - 2 TB    |                                  |
|----------------------------------------|-------|-------------------|----------------------------------|
|----------------------------------------|-------|-------------------|----------------------------------|
|Disk OS Sum	                         |  10	 |	             |                                  |
|----------------------------------------|-------|-------------------|----------------------------------|
|----------------------------------------|-------|-------------------|----------------------------------|
|Red Hat Enterprise Linux 7 (64-bit)	 |   7	 |   2 TB - 10 TB    |                                  |
|----------------------------------------|-------|-------------------|----------------------------------|
|Disk OS Sum	                         |   7	 | 	              |                                  |
|----------------------------------------|-------|-------------------|----------------------------------|
|----------------------------------------|-------|-------------------|----------------------------------|
|Total Machine Count	                 |  17	 |                   |                                  |
|----------------------------------------|-------|-------------------|----------------------------------|
|----------------------------------------|-------|-------------------|----------------------------------|
|VMware Photon OS (64-bit)	          |   5	 |  All Capacities	 |   VMware Photon OS (64-bit)      |
|----------------------------------------|-------|-------------------|----------------------------------|
|----------------------------------------|-------|-------------------|----------------------------------|
|Disk OS Sum	                         |   5   |  All Capacities   |                                  |
|----------------------------------------|-------|-------------------|----------------------------------|
```
---
## License
This project is licensed under the GNU General Public License v3.0. You may copy, distribute, and modify the software under the terms of the GNU GPL as published by the Free Software Foundation.

