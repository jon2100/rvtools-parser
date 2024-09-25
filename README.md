# rvtools-parser for disk usage
This parser will OS by "OS ass configuration file" and Disk by "Capacity Range" which is pulled from label Total disk capacity MiB from the rvtools export.  The output is grouped into the following:
```
0 MB - 150 MB
150 MB - 2 TB
2 TB - 10 TB
10 TB - 20 TB
20 TB - 40 TB
```

This tool converts the MiB to MB.

Parse data to produce a csv which has OS type to Disk size grouped by size 
usage: checkme3.py [-h] [-s SRC] [-d DST] [-n NAME]

Process Excel files and generate OS disk capacity reports.

options:
  -h, --help            show this help message and exit
  -s SRC, --src SRC     Source folder containing Excel files (default: ./data)
  -d DST, --dst DST     Destination folder for the output file (default:
                        ./output)
  -n NAME, --name NAME  Base name of the output file without extension
                        (default: output)
usage: checkme3.py [-h]
                   [--capacity-ranges CAPACITY_RANGES [CAPACITY_RANGES ...]]
                   file_paths [file_paths ...]

Process Excel files in parallel

positional arguments:
  file_paths            Paths to Excel files

options:
  -h, --help            show this help message and exit
  --capacity-ranges CAPACITY_RANGES [CAPACITY_RANGES ...]
                        Capacity ranges in format "min:max:label"
