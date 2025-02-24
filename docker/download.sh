#!/bin/bash
# Shebang line that specify things below should be executed with Bash

# Download zip fils
echo "Downloading files "
wget https://pages.cs.wisc.edu/~harter/cs544/data/wi2021.csv.gz || echo "failed to download wi2021.csv.gz"
wget https://pages.cs.wisc.edu/~harter/cs544/data/wi2022.csv.gz || echo "failed to downloa
d wi2022.csv.gz"
wget https://pages.cs.wisc.edu/~harter/cs544/data/wi2023.csv.gz || echo "failed to downloa
d wi2023.csv.gz"

# decompress zip files: help from man gzip
echo "unzip files "
gzip -d wi2021.csv.gz || echo "failed to unzip wi2021.csv.gz"
gzip -d wi2022.csv.gz || echo "failed to unzip win2022.csv.gz"
gzip -d wi2023.csv.gz || echo "failed to unzip win2024.csv.gz"

#contents will be in wi.txt
cat wi2021.csv wi2022.csv wi2023.csv > wi.txt || echo "files can't be stored" 
