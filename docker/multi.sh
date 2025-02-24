#!/bin/bash

echo "generate wi.txt from dowload.sh "
./download.sh


#counts the number of lines in wi.txt containing the text "Multifamily" in any case
echo "counting how many lines contain the text 'Multifamily' - ignore the case"
count=$(grep -i "Multifamily" wi.txt | wc -l)

echo $count