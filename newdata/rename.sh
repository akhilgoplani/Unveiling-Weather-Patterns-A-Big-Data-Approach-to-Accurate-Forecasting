#!/bin/bash

# Check if the number of arguments provided is correct
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <parent_folder_path>"
    exit 1
fi

parent_folder="$1"

# Check if the provided path is a directory
if [ ! -d "$parent_folder" ]; then
    echo "$parent_folder is not a valid directory."
    exit 1
fi

# Loop through each folder in the parent folder
for folder in "$parent_folder"/*/; do
    city_name=$(basename -- "$folder")  # Extract the city name from the folder name
    
    # Loop through each .csv file in the folder
    for file in "$folder"/*.csv; do
        if [ -f "$file" ]; then  # Check if the item is a file
            # Rename the file with the city name
            mv "$file" "$folder/$city_name.csv"
        fi
    done
done

