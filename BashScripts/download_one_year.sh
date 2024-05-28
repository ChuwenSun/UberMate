#!/bin/bash

# Prompt the user for the year
read -p "Enter the year (e.g., 2023): " year

# Define the base URL
base_url="https://d37ci6vzurychx.cloudfront.net/trip-data"

# Define the types of trip data
declare -a trip_types=("yellow" "green" "fhvhv")

# Define the directories
temp_directory="/usr/local/nifi/temp"
data_in_directory="/usr/local/nifi/data-in"

# Create the temporary and data-in directories
mkdir -p "$temp_directory"
mkdir -p "$data_in_directory"

# Loop over each month
for month in {01..12}; do
    # Download the files for each trip type
    for trip_type in "${trip_types[@]}"; do
        # Construct the file name
        file_name="${trip_type}_tripdata_${year}-${month}.parquet"

        # Construct the full URL
        full_url="${base_url}/${file_name}"
        
        # Download the file to the temporary directory
        wget -c "${full_url}" --no-check-certificate -O "${temp_directory}/${file_name}"

        # Check if the file has been downloaded successfully
        if [ ! -f "${temp_directory}/${file_name}" ]; then
            echo "Failed to download ${file_name}. It might not exist at the specified URL."
        else
            actual_size=$(wc -c <"${temp_directory}/${file_name}")
            # If the file size is less than 1000 bytes, we assume it's an error page and delete it
            if [ $actual_size -lt 1000 ]; then
                echo "Removing incomplete file: ${file_name}"
                rm "${temp_directory}/${file_name}"
            else
                echo "Downloaded ${file_name} successfully to temporary directory."
            fi
        fi
    done
done

# Move downloaded files to the data-in directory
echo "Moving downloaded files to ${data_in_directory}."
mv "${temp_directory}"/* "${data_in_directory}"

echo "Cleaning up: Removing temporary directory."
rm -r "$temp_directory"
echo "Download and move process complete for the year $year."
