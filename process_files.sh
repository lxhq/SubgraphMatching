#!/bin/bash

# Directory containing the input files
prefix="/home/ubuntu/Documents/workspace/SubgraphMatching"
input_dir="$prefix/test/query_graph/pattern_graph_new/"
data_graph="$prefix/test/data_graph/web-spam-new.graph"
output_dir="$prefix/test/outputs/"
executable="$prefix/build/matching/SubgraphMatching.out"
log_file="$prefix/task_log.txt"

# Function to process each file
process_file() {
    input_file="$1"
    input_file_basename="$(basename "$input_file")"
    output_file="$2/$(basename "$input_file")"

    if grep -Fxq "$input_file_basename" "$5"; then
        echo "Skipping already processed file: $input_file_basename"
        return
    fi

    # Run the processing command and save output to the corresponding file
    "$3" \
    -d "$4" \
    -filter GQL -order GQL -engine LFTJ -num MAX \
    -q "$input_file" > "$output_file"

    echo "$input_file_basename" >> "$5"
    echo "Done: "$input_file_basename""
}

export -f process_file  # Export the function for parallel to use

# Find all files in the input directory and process them in parallel
find "$input_dir" -type f | parallel --line-buffer process_file {} "$output_dir" "$executable" "$data_graph" "$log_file"