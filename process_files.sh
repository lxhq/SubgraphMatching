#!/bin/bash

# Directory containing the input files
input_dir="/home/ubuntu/Documents/workspace/SubgraphMatching/test/query_graph/pattern_graph_new/6voc/"

# Function to process each file
process_file() {
    input_file="$1"
    output_file="/home/ubuntu/Documents/workspace/SubgraphMatching/test/outputs/6voc/$(basename "$input_file")"

    # Check if output file already exists
    if [ -f "$output_file" ]; then
        return
    fi

    echo "Processing $input_file"

    # Run the processing command and save output to the corresponding file
    /home/ubuntu/Documents/workspace/SubgraphMatching/build/matching/SubgraphMatching.out \
    -d ~/Documents/workspace/SubgraphMatching/test/data_graph/web-spam-new.graph \
    -filter GQL -order GQL -engine LFTJ -num MAX \
    -q "$input_file" > "$output_file"
}

export -f process_file  # Export the function for parallel to use

# Find all files in the input directory and process them in parallel
find "$input_dir" -type f | parallel --line-buffer process_file {}
