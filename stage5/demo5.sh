#!/bin/bash

# Get the absolute path of the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Set the output file path
OUTPUT_FILE="${SCRIPT_DIR}/output.txt"

# Create output.txt file
touch "${OUTPUT_FILE}"

# Run the Java program with the output file path as an argument
java -cp target/classes edu.yu.cs.com3800.stage5.ZKPSInstanceManagerForDemo "${OUTPUT_FILE}"

# Display a message indicating the completion of the script
echo "Script completed"
