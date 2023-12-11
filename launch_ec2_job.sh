#!/bin/bash

# Check if at least three arguments are provided (zip file, destination, and Python script name)
if [ "$#" -lt 3 ]; then
    echo "Usage: $0 <zipfile> <destination> <python_script> [additional arguments...]"
    exit 1
fi

# Assign arguments to variables
zipfile=$1
destination=$2
python_script=$3

# Remove the first three arguments (zip file, destination, and Python script name)
shift 3

# Check if the zip file exists
if [ ! -f "$zipfile" ]; then
    echo "Error: Zip file '$zipfile' does not exist."
    exit 1
fi


# Remove the destination directory if it exists
if [ -d "$destination" ]; then
    echo "Destination directory '$destination' exists. Removing it."
    rm -rf "$destination"
fi

# Create destination directory
echo "Creating destination directory '$destination'."
mkdir -p "$destination"

# Unzip the file
unzip -q "$zipfile" -d "$destination"

# Change directory to the destination
cd "$destination"

echo "Installing Python requirements."
pip install -r requirements.txt

# Check if the specified Python script exists in the destination directory
if [ ! -f "$python_script" ]; then
    echo "Error: Python script '$python_script' does not exist in the destination directory."
    exit 1
fi

# Run the Python script with forwarded arguments
python3 "$python_script" "$@"

cd ".."
echo "Removing destination directory '$destination'."
rm -rf "$destination"
