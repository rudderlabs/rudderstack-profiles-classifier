import json

# Function to extract JSON part from the text file
def extract_json_from_file(file_path):
    with open(file_path, 'r') as file:
        # Flag to indicate when to start capturing JSON
        capture_json = False
        json_lines = []

        # Iterate through each line in the file
        for line in file:
            # If the line ends with "Printing models", set the flag to start capturing JSON from the next line
            if line.strip().endswith('Printing models'):
                capture_json = True
                continue
            
            # If capture flag is set, add the line to json_lines
            if capture_json:
                json_lines.append(line.strip())

        # Join the lines to form the JSON string
        json_string = ''.join(json_lines)

        # Parse JSON
        json_data = json.loads(json_string)

        return json_data

# Path to the text file
file_path = 'output.txt'

# Extract JSON from the file
json_data = extract_json_from_file(file_path)

# Output the dictionary
print(json_data)
