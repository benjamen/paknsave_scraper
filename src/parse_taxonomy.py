import json

# Function to read the taxonomy file and convert it into a hierarchical structure
def build_taxonomy_structure(file_path):
    taxonomy = {}
    
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Process each line in the file
    for line in lines:
        # Strip any leading/trailing whitespace
        line = line.strip()
        
        # Skip the version line (e.g., 'Google_Product_Taxonomy_Version: 2021-09-21')
        if line.startswith("#"):
            continue

        # Split the line into categories
        categories = line.split(' > ')

        # Build the nested dictionary
        current_level = taxonomy
        for category in categories:
            # If the category does not exist, create it
            if category not in current_level:
                current_level[category] = {}
            current_level = current_level[category]
    
    return taxonomy

# Example file path (replace with the actual path to your taxonomy file)
taxonomy_file = 'src/taxonomy.en-US.txt'

# Build the taxonomy structure
google_product_taxonomy = build_taxonomy_structure(taxonomy_file)

# Function to flatten the taxonomy into a list of product categories
def flatten_product_types(hierarchy):
    flat_list = []
    for category, subcategories in hierarchy.items():
        flat_list.append(category)
        flat_list.extend(flatten_product_types(subcategories))  # Recurse for subcategories
    return flat_list

# Get flattened product types
flat_product_types = flatten_product_types(google_product_taxonomy)

# Print flattened categories (optional)
print(flat_product_types)

# Save the flattened categories to a JSON file (optional)
with open('flattened_google_taxonomy.json', 'w') as outfile:
    json.dump(flat_product_types, outfile, indent=4)

print("Flattened taxonomy saved to 'flattened_google_taxonomy.json'.")
