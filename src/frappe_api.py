import requests
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Frappe API Configuration
FRAPPE_URL = os.environ.get('FRAPPE_URL', 'https://app.besty.nz/api/resource/Product%20Item')
FRAPPE_API_KEY = os.environ.get('FRAPPE_API_KEY', '32522add18495f4')
FRAPPE_API_SECRET = os.environ.get('FRAPPE_API_SECRET', '45236bb4ab1dcc0')

def get_headers():
    return {
        'Authorization': f'token {FRAPPE_API_KEY}:{FRAPPE_API_SECRET}',
        'Content-Type': 'application/json'
    }

def check_product_exists(product_id):
    try:
        url = f"{FRAPPE_URL}?product_id={product_id}"
        headers = get_headers()
        logging.info(f"Checking if product exists with URL: {url}")
        
        response = requests.get(url, headers=headers)
        
        logging.info(f"Response Status Code: {response.status_code}")
        logging.info(f"Response Content: {response.content}")

        if response.status_code == 200:
            data = response.json()
            if 'data' in data and len(data['data']) > 0:
                logging.info("Product exists.")
                return True, data['data'][0]  # Return the first matching product
            else:
                logging.info("Product does not exist.")
                return False, None
        else:
            logging.error(f"Error checking product existence: {response.status_code} - {response.content}")
            return False, None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return False, None

def update_product(existing_product_id, product):
    try:
        response = requests.put(f"{FRAPPE_URL}/{existing_product_id}", json=product, headers=get_headers())
        response.raise_for_status()
        logging.info(f"Successfully updated product in Frappe: {product['productname']}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to update product {existing_product_id}: {e}")
        logging.error(f"Response content: {response.content if response else 'No response content'}")

def create_product(product):
    try:
        response = requests.post(FRAPPE_URL, json=product, headers=get_headers())
        response.raise_for_status()
        logging.info(f"Successfully created product in Frappe: {product['productname']}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to create product: {e}")
        logging.error(f"Response content: {response.content if response else 'No response content'}")

def test_write_to_frappe(product):
    exists, existing_product = check_product_exists(product['product_id'])
    
    if exists:
        logging.info(f"Product {product['productname']} already exists. Updating...")
        update_product(existing_product['name'], product)
    else:
        logging.info(f"Product {product['productname']} does not exist. Creating new entry...")
        create_product(product)
