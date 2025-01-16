import logging
import re
import os
import json
import random
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from playwright.async_api import async_playwright
import requests
from frappe_api import test_write_to_frappe

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class ScraperConfig:
    """Configuration settings for the scraper."""
    base_url: str
    page_load_delay: int = 7
    product_log_delay: float = 0.02
    max_retries: int = 3

class PaknSaveScraper:
    """PaknSave specific scraper implementation."""

    def __init__(self, config: ScraperConfig):
        self.config = config
        self.all_products = []

    async def safe_get(self, page, url: str) -> bool:
        """Safely navigate to a URL with retries."""
        for attempt in range(self.config.max_retries):
            try:
                await page.goto(url, wait_until="domcontentloaded")
                await asyncio.sleep(self.config.page_load_delay)
                return True
            except Exception as e:
                logging.error(f"Error accessing {url} (attempt {attempt + 1}): {e}")
                await asyncio.sleep(self.config.page_load_delay)
        return False

    async def fetch_product_details(self, page, product_url: str) -> Dict:
        """Fetch additional details from individual product page."""
        details = {}
        try:
            await self.safe_get(page, product_url)

            description_elem = await page.query_selector("div.fs-product-details__description")
            if description_elem:
                details['description'] = await description_elem.inner_text()

            nutrition_table = await page.query_selector("table.fs-nutritional-info")
            if nutrition_table:
                nutrition_data = {}
                rows = await nutrition_table.query_selector_all("tr")
                for row in rows:
                    cols = await row.query_selector_all("td")
                    if len(cols) >= 2:
                        key = await cols[0].inner_text()
                        value = await cols[1].inner_text()
                        nutrition_data[key] = value
                details['nutritionalInfo'] = nutrition_data

            ingredients_elem = await page.query_selector("div.fs-product-details__ingredients")
            if ingredients_elem:
                details['ingredients'] = await ingredients_elem.inner_text()

            logging.info(f"Successfully fetched product details from {product_url}")
            return details

        except Exception as e:
            logging.error(f"Error fetching product details from {product_url}: {e}")
            return details


    async def fetch_categories(self, page) -> List[Dict[str, str]]:
        """Fetch all available categories from the website."""
        try:
            await self.safe_get(page, 'https://www.paknsave.co.nz/shop/category/fresh-foods-and-bakery?pg=1')

            # Close the tooltip if it appears
            try:
                tooltip_close_button = await page.query_selector('button._19kx3s2')
                if tooltip_close_button:
                    await tooltip_close_button.click()
                    logging.info("Closed the tooltip")
            except Exception as e:
                logging.warning(f"Tooltip not found or unable to close it: {e}")

            logging.info("Waiting for menu panel to load...")
            groceries_button = await page.query_selector('//span[contains(text(), "Groceries")]/..')
            if groceries_button:
                await groceries_button.click()
                logging.info("Clicked on the 'Groceries' menu item")
                await asyncio.sleep(2)

            categories = []
            category_elements = await page.query_selector_all('button._177qnsx7')
            for element in category_elements:
                category_name = await element.inner_text()
                if category_name.lower() in ["featured", "all null"]:
                    continue
                url_name = re.sub(r'[^a-zA-Z0-9 ]', '', category_name.lower()).replace(" & ", "-and-").replace(" ", "-")
                url_name = url_name.replace("--", "-and-")
                category_url = f"{self.config.base_url}/shop/category/{url_name}?pg=1"
                categories.append({"name": category_name, "url": category_url})
                logging.info(f"Found category: {category_name} - URL: {category_url}")

            logging.info(f"Successfully fetched {len(categories)} categories")
            return categories

        except Exception as e:
            logging.error(f"Error in fetch_categories: {e}", exc_info=True)
            return []


    async def save_progress(self):
        """Save current progress to a temporary file."""
        temp_filename = f"paknsave_products_temp_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
        try:
            with open(temp_filename, 'w') as f:
                json.dump(self.all_products, f, indent=4)
            logging.info(f"Progress saved to {temp_filename}")
        except Exception as e:
            logging.error(f"Error saving progress: {e}")

 
    async def scrape_products(self, page, category_url: str) -> List[Dict]:
        """Scrape products from the given category URL."""
        try:
            await self.safe_get(page, category_url)
            await asyncio.sleep(self.config.page_load_delay)

            products = []
            
            while True:
                await asyncio.sleep(random.uniform(2, 5))  # Random delay
                product_elements = await page.query_selector_all('div[data-testid$="-EA-000"]')

                for element in product_elements:
                    product_data = await self.extract_product_data(element)
                    if product_data:
                        # Transform product data to match Frappe schema
                        frappe_product = self.transform_to_frappe_format(product_data)
                        
                        try:
                            # Write to Frappe
                            test_write_to_frappe(frappe_product)
                            logging.info(f"Successfully sent product to Frappe: {frappe_product['productname']}")
                            
                            # Add to products list after successful Frappe write
                            products.append(product_data)
                            logging.info(f"Scraped product: {product_data.get('name')}")
                        except Exception as e:
                            logging.error(f"Error writing product to Frappe: {e}")
                    else:
                        logging.warning("Product data extraction returned None.")

                next_page = await page.query_selector('a[data-testid="pagination-increment"]')
                if next_page:
                    await next_page.click()
                    await page.wait_for_load_state('networkidle')
                else:
                    break

            logging.info(f"Total products scraped: {len(products)}")
            return products
            
        except Exception as e:
            logging.error(f"Error scraping products: {e}")
            return []

    def transform_to_frappe_format(self, product: Dict) -> Dict:
        """Transform scraped product data to Frappe format."""
        
        # Extract price value from string and convert to float
        price_str = product.get('price', '0.00')
        try:
            current_price = float(price_str)
        except ValueError:
            current_price = 0.00

        # Extract the product ID and other fields
        product_id = product.get("product_id")
        product_name = product.get("name")
        category = product.get("category")  # Make sure to fetch this from the product data
        source_site = product.get("sourceSite")
        
        # Extract unit information
        unit_info = self.extract_unit_info(product.get('name', ''), product.get('subtitle', ''))

        transformed_product = {
            "product_id": product_id,  # Extracted product ID
            "productname": product_name,  # Product Name
            "category": category,  # Category from the product data
            "source_site": source_site,  # Source Site
            "size": unit_info['size'],  # Size extracted from unit info
            "image_url": product.get('imageUrl', ''),  # Image URL
            "unit_price": unit_info['unit_price'] or current_price,  # Unit Price
            "unit_name": unit_info['unit_name'],  # Unit Name
            "original_unit_quantity": 1.0,  # Default value
            "current_price": current_price,  # Current Price
            "price_history": '',  # Initialize as empty or provide logic to fill
            "last_updated": product['lastUpdated'],  # Last Updated
            "last_checked": product['lastChecked'],  # Last Checked
            "product_categories": self.build_category_hierarchy(product.get('category', ''))  # Category hierarchy
        }

        logging.info(f"Transformed product: {transformed_product}")
        return transformed_product

    def extract_unit_info(self, name: str, subtitle: str) -> Dict:
        """Extract unit information from product name and subtitle."""
        # Common units and their variations
        units = {
            'kg': ['kg', 'kilo', 'kilogram'],
            'g': ['g', 'gram'],
            'l': ['l', 'liter', 'litre'],
            'ml': ['ml', 'milliliter', 'millilitre'],
            'ea': ['ea', 'each', 'unit', '']
        }
        
        # Default values
        unit_info = {
            'size': '',
            'unit_name': 'ea',
            'unit_price': None
        }
        
        # Combine name and subtitle for searching
        full_text = f"{name} {subtitle}".lower()
        
        # Look for numbers followed by units
        pattern = r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)'
        matches = re.findall(pattern, full_text)
        
        if matches:
            for value, unit in matches:
                # Find the standardized unit
                for std_unit, variations in units.items():
                    if unit in variations:
                        unit_info['size'] = f"{value}{std_unit}"
                        unit_info['unit_name'] = std_unit
                        break
        
        return unit_info

    def build_category_hierarchy(self, category: str) -> List[str]:
        """Build a hierarchical category list."""
        if not category:
            return []
            
        # Split category by any common separators
        categories = [cat.strip() for cat in re.split(r'[/>,\|]', category) if cat.strip()]
        
        # Build hierarchical list
        hierarchy = []
        for i in range(len(categories)):
            hierarchy.append(" > ".join(categories[:i+1]))
            
        return hierarchy

    async def extract_product_data(self, entry) -> Optional[Dict]:
        """Extract product data from the product entry."""
        product = {
            "sourceSite": "paknsave.co.nz",
            "lastChecked": datetime.now().isoformat(),
            "lastUpdated": datetime.now().isoformat()
        }

        try:
            # Extract product name
            name_element = await entry.query_selector('p[data-testid="product-title"]')
            if name_element:
                product_name = await name_element.inner_text()
                product["name"] = product_name.strip() if product_name else None
                logging.info(f"Extracted product name: {product['name']}")
            else:
                logging.warning("Product name element not found.")

            # Extract product subtitle
            subtitle_element = await entry.query_selector('p[data-testid="product-subtitle"]')
            if subtitle_element:
                product["subtitle"] = (await subtitle_element.inner_text()).strip()
                logging.info(f"Extracted product subtitle: {product['subtitle']}")
            else:
                logging.warning("Product subtitle element not found.")

            # Extract image URL
            img_element = await entry.query_selector('img')
            if img_element:
                product["imageUrl"] = await img_element.get_attribute("src")
                logging.info(f"Extracted image URL: {product['imageUrl']}")
            else:
                logging.warning("Image element not found.")

            # Extract price
            price_element = await entry.query_selector('p[data-testid="price-dollars"]')
            if price_element:
                price_dollars = await price_element.inner_text()
                cents_element = await entry.query_selector('p[data-testid="price-cents"]')
                price_cents = await cents_element.inner_text() if cents_element else "00"
                product["price"] = f"{price_dollars}.{price_cents}"
                logging.info(f"Extracted product price: {product['price']}")
            else:
                logging.warning("Price element not found.")

            # Extract product ID from the data-testid attribute
            data_testid = await entry.get_attribute("data-testid")
            if data_testid:
                match = re.search(r'product-(\d+)-', data_testid)
                product["product_id"] = f"pk{match.group(1)}" if match else None  # Prefix 'pk' to the product ID
                logging.info(f"Extracted product ID: {product['product_id']}")
            else:
                logging.warning("data-testid attribute not found.")

            return product if product.get("name") else None

        except Exception as e:
            logging.error(f"Error in extract_product_data: {e}")
            return None

    async def scrape_all_categories(self, playwright):
        """Scrape products from all categories."""
        try:
            # First browser session to fetch categories
            browser = await playwright.chromium.launch(headless=False)
            category_page = await browser.new_page()
            categories = await self.fetch_categories(category_page)
            await browser.close()

            if not categories:
                logging.error("No categories found to process")
                return []
            
            for category in categories:
                try:
                    logging.info(f"Starting to scrape category: {category['name']}")
                    
                    # Create new browser session for each category
                    category_browser = await playwright.chromium.launch(headless=False)
                    product_page = await category_browser.new_page()
                    
                    products = await self.scrape_products(product_page, category["url"])
                    
                    # Products are already written to Frappe in scrape_products
                    self.all_products.extend(products)
                    logging.info(f"Completed scraping {len(products)} products from {category['name']}")
                    
                    # Save progress after each category
                    await self.save_progress()
                    
                    # Close the browser session for this category
                    await category_browser.close()
                    
                except Exception as e:
                    logging.error(f"Error scraping category {category['name']}: {e}")
                    continue

            return self.all_products

        except Exception as e:
            logging.error(f"Error in scrape_all_categories: {e}")
            return []

async def save_products_to_json(products, filename):
    """Save scraped products to a JSON file."""
    try:
        with open(filename, 'w') as f:
            json.dump(products, f, indent=4)
        logging.info(f"Results written to {filename}")
    except Exception as e:
        logging.error(f"Error writing to JSON file: {e}")

async def main():
    config = ScraperConfig(
        base_url="https://www.paknsave.co.nz",
        page_load_delay=int(os.environ.get("PAGE_LOAD_DELAY", 7)),
        product_log_delay=float(os.environ.get("PRODUCT_LOG_DELAY", 0.02))
    )

    filename = f"paknsave_products_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    
    async with async_playwright() as p:
        scraper = PaknSaveScraper(config)
        
        # Pass playwright instance instead of browser
        all_products = await scraper.scrape_all_categories(p)
        
        # Save final results
        await save_products_to_json(all_products, filename)
        logging.info(f"Scraping completed. Results written to {filename}")

if __name__ == "__main__":
    asyncio.run(main())
