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

            return product if product.get("name") else None

        except Exception as e:
            logging.error(f"Error in extract_product_data: {e}")
            return None

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
                        products.append(product_data)
                        logging.info(f"Scraped product: {product_data.get('name')}")
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

    async def scrape_all_categories(self, browser):
        """Scrape products from all categories."""
        try:
            # First page to fetch categories
            category_page = await browser.new_page()
            categories = await self.fetch_categories(category_page)
            await category_page.close()

            if not categories:
                logging.error("No categories found to process")
                return []

            # Create a new page for product scraping
            product_page = await browser.new_page()
            
            for category in categories:
                try:
                    logging.info(f"Starting to scrape category: {category['name']}")
                    products = await self.scrape_products(product_page, category["url"])
                    
                    # Add category information to each product
                    for product in products:
                        product["category"] = category["name"]
                    
                    self.all_products.extend(products)
                    logging.info(f"Completed scraping {len(products)} products from {category['name']}")
                    
                    # Save progress after each category
                    await self.save_progress()
                    
                except Exception as e:
                    logging.error(f"Error scraping category {category['name']}: {e}")
                    continue

            await product_page.close()
            return self.all_products

        except Exception as e:
            logging.error(f"Error in scrape_all_categories: {e}")
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
        browser = await p.chromium.launch(headless=False)
        scraper = PaknSaveScraper(config)
        
        # Scrape all categories
        all_products = await scraper.scrape_all_categories(browser)
        
        # Save final results
        await save_products_to_json(all_products, filename)
        
        await browser.close()
        logging.info(f"Scraping completed. Results written to {filename}")

if __name__ == "__main__":
    asyncio.run(main())