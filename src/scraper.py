import time
import logging
import re
import os
import pickle
from datetime import datetime
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import json
import requests
import random

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import *
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains

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
    timeout: int = 20
    chrome_options: List[str] = None

    def __post_init__(self):
        if self.chrome_options is None:
            self.chrome_options = [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--start-maximized"
            ]

class WebDriverManager:
    """Manages WebDriver initialization and cleanup."""
    
    @staticmethod
    def get_driver(config: ScraperConfig) -> Optional[webdriver.Chrome]:
        options = Options()
        for option in config.chrome_options:
            options.add_argument(option)
            
        try:
            service = Service(ChromeDriverManager().install())
            return webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logging.error(f"Error initializing WebDriver: {e}")
            return None

class BaseScraper(ABC):
    """Abstract base class for web scrapers."""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.driver = None
        
    def __enter__(self):
        self.driver = WebDriverManager.get_driver(self.config)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()

    @abstractmethod
    def fetch_categories(self) -> List[Dict[str, str]]:
        """Fetch all categories to be scraped."""
        pass

    @abstractmethod
    def extract_product_data(self, entry: BeautifulSoup, **kwargs) -> Optional[Dict]:
        """Extract product data from a BeautifulSoup element."""
        pass

    def wait_for_element(self, by: By, selector: str, timeout: Optional[int] = None) -> bool:
        """Wait for an element to be present and visible."""
        try:
            WebDriverWait(self.driver, timeout or self.config.timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            return True
        except TimeoutException:
            return False


    def get_page_source(self) -> Optional[BeautifulSoup]:
        """Get the current page source as BeautifulSoup object."""
        try:
            return BeautifulSoup(self.driver.page_source, "html.parser")
        except Exception as e:
            logging.error(f"Error getting page source: {e}")
            return None

    def safe_get(self, url: str) -> bool:
        """Safely navigate to a URL with retries and Cloudflare handling."""
        for attempt in range(self.config.max_retries):
            try:
                self.driver.get(url)
                time.sleep(self.config.page_load_delay)
                
                # Check for Cloudflare challenge
                if self.handle_cloudflare_check():
                    logging.info("Successfully handled verification check")
                    return True
                    
                return True
                
            except Exception as e:
                logging.error(f"Error accessing {url} (attempt {attempt + 1}): {e}")
                if attempt == self.config.max_retries - 1:
                    return False
                time.sleep(self.config.page_load_delay)
                
        return False

    def handle_cloudflare_check(self) -> bool:
        """Handle Cloudflare and other verification challenges."""
        try:
            # Common verification indicators
            verification_selectors = [
                "iframe[title='Widget containing a Cloudflare security challenge']",
                "#challenge-form",
                "#cf-challenge-running",
                "[data-testid='verify-human']",
                "div.cf-wrapper",
                "#captcha-bypass"
            ]
            
            for selector in verification_selectors:
                try:
                    element = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    if element.is_displayed():
                        logging.info(f"Verification challenge detected ({selector})")
                        
                        # If it's a simple button click verification
                        if selector == "[data-testid='verify-human']":
                            element.click()
                            time.sleep(2)
                            return True
                            
                        # For Cloudflare and other challenges
                        logging.info("Waiting for manual verification completion...")
                        # Wait for the challenge element to disappear
                        WebDriverWait(self.driver, 120).until_not(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        logging.info("Verification completed")
                        
                        # Add delay after verification
                        time.sleep(5)
                        
                        # Refresh if needed
                        if any(term in self.driver.current_url.lower() for term in ["challenge", "cdn-cgi", "captcha"]):
                            logging.info("Refreshing page after verification...")
                            self.driver.refresh()
                            time.sleep(5)
                        
                        return True
                        
                except TimeoutException:
                    continue
                    
            return False  # No verification detected
            
        except Exception as e:
            logging.error(f"Error in handle_cloudflare_check: {e}")
            return False

    def scrape_products(self, url: str) -> List[Dict]:
        """Scrape products with verification handling."""
        if not self.safe_get(url):
            logging.error(f"Failed to access URL: {url}")
            return []

        products = []
        current_page = 1
        logging.info(f"Starting to scrape products from: {url}")

        while True:
            # Get page source and check for products
            soup = self.get_page_source()
            if not soup:
                logging.error("Failed to retrieve page source.")
                break

            product_entries = self.find_product_entries(soup)
            
            # If no products found, check if it's due to verification
            if not product_entries:
                if self.handle_cloudflare_check():
                    logging.info("Verification handled, retrying page...")
                    if not self.safe_get(url):
                        break
                    continue
                else:
                    logging.warning("No product entries found on the current page.")
                    break

            # Process products
            for entry in product_entries:
                product = self.extract_product_data(entry)
                if product:
                    products.append(product)
                    logging.info(f"Successfully extracted product: {product.get('name', 'Unknown')}")
                    time.sleep(random.uniform(0.01, self.config.product_log_delay))
                else:
                    logging.error("Failed to extract product data from entry.")

            if not self.goto_next_page():
                logging.info("No more pages to navigate.")
                break
                
            current_page += 1
            logging.info(f"Moving to page {current_page}")

        logging.info(f"Scraping completed. Total products scraped: {len(products)}")
        return products

    @abstractmethod
    def find_product_entries(self, soup: BeautifulSoup) -> List[Any]:
        """Find all product entries on the current page."""
        pass

    @abstractmethod
    def goto_next_page(self) -> bool:
        """Navigate to the next page if available."""
        pass

class PaknSaveScraper(BaseScraper):
    """PaknSave specific implementation of the BaseScraper."""

    def fetch_categories(self) -> List[Dict[str, str]]:
        try:
            if not self.safe_get('https://www.paknsave.co.nz/shop/category/fresh-foods-and-bakery?pg=1'):
                logging.error("Failed to load base URL")
                return []

            # Close the tooltip if it appears
            try:
                self.driver.implicitly_wait(10)
                tooltip_close_button = self.driver.find_element(By.CSS_SELECTOR, 'button._19kx3s2')
                tooltip_close_button.click()
                logging.info("Closed the tooltip")
            except Exception as e:
                logging.warning(f"Tooltip not found or unable to close it: {e}")

            # Adding longer wait time and logging the state
            logging.info("Waiting for menu panel to load...")

            # Attempt to find and interact with the 'Groceries' menu item
            try:
                groceries_button = self.driver.find_element(By.XPATH, '//span[contains(text(), "Groceries")]/..')
                groceries_button.click()
                logging.info("Clicked on the 'Groceries' menu item")
                time.sleep(2)  # Give some time for the menu to expand
            except Exception as e:
                logging.error(f"Unable to find or click the 'Groceries' menu item: {e}")
                return []

            if not self.wait_for_element(By.CSS_SELECTOR, 'div._177qnsx5', timeout=30):
                logging.error("Menu panel not found after wait")
                return []

            # Handle human verification if it appears
            try:
                human_verification_button = self.driver.find_element(By.CSS_SELECTOR, 'button[data-testid="verify-human"]')
                human_verification_button.click()
                logging.info("Clicked on human verification button")
                time.sleep(2)  # Give some time for the verification process
            except Exception as e:
                logging.info("No human verification required")

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            if not soup:
                logging.error("Failed to get page source")
                return []

            # Log the page source for debugging
            logging.debug(f"Page source: {soup.prettify()}")

            # Attempt to find the menu panel
            menu_panel = soup.find('div', class_='_177qnsx5')
            if menu_panel:
                logging.debug(f"Menu panel HTML: {menu_panel.prettify()}")
            else:
                logging.error("Menu panel not found in page source")
                return []

            # Updated selector to match the HTML structure
            category_elements = menu_panel.select('button._177qnsx7')

            logging.info(f"Got the {len(category_elements)} categories")

            if not category_elements:
                logging.error("No category elements found with updated selector")
                # Log the actual HTML for debugging
                logging.debug(f"Menu panel HTML: {menu_panel.prettify()}")
                return []

            categories = []
            for element in category_elements:
                try:
                    # Get the full category name and clean it up
                    category_name = element.get_text(strip=True)

                    # Skip "Featured" category as it's not a main category
                    if category_name.lower() == "featured":
                        continue

                    if category_name.lower() == "all null":
                        continue

                    # Convert category name to URL format
                    url_name = category_name.lower()
                    url_name = url_name.replace(" & ", "-and-")
                    url_name = url_name.replace(", ", "-")
                    url_name = url_name.replace(" ", "-")

                    # Some common replacements for special characters
                    url_name = url_name.replace("&amp;", "and")

                    category_url = f"https://www.paknsave.co.nz/shop/category/{url_name}?pg=1"
                    categories.append({
                        "name": category_name,
                        "url": category_url
                    })
                    logging.info(f"Found category: {category_name} - URL: {category_url}")

                except Exception as e:
                    logging.error(f"Error processing category element: {e}")
                    continue

            logging.info(f"Successfully fetched {len(categories)} categories")


            return categories

        except Exception as e:
            logging.error(f"Error in fetch_categories: {e}", exc_info=True)
            return []


    def fetch_product_details(self, product_url: str) -> Dict:
        """Fetch additional details from individual product page."""
        details = {}
        try:
            if not self.safe_get(product_url):
                logging.error(f"Failed to load product page: {product_url}")
                return details

            if not self.wait_for_element(By.CLASS_NAME, "fs-product-details", timeout=10):
                logging.error(f"Product details not found: {product_url}")
                return details

            soup = self.get_page_source()
            if not soup:
                logging.error(f"Failed to get product page source: {product_url}")
                return details

            # Extract detailed description
            description_elem = soup.select_one("div.fs-product-details__description")
            if description_elem:
                details['description'] = description_elem.text.strip()
            
            # Extract nutritional information
            nutrition_table = soup.select_one("table.fs-nutritional-info")
            if nutrition_table:
                nutrition_data = {}
                rows = nutrition_table.select("tr")
                for row in rows:
                    try:
                        cols = row.select("td")
                        if len(cols) >= 2:
                            key = cols[0].text.strip()
                            value = cols[1].text.strip()
                            nutrition_data[key] = value
                    except Exception as e:
                        logging.error(f"Error processing nutrition row: {e}")
                details['nutritionalInfo'] = nutrition_data

            # Extract ingredients
            ingredients_elem = soup.select_one("div.fs-product-details__ingredients")
            if ingredients_elem:
                details['ingredients'] = ingredients_elem.text.strip()

            logging.info(f"Successfully fetched product details from {product_url}")
            return details

        except Exception as e:
            logging.error(f"Error fetching product details from {product_url}: {e}")
            return details

    def extract_product_data(self, entry: BeautifulSoup, **kwargs) -> Optional[Dict]:
        product = {
            "sourceSite": "paknsave.co.nz",
            "lastChecked": datetime.now().isoformat(),
            "lastUpdated": datetime.now().isoformat()
        }

        try:
            # Extract product ID
            product_link = entry.select_one("a.fs-product-card__row")
            if product_link and 'href' in product_link.attrs:
                try:
                    product_url = f"https://www.paknsave.co.nz{product_link['href']}"
                    product_id = re.search(r'product/(\d+)', product_url)
                    if product_id:
                        product["id"] = product_id.group(1)
                        # Fetch additional details from product page
                        details = self.fetch_product_details(product_url)
                        product.update(details)
                except Exception as e:
                    logging.error(f"Error processing product URL and details: {e}")

            # Extract name and size
            try:
                name_element = entry.select_one("h3.u-p2")
                if name_element:
                    raw_name_size = name_element.text.strip().lower()
                    size_match = re.search(r"(pk\s\d+)|(\d+(\.\d+)?(\-\d+\.\d+)?\s?(g|kg|l|ml|pack))\b", raw_name_size)
                    if size_match:
                        product["name"] = raw_name_size[:size_match.start()].strip().title()
                        product["size"] = size_match.group(0).replace("l", "L").replace("pk", "Pack")
                    else:
                        product["name"] = raw_name_size.title()
                        product["size"] = ""
            except Exception as e:
                logging.error(f"Error extracting name and size: {e}")

            # Extract image URL
            try:
                img_element = entry.select_one("img.u-img-responsive")
                if img_element:
                    product["imageUrl"] = img_element.get("src")
            except Exception as e:
                logging.error(f"Error extracting image URL: {e}")

            # Extract prices
            try:
                self._extract_price(entry, product)
            except Exception as e:
                logging.error(f"Error extracting price: {e}")

            try:
                self._extract_unit_price(entry, product)
            except Exception as e:
                logging.error(f"Error extracting unit price: {e}")

            # Extract categories
            try:
                breadcrumbs = entry.select("nav.c-breadcrumbs li")
                if breadcrumbs:
                    product["product_categories"] = [crumb.text.strip() for crumb in breadcrumbs if crumb.text.strip()]
            except Exception as e:
                logging.error(f"Error extracting categories: {e}")

            # Validate required fields
            if not product.get("name"):
                logging.error("Product name not found, skipping product")
                return None

            return product

        except Exception as e:
            logging.error(f"Error in extract_product_data: {e}")
            return None

    def _extract_price(self, entry: BeautifulSoup, product: Dict) -> None:
        try:
            price_element = entry.select_one("div.fs-price-lockup__dollars")
            cents_element = entry.select_one("div.fs-price-lockup__cents")
            
            if price_element:
                dollars = price_element.text.strip()
                cents = cents_element.text.strip() if cents_element else "00"
                try:
                    product["currentPrice"] = float(f"{dollars}.{cents}")
                except ValueError as e:
                    logging.error(f"Error converting price {dollars}.{cents}: {e}")
        except Exception as e:
            logging.error(f"Error in _extract_price: {e}")
            raise

    def _extract_unit_price(self, entry: BeautifulSoup, product: Dict) -> None:
        try:
            unit_price_element = entry.select_one("div.fs-price-lockup__cup-price")
            if unit_price_element:
                raw_unit_price = unit_price_element.text.strip()
                unit_price_match = re.match(r"\$([\d.]+) / (\d+(g|kg|ml|l))", raw_unit_price)
                if unit_price_match:
                    self._process_unit_price(unit_price_match, product)
        except Exception as e:
            logging.error(f"Error in _extract_unit_price: {e}")
            raise

    def find_product_entries(self, soup: BeautifulSoup) -> List[Any]:
        """
        Find product entries on the page using BeautifulSoup.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of product elements found on the page
        """
        try:
            logging.info("Finding Product Entries")
            
            # Get all div elements
            all_div_elements = soup.find_all('div')
            
            # Filter for product elements with specific test ID pattern
            product_elements = [
                element for element in all_div_elements
                if element.get('data-testid', '').endswith('-EA-000')
            ]
            
            logging.info(f"Found {len(product_elements)} product entries on current page")
            return product_elements
            
        except Exception as e:
            logging.error(f"Error finding product entries: {e}")
            return []
        
    def goto_next_page(self) -> bool:
        try:
            next_button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "button.fs-pagination__next"))
            )
            
            if not next_button.is_displayed() or 'disabled' in next_button.get_attribute('class'):
                logging.info("No more pages available")
                return False

            self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(1)
            next_button.click()
            time.sleep(self.config.page_load_delay)
            logging.info("Successfully navigated to next page")
            return True
            
        except TimeoutException:
            logging.info("Next page button not found - likely last page")
            return False
        except Exception as e:
            logging.error(f"Error navigating to next page: {e}")
            return False


def main():
    config = ScraperConfig(
        base_url="https://www.paknsave.co.nz",
        page_load_delay=int(os.environ.get("PAGE_LOAD_DELAY", 7)),
        product_log_delay=float(os.environ.get("PRODUCT_LOG_DELAY", 0.02))
    )

    filename = f"paknsave_products_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    
    with open(filename, 'w') as outfile:
        logging.info(f"Opened file {filename} for writing.")
        with PaknSaveScraper(config) as scraper:
            # Fetch categories with additional delay
            categories = scraper.fetch_categories()
            if not categories:
                logging.error("No categories found to process")
                return

            # Add a longer delay after fetching categories
            time.sleep(10)  # Allow page to settle
            
            # Process each category with careful timing
            for idx, category in enumerate(categories):
                try:
                    logging.info(f"Preparing to start category {idx + 1}/{len(categories)}: {category['name']}")
                    
                    # Add random delay between categories
                    delay = random.uniform(8, 15)
                    logging.info(f"Waiting {delay:.2f} seconds before processing category...")
                    time.sleep(delay)
                    
                    # If this is the first category, add extra delay and handle differently
                    if idx == 0:
                        # Start with a fresh browser session for first category
                        scraper.driver.delete_all_cookies()
                        scraper.driver.refresh()
                        time.sleep(5)
                        
                        # Navigate to homepage first
                        scraper.safe_get(config.base_url)
                        time.sleep(8)
                        
                        # Then navigate to category
                        logging.info(f"Starting first category: {category['name']}")
                        
                    products = scraper.scrape_products(category["url"])
                    
                    if products:
                        logging.info(f"Found {len(products)} products in category {category['name']}")
                        for product in products:
                            if product:  # Only process valid products
                                json.dump(product, outfile)
                                outfile.write('\n')
                                logging.info(f"Written product to file: {product.get('name', 'Unknown')}")
                    else:
                        logging.warning(f"No products found in category: {category['name']}")
                        
                    # Add delay after processing each category
                    time.sleep(config.page_load_delay * 2)
                    
                except Exception as e:
                    logging.error(f"Error processing category {category['name']}: {e}")
                    # Add extra delay after an error
                    time.sleep(15)
                    continue

    logging.info(f"Scraping completed. Results written to {filename}")

if __name__ == "__main__":
    main()