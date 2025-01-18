import logging
import re
import os
import json
import random
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from playwright.async_api import async_playwright, Browser, Page
import aiohttp
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor
import time
from itertools import cycle
from frappe_api import test_write_to_frappe

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)



class ProxyFetcher:
    @staticmethod
    def fetch_proxies_from_json(url: str) -> List[str]:
        """Fetch proxies from a JSON URL."""
        try:
            response = requests.get(url)
            data = response.json()

            proxies = set()
            for entry in data:
                proxy = entry.get('proxy')
                if proxy:
                    proxies.add(proxy)
                    logging.info(f"Found proxy: {proxy}")

            logging.info(f"Successfully fetched {len(proxies)} proxies from the JSON URL")
            return list(proxies)
        except Exception as e:
            logging.error(f"Error fetching proxies from JSON URL: {e}")
            return []


    @staticmethod
    def test_proxy(proxy: str, timeout: int = 5) -> bool:
        """Test if a proxy is working with multiple retries."""
        test_urls = [
            'https://www.paknsave.co.nz',
            'https://www.google.com'
        ]
        
        for url in test_urls:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                }
                
                response = requests.get(
                    url,
                    proxies={
                        'http': f'http://{proxy}',
                        'https': f'http://{proxy}'
                    },
                    timeout=timeout,
                    headers=headers
                )
                
                if response.status_code == 200:
                    logging.info(f"Proxy {proxy} successfully tested against {url}")
                    return True
                    
            except Exception as e:
                logging.debug(f"Proxy {proxy} failed testing against {url}: {e}")
                continue
                
        return False

@dataclass
class ScraperConfig:
    """Configuration settings for the scraper."""
    base_url: str
    page_load_delay: int = 2
    product_log_delay: float = 0.02
    max_retries: int = 3
    concurrent_categories: int = 3
    proxy_list: List[str] = None

    def __post_init__(self):
        if self.proxy_list is None:
            logging.info("Fetching and validating free proxies...")
            self.proxy_list = ProxyFetcher.fetch_proxies_from_json("https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.json")
            logging.info(f"Found {len(self.proxy_list)} working proxies")


class ProxyManager:
    def __init__(self, proxies: List[str]):
        self.proxies = cycle(proxies)
        self.current_proxy = next(self.proxies)
        self.failed_attempts = {}
        self.lock = asyncio.Lock()

    async def get_next_proxy(self) -> str:
        """Get next working proxy with thread safety."""
        async with self.lock:
            self.current_proxy = next(self.proxies)
            return self.current_proxy

    async def mark_proxy_failed(self, proxy: str):
        """Mark a proxy as failed and get next one if too many failures."""
        async with self.lock:
            self.failed_attempts[proxy] = self.failed_attempts.get(proxy, 0) + 1
            if self.failed_attempts[proxy] >= 3:
                await self.get_next_proxy()










class PaknSaveScraper:
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.all_products = []
        self.proxy_manager = ProxyManager(config.proxy_list)
        self.browser = None  # Will be set when scraping starts

        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
        ]
        
    async def initialize_browser(self, playwright):
        """Initialize the browser instance."""
        if not self.browser:
            self.browser = await playwright.chromium.launch(headless=False)
        return self.browser

    async def safe_get(self, page: Page, url: str) -> bool:
        """Enhanced safe navigation with anti-detection measures."""
        for attempt in range(self.config.max_retries):
            try:
                await asyncio.sleep(random.uniform(1, 3))
                await page.goto(url, wait_until="domcontentloaded")
                
                if await self.detect_blocking(page):
                    await self.proxy_manager.mark_proxy_failed(self.proxy_manager.current_proxy)
                    return False
                
                return True
            except Exception as e:
                logging.error(f"Error accessing {url} (attempt {attempt + 1}): {e}")
                await asyncio.sleep(self.config.page_load_delay)
        return False


    async def create_browser_context(self, playwright) -> Browser:
        """Create a new browser context with proxy and random user agent."""
        proxy = self.proxy_manager.current_proxy
        browser = await playwright.chromium.launch(
            headless=True,
            proxy={
                "server": f"http://{proxy}"
            }
        )
        context = await browser.new_context(
            user_agent=random.choice(self.user_agents),
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=1,
        )
        return browser, context



    async def fetch_categories(self, page: Page) -> List[Dict[str, str]]:
        """Enhanced category fetching with better selectors."""
        try:
            await self.safe_get(page, f"{self.config.base_url}/shop")
            await asyncio.sleep(2)

            # Wait for and click the category menu
            await page.wait_for_selector('button[data-testid="category-nav-button"]')
            await page.click('button[data-testid="category-nav-button"]')
            
            # Wait for category panel
            await page.wait_for_selector('div[data-testid="category-panel"]')
            
            categories = []
            category_elements = await page.query_selector_all('div[data-testid="category-panel"] a')
            
            for element in category_elements:
                category_name = await element.inner_text()
                href = await element.get_attribute('href')
                
                if href and category_name.strip():
                    full_url = f"{self.config.base_url}{href}"
                    categories.append({
                        "name": category_name.strip(),
                        "url": full_url,
                        "parent": None
                    })
            
            return categories
        except Exception as e:
            logging.error(f"Error in fetch_categories: {e}", exc_info=True)
            return []

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

            # Extract product subtitle
            subtitle_element = await entry.query_selector('p[data-testid="product-subtitle"]')
            if subtitle_element:
                product["subtitle"] = (await subtitle_element.inner_text()).strip()

            # Extract image URL
            img_element = await entry.query_selector('img')
            if img_element:
                product["imageUrl"] = await img_element.get_attribute("src")

            # Extract price
            price_element = await entry.query_selector('p[data-testid="price-dollars"]')
            if price_element:
                price_dollars = await price_element.inner_text()
                cents_element = await entry.query_selector('p[data-testid="price-cents"]')
                price_cents = await cents_element.inner_text() if cents_element else "00"
                product["price"] = f"{price_dollars}.{price_cents}"

            # Extract product ID
            data_testid = await entry.get_attribute("data-testid")
            if data_testid:
                match = re.search(r'product-(\d+)-', data_testid)
                product["product_id"] = f"pk{match.group(1)}" if match else None

            return product if product.get("name") else None

        except Exception as e:
            logging.error(f"Error in extract_product_data: {e}")
            return None

    def transform_to_frappe_format(self, product: Dict) -> Dict:
        """Transform scraped product data to Frappe format."""
        try:
            price_str = product.get('price', '0.00')
            current_price = float(price_str)
        except ValueError:
            current_price = 0.00

        product_id = product.get("product_id")
        product_name = product.get("name")
        category = product.get("category")
        source_site = product.get("sourceSite")
        
        unit_info = self.extract_unit_info(product.get('name', ''), product.get('subtitle', ''))

        transformed_product = {
            "product_id": product_id,
            "productname": product_name,
            "category": category,
            "source_site": source_site,
            "size": unit_info['size'],
            "image_url": product.get('imageUrl', ''),
            "unit_price": unit_info['unit_price'] or current_price,
            "unit_name": unit_info['unit_name'],
            "original_unit_quantity": 1.0,
            "current_price": current_price,
            "price_history": '',
            "last_updated": product['lastUpdated'],
            "last_checked": product['lastChecked'],
            "product_categories": self.build_category_hierarchy(product.get('category', ''))
        }

        return transformed_product

    def extract_unit_info(self, name: str, subtitle: str) -> Dict:
        """Extract unit information from product name and subtitle."""
        units = {
            'kg': ['kg', 'kilo', 'kilogram'],
            'g': ['g', 'gram'],
            'l': ['l', 'liter', 'litre'],
            'ml': ['ml', 'milliliter', 'millilitre'],
            'ea': ['ea', 'each', 'unit', '']
        }
        
        unit_info = {
            'size': '',
            'unit_name': 'ea',
            'unit_price': None
        }
        
        full_text = f"{name} {subtitle}".lower()
        pattern = r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)'
        matches = re.findall(pattern, full_text)
        
        if matches:
            for value, unit in matches:
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
            
        categories = [cat.strip() for cat in re.split(r'[/>,\|]', category) if cat.strip()]
        hierarchy = []
        for i in range(len(categories)):
            hierarchy.append(" > ".join(categories[:i+1]))
            
        return hierarchy

    async def scrape_products(self, page, start_url: str) -> List[Dict]:
            """Scrape products from the starting URL."""
            products = []
            try:
                await self.safe_get(page, start_url)
                
                while True:
                    await asyncio.sleep(random.uniform(2, 5))
                    product_elements = await page.query_selector_all('div[data-testid$="-EA-000"]')

                    for element in product_elements:
                        product_data = await self.extract_product_data(element)
                        if product_data:
                            # Get product URL for fetching details
                            url_element = await element.query_selector('a[href]')
                            if url_element:
                                href = await url_element.get_attribute('href')
                                full_url = f"{self.config.base_url}{href}"
                                
                                # Use the existing browser instance
                                product_page = await self.browser.new_page()
                                
                                # Fetch details including categories
                                details = await self.fetch_product_details(product_page, full_url)
                                product_data.update(details)
                                
                                await product_page.close()

                                # Transform and write to Frappe
                                frappe_product = self.transform_to_frappe_format(product_data)
                                try:
                                    test_write_to_frappe(frappe_product)
                                    logging.info(f"Successfully sent product to Frappe: {frappe_product['productname']}")
                                    products.append(product_data)
                                except Exception as e:
                                    logging.error(f"Error writing product to Frappe: {e}")

                    next_page = await page.query_selector('a[data-testid="pagination-increment"]')
                    if next_page:
                        await next_page.click()
                        await page.wait_for_load_state('networkidle')
                    else:
                        break

                return products

            except Exception as e:
                logging.error(f"Error scraping products: {e}")
                return products

    async def scrape_all_categories(self, playwright):
        """Scrape products from all categories."""
        try:
            # Initialize the main browser instance
            self.browser = await self.initialize_browser(playwright)
            
            # First browser session to fetch categories
            category_page = await self.browser.new_page()
            categories = await self.fetch_categories(category_page)
            await category_page.close()

            if not categories:
                logging.error("No categories found to process")
                return []
            
            for category in categories:
                try:
                    logging.info(f"Starting to scrape category: {category['name']}")
                    
                    # Create new page in existing browser session
                    product_page = await self.browser.new_page()
                    
                    products = await self.scrape_products(product_page, category["url"])
                    
                    # Products are already written to Frappe in scrape_products
                    self.all_products.extend(products)
                    logging.info(f"Completed scraping {len(products)} products from {category['name']}")
                    
                    # Close the page for this category
                    await product_page.close()
                    
                except Exception as e:
                    logging.error(f"Error scraping category {category['name']}: {e}")
                    continue
                    
            # Close the browser when done
            await self.browser.close()
            return self.all_products

        except Exception as e:
            logging.error(f"Error in scrape_all_categories: {e}")
            if self.browser:
                await self.browser.close()
            return []

    async def fetch_product_details(self, page, product_url: str) -> Dict:
        """Fetch additional details from individual product page."""
        details = {}
        try:
            await self.safe_get(page, product_url)

            # Fetch categories with new structure
            category_data = await self.fetch_product_categories(product_url)
            details['category_data'] = category_data

            # Get product name
            name_elem = await page.query_selector('[data-testid="product-title"]')
            if name_elem:
                details['name'] = await name_elem.inner_text()

            # Get product description
            description_elem = await page.query_selector("div.fs-product-details__description")
            if description_elem:
                details['description'] = await description_elem.inner_text()

            # Get nutritional information
            nutrition_table = await page.query_selector("table.fs-nutritional-info")
            if nutrition_table:
                nutrition_data = {}
                rows = await nutrition_table.query_selector_all("tr")
                for row in rows:
                    cols = await row.query_selector_all("td")
                    if len(cols) >= 2:
                        key = await cols[0].inner_text()
                        value = await cols[1].inner_text()
                        nutrition_data[key.strip()] = value.strip()
                details['nutritionalInfo'] = nutrition_data

            # Get ingredients
            ingredients_elem = await page.query_selector("div.fs-product-details__ingredients")
            if ingredients_elem:
                details['ingredients'] = await ingredients_elem.inner_text()

            # Get brand information
            brand_elem = await page.query_selector("div.fs-product-details__brand")
            if brand_elem:
                details['brand'] = await brand_elem.inner_text()

            # Get price information
            price_dollars = await page.query_selector('p[data-testid="price-dollars"]')
            price_cents = await page.query_selector('p[data-testid="price-cents"]')
            if price_dollars and price_cents:
                dollars = await price_dollars.inner_text()
                cents = await price_cents.inner_text()
                details['price'] = f"{dollars}.{cents}"

            # Get product subtitle (usually contains size/weight information)
            subtitle_elem = await page.query_selector('[data-testid="product-subtitle"]')
            if subtitle_elem:
                details['subtitle'] = await subtitle_elem.inner_text()

            # Get product image URL
            img_elem = await page.query_selector('img[data-testid="product-image"]')
            if img_elem:
                details['imageUrl'] = await img_elem.get_attribute('src')

            # Get any promotional information
            promo_elem = await page.query_selector("div.fs-product-details__promotion")
            if promo_elem:
                details['promotion'] = await promo_elem.inner_text()

            # Add timestamp information
            details['lastChecked'] = datetime.now().isoformat()
            details['lastUpdated'] = datetime.now().isoformat()

            logging.info(f"Successfully fetched product details from {product_url}")
            return details

        except Exception as e:
            logging.error(f"Error fetching product details from {product_url}: {e}")
            error_details = {
                'error': str(e),
                'url': product_url,
                'timestamp': datetime.now().isoformat()
            }
            logging.debug(f"Detailed error information: {error_details}")
            return details

        finally:
            try:
                # Ensure we've captured essential information even if there were errors
                if 'lastChecked' not in details:
                    details['lastChecked'] = datetime.now().isoformat()
                if 'lastUpdated' not in details:
                    details['lastUpdated'] = datetime.now().isoformat()
                if 'category_data' not in details:
                    details['category_data'] = {'full_hierarchy': '', 'category': '', 'categories_list': []}
            except Exception as e:
                logging.error(f"Error in finally block of fetch_product_details: {e}")

    async def fetch_product_categories(self, product_url: str) -> Dict[str, str]:
        """Fetch categories for a specific product using a fresh browser session."""
        try:
            context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            page = await context.new_page()
            
            try:
                await self.safe_get(page, product_url)
                await asyncio.sleep(5)
                
                category_data = {
                    'categories_list': [],
                    'category': '',
                    'product_categories': []
                }
                
                categories = []
                
                # Extract the categories
                breadcrumbs = await page.query_selector('nav[aria-label="Breadcrumbs"]')
                if breadcrumbs:
                    for i in range(3):
                        category = await page.query_selector(f'[data-testid="product-category-{i}"] p')
                        if category:
                            category_text = await category.inner_text()
                            if category_text:
                                categories.append(category_text.strip())
                
                # Get product name without 'ea' suffix
                product_name = await page.query_selector('[data-testid="product-title"]')
                if product_name:
                    product_name_text = (await product_name.inner_text()).strip()
                    product_name_cleaned = re.sub(r'\s*ea\s*$', '', product_name_text)  # Remove 'ea' suffix
                    categories.append(product_name_cleaned)
                
                # Set the individual category names
                for i, cat in enumerate(categories):
                    category_data[f'category_name_{i+1}'] = cat
                
                # Set the 3rd category
                if len(categories) >= 3:
                    category_data['category'] = categories[2]  # 3rd category
                
                # Populate product_categories
                category_data['product_categories'] = [{'doctype': 'Product Category', 'category_name': cat} for cat in categories]
                
                logging.info(f"Extracted categories: {categories}")
                return category_data
                
            except Exception as e:
                logging.error(f"Error fetching product categories: {e}")
                return {}
                
            finally:
                await context.close()
        except Exception as e:
            logging.error(f"Error creating browser context: {e}")
            return {}

    def extract_unit_info(self, name: str, subtitle: str) -> Dict:
        """Extract unit information from product name and subtitle."""
        units = {
            'kg': ['kg', 'kilo', 'kilogram'],
            'g': ['g', 'gram'],
            'l': ['l', 'liter', 'litre'],
            'ml': ['ml', 'milliliter', 'millilitre'],
            'ea': ['ea', 'each', 'unit', '']
        }
        
        unit_info = {
            'size': '',
            'unit_name': 'ea',
            'unit_price': None
        }
        
        full_text = f"{name} {subtitle}".lower()
        pattern = r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)'
        matches = re.findall(pattern, full_text)
        
        if matches:
            for value, unit in matches:
                for std_unit, variations in units.items():
                    if unit in variations:
                        unit_info['size'] = f"{value}{std_unit}"
                        unit_info['unit_name'] = std_unit
                        break
        
        return unit_info

    async def extract_product_data(self, entry) -> Optional[Dict]:
        """Extract product data from the product entry."""
        product = {
            "sourceSite": "paknsave.co.nz",
            "lastChecked": datetime.now().isoformat(),
            "lastUpdated": datetime.now().isoformat()
        }

        try:
            name_element = await entry.query_selector('p[data-testid="product-title"]')
            if name_element:
                product_name = await name_element.inner_text()
                product["name"] = product_name.strip() if product_name else None
                logging.info(f"Extracted product name: {product['name']}")

            subtitle_element = await entry.query_selector('p[data-testid="product-subtitle"]')
            if subtitle_element:
                product["subtitle"] = (await subtitle_element.inner_text()).strip()

            img_element = await entry.query_selector('img')
            if img_element:
                product["imageUrl"] = await img_element.get_attribute("src")

            price_element = await entry.query_selector('p[data-testid="price-dollars"]')
            if price_element:
                price_dollars = await price_element.inner_text()
                cents_element = await entry.query_selector('p[data-testid="price-cents"]')
                price_cents = await cents_element.inner_text() if cents_element else "00"
                product["price"] = f"{price_dollars}.{price_cents}"

            data_testid = await entry.get_attribute("data-testid")
            if data_testid:
                match = re.search(r'product-(\d+)-', data_testid)
                product["product_id"] = f"pk{match.group(1)}" if match else None

            return product if product.get("name") else None

        except Exception as e:
            logging.error(f"Error in extract_product_data: {e}")
            return None

    async def scrape_all_products(self, playwright):
        """Scrape all products starting from the main shop page."""
        try:
            start_url = f"{self.config.base_url}/shop"
            
            main_browser = await playwright.chromium.launch(headless=False)
            main_page = await main_browser.new_page()
            
            products = await self.scrape_products(main_page, start_url)
            self.all_products.extend(products)
            
            await main_browser.close()
            return self.all_products

        except Exception as e:
            logging.error(f"Error in scrape_all_products: {e}")
            return []
            
         
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


async def main():
    config = ScraperConfig(
        base_url="https://www.paknsave.co.nz",
        page_load_delay=int(os.environ.get("PAGE_LOAD_DELAY", 7)),
        product_log_delay=float(os.environ.get("PRODUCT_LOG_DELAY", 0.02))
    )

    filename = f"paknsave_products_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    
    async with async_playwright() as p:
        scraper = PaknSaveScraper(config)
        all_products = await scraper.scrape_all_categories(p)
        
        # Save final results
        await scraper.save_products_to_json(filename)
        logging.info(f"Scraping completed. Results written to {filename}")

if __name__ == "__main__":
    asyncio.run(main())