import asyncio
import random
import json
import re
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional
from playwright.async_api import async_playwright, Page, Browser, Playwright, ElementHandle

@dataclass
class DatedPrice:
    date: datetime
    price: float

@dataclass
class Product:
    id: str
    name: str
    size: Optional[str]
    current_price: float
    category: List[str]
    source_site: str
    price_history: List[DatedPrice]
    last_updated: datetime
    last_checked: datetime
    unit_price: Optional[float]
    unit_name: Optional[str]
    original_unit_quantity: Optional[float]

@dataclass
class CategorisedURL:
    url: str
    category: str

class PakScraper:
    def __init__(self):
        self.seconds_delay_between_page_scrapes = 11
        self.upload_to_database = False
        self.upload_images = False
        self.use_headless_browser = False
        
        self.playwright: Optional[Playwright] = None
        self.page: Optional[Page] = None
        self.browser: Optional[Browser] = None
        
        self.config = {
            "GEOLOCATION_LAT": "-41.21",
            "GEOLOCATION_LONG": "174.91"
        }
        
        self.load_config()

    def load_config(self):
        try:
            with open('appsettings.json') as f:
                self.config.update(json.load(f))
                print("Configuration loaded from appsettings.json")
        except FileNotFoundError:
            print("Warning: appsettings.json not found, using default settings")

    def read_urls_file(self) -> List[CategorisedURL]:
        try:
            with open('Urls.txt') as f:
                urls = [CategorisedURL(line.strip(), 'default') for line in f if 'paknsave.co.nz' in line]
                print(f"URLs read: {len(urls)}")
                return urls
        except FileNotFoundError:
            print("Error: Urls.txt not found")
            return []

    async def establish_playwright(self, headless: bool):
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(headless=headless)
            self.page = await self.browser.new_page()
            print("Playwright established")
        except Exception as e:
            print(f"Error establishing Playwright: {str(e)}")
            raise

    async def set_geolocation(self):
        try:
            latitude = float(self.config.get('GEOLOCATION_LAT', ''))
            longitude = float(self.config.get('GEOLOCATION_LONG', ''))

            await self.page.context.set_geolocation({
                'latitude': latitude,
                'longitude': longitude
            })
            await self.page.context.grant_permissions(['geolocation'])
            print(f"Setting geolocation: ({latitude}, {longitude})")
        except (ValueError, KeyError):
            print("Using default location")

    async def scrape_page(self, url: CategorisedURL) -> List[Product]:
        products = []
        try:
            await self.page.goto(url.url, wait_until="domcontentloaded", timeout=60000)
            print(f"Navigating to {url.url}")

            # Randomized delay before scrolling
            await asyncio.sleep(random.uniform(2, 5))

            # Scroll to trigger lazy loading
            for _ in range(3):  # Adjust the number of scrolls based on your needs
                await self.page.keyboard.press("PageDown")
                await self.page.wait_for_timeout(random.uniform(500, 1000))  # Random delay between scrolls

            # Wait for products to load
            await self.page.wait_for_selector('div[data-testid$="-EA-000"]', timeout=60000)

            product_elements = await self.page.query_selector_all('div[data-testid$="-EA-000"]')
            print(f"Found {len(product_elements)} products on {url.url}")

            for element in product_elements:
                product = await self.scrape_product_element(element, url.url, [url.category])
                if product:
                    products.append(product)

            # Randomized delay after scraping
            await asyncio.sleep(random.uniform(30, 60))  # Wait before the next request

        except Exception as e:
            print(f"Error scraping page {url.url}: {str(e)}")
            self.log_error(f"Error scraping page {url.url}: {str(e)}")

        return products
        
    async def scrape_product_element(self, product_element: ElementHandle, source_url: str, category: List[str]) -> Optional[Product]:
        try:
            name_elem = await product_element.query_selector('p[data-testid="product-title"]')
            price_dollars_elem = await product_element.query_selector('p[data-testid="price-dollars"]')
            price_cents_elem = await product_element.query_selector('p[data-testid="price-cents"]')

            name = await name_elem.inner_text() if name_elem else ""
            dollars = await price_dollars_elem.inner_text() if price_dollars_elem else "0"
            cents = await price_cents_elem.inner_text() if price_cents_elem else "0"
            price = float(f"{dollars}.{cents}")

            print(f"Found product name: {name}, price: {price}")

            return Product(
                id='dummy_id',  # Placeholder for actual product ID
                name=name,
                size=None,
                current_price=price,
                category=category,
                source_site='paknsave.co.nz',
                price_history=[],
                last_updated=datetime.now(),
                last_checked=datetime.now(),
                unit_price=None,
                unit_name=None,
                original_unit_quantity=None
            )

        except Exception as e:
            print(f"Error scraping product {name if 'name' in locals() else 'unknown'}: {str(e)}")
            self.log_error(f"Error scraping product {name if 'name' in locals() else 'unknown'}: {str(e)}")
            return None

    def log_error(self, message: str):
        with open('scraper_errors.log', 'a') as f:
            f.write(f"{datetime.now()}: {message}\n")
        print(f"Error logged: {message}")

    async def main(self):
        try:
            await self.establish_playwright(self.use_headless_browser)
            await self.set_geolocation()

            urls = self.read_urls_file()
            all_products = []

            for i, url in enumerate(urls, 1):
                print(f"\nScraping page {i}/{len(urls)}: {url.url}")
                products = await self.scrape_page(url)
                all_products.extend(products)

                if i < len(urls):
                    print(f"Waiting {self.seconds_delay_between_page_scrapes} seconds before next page...")
                    await asyncio.sleep(self.seconds_delay_between_page_scrapes)

            print(f"\nScraping completed. Total products scraped: {len(all_products)}")

        except Exception as e:
            print(f"Fatal error: {str(e)}")
            self.log_error(f"Fatal error: {str(e)}")
        finally:
            if self.page:
                await self.page.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            print("Cleanup completed")

def main():
    scraper = PakScraper()
    asyncio.run(scraper.main())

if __name__ == "__main__":
    main()