import asyncio
import random
from playwright.async_api import async_playwright

async def scrape_products(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Set headless=True to run in headless mode
        page = await browser.new_page()

        await page.goto(url, wait_until="domcontentloaded")
        products = []

        while True:
            await asyncio.sleep(random.uniform(2, 5))  # Random delay

            # Scrape products from the current page
            product_elements = await page.query_selector_all('div[data-testid$="-EA-000"]')
            for element in product_elements:
                product_name = await element.inner_text()
                products.append(product_name)  # Customize this to extract more product details
                print(f"Scraped product: {product_name}")

            # Check if there's a next page
            next_page = await page.query_selector('a[data-testid="pagination-increment"]')
            if next_page:
                await next_page.click()  # Click the "Next page" button
                await page.wait_for_load_state('networkidle')  # Wait for the next page to load
            else:
                break  # Exit the loop if there are no more pages

        await browser.close()
        return products

# Starting URL for scraping
url = "https://www.paknsave.co.nz/shop/category/fresh-foods-and-bakery?pg=1"
products = asyncio.run(scrape_products(url))
print(f"Total products scraped: {len(products)}")