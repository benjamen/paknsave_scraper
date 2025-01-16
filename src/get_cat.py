import asyncio
import logging
from playwright.async_api import async_playwright

# Configure logging
logging.basicConfig(level=logging.INFO)

async def fetch_product_categories(product_url: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            # Navigate to the product page
            await page.goto(product_url, wait_until="domcontentloaded")
            await asyncio.sleep(5)  # Wait for a short time to allow content to load

            # Wait for an alternative element or increase timeout
            await page.wait_for_selector('nav[aria-label="Breadcrumbs"]', timeout=60000)

            # Initialize a list to hold the relevant categories
            categories = []

            # Extract categories using data-testid
            for i in range(3):  # We expect three categories
                category = await page.query_selector(f'[data-testid="product-category-{i}"] p')
                if category:
                    categories.append(await category.inner_text())

            # Join the relevant categories with " - "
            result = " - ".join(categories)

            logging.info(f"Extracted categories: {result}")
            return result

        except Exception as e:
            logging.error(f"Error fetching product categories: {e}")
            return ""

        finally:
            await page.close()
            await browser.close()

async def get_categories_async(url: str) -> str:
    """Fetches product categories from the given URL asynchronously."""
    return await fetch_product_categories(url)

def get_categories(url: str) -> str:
    """Fetches product categories from the given URL. This function is synchronous."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If the loop is running, create a new task and return it
            task = loop.create_task(get_categories_async(url))
            return asyncio.run(task)
        else:
            return loop.run_until_complete(get_categories_async(url))
    except RuntimeError as e:
        logging.error(f"An error occurred: {e}")
        return ""

# If this script is run directly, you can test it with a URL
if __name__ == "__main__":
    product_url = "https://www.paknsave.co.nz/shop/product/5028110_ea_000pns?name=-avocado"
    categories = get_categories(product_url)
    print(categories)