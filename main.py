import asyncio
import csv
import ssl
import aiohttp
import certifi
from bs4 import BeautifulSoup

BASE_URL = "https://store.igefa.de/"
CATEGORY_URL = 'https://store.igefa.de/c/kategorien/f4SXre6ovVohkGNrAvh3zR'

ssl_context = ssl.create_default_context(cafile=certifi.where())

async def fetch_product_links(session, url):
    async with session.get(url) as response:
        html = await response.text()
        soup = BeautifulSoup(html, "html.parser")

        product_links = []
        for link in soup.select('.product-list-item a'):
            product_url = BASE_URL + link['href']
            product_links.append(product_url)

        print("Fetched product links:", product_links)
        return product_links

async def fetch_product_details(session, product_url):
    async with session.get(product_url) as response:
        html = await response.text()
        soup = BeautifulSoup(html, "html.parser")

        product_data = {
            "Product Name": soup.select_one('.ProductInformation_productTitle__61297').text.strip() if soup.select_one(
                '.ProductInformation_productTitle__61297') else None,
            "Original Data Column 1 (Breadcrumb)": soup.select_one(
                '.CategoryBreadcrumbs_sectionWrap__b5732').text.strip() if soup.select_one(
                '.CategoryBreadcrumbs_sectionWrap__b5732') else None,
            "Original Data Column 2 (Ausführung)": soup.select_one(
                '.ProductCard_paragraph__03d53').text.strip() if soup.select_one(
                '.ProductCard_paragraph__03d53') else None,
            "Supplier Article Number": (
                soup.select_one('[data-testid="product-information-sku"]').text.strip().split(': ')[1]
                if soup.select_one('[data-testid="product-information-sku"]') else None
            ),
            "EAN/GTIN": (
                soup.select_one('[data-testid="product-information-gtin"]').text.strip().split(': ')[1]
                if soup.select_one('[data-testid="product-information-gtin"]') else None
            ),
            "Article Number": (
                soup.select_one('.ant-typography-secondary[data-testid="product-information-sku"]').text.strip().split(
                    ': ')[1]
                if soup.select_one('.ant-typography-secondary[data-testid="product-information-sku"]') else None
            ),
            "Product Description": (
                soup.select_one('.ProductDescription_description__4e5b7 p').text.strip()
                if soup.select_one('.ProductDescription_description__4e5b7 p') else None
            ),
            "Supplier": "igefa Handelsgesellschaft" if "igefa Handelsgesellschaft" in soup.text else None,
            "Supplier-URL": product_url,
            "Product Image URL": soup.select_one('.image-gallery-image')['src'] if soup.select_one(
                '.image-gallery-image') else None,
            "Manufacturer": soup.select_one('tr[data-row-key="33"] td:nth-child(2)').text.strip() if soup.select_one(
                'tr[data-row-key="33"] td:nth-child(2)') else None,
            "Original Data Column 3 (Add. Description)": ' '.join(li.text.strip() for li in soup.select(
                '.ProductBenefits_productBenefits__1b77a li')) if soup.select(
                '.ProductBenefits_productBenefits__1b77a li') else None,
        }

        print("Fetched product data:", product_data)
        return product_data

async def main():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        product_links = await fetch_product_links(session, CATEGORY_URL)

        tasks = [fetch_product_details(session, link) for link in product_links]
        all_product_data = await asyncio.gather(*tasks)

        with open('products.csv', mode='w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                "Product Name",
                "Original Data Column 1 (Breadcrumb)",
                "Original Data Column 2 (Ausführung)",
                "Supplier Article Number",
                "EAN/GTIN",
                "Article Number",
                "Product Description",
                "Supplier",
                "Supplier-URL",
                "Product Image URL",
                "Manufacturer",
                "Original Data Column 3 (Add. Description)"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for data in all_product_data:
                if data:
                    writer.writerow(data)

if __name__ == "__main__":
    asyncio.run(main())
