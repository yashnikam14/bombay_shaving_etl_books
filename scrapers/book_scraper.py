# import asyncio
import logging
from playwright.async_api import async_playwright
from etl_books.utils import logger
import traceback
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO)

BASE_URL = "https://books.toscrape.com/"

async def scrape_books():
    logger.write_log({'function': 'scrape_books', 'status': 'started'})
    books = []
    browser = None

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True) # opens browser in background
            page = await browser.new_page()

            await page.goto(BASE_URL)  # loads website
            page_number = 1
            while True:
                items = await page.query_selector_all(".product_pod") # gets all books on page

                for item in items:
                    try:
                        link = await item.query_selector("h3 a")

                        if not link:
                            logger.write_log({
                                'function': 'scrape_books',
                                'reason': 'Link not found'
                            })
                            continue

                        # pulls title, price, rating, etc
                        title = await link.get_attribute("title")
                        detail_url = await link.get_attribute("href")

                        price_el = await item.query_selector(".price_color")
                        price = await price_el.inner_text() if price_el else ""

                        rating_el = await item.query_selector(".star-rating")
                        rating_class = await rating_el.get_attribute("class") if rating_el else ""

                        availability_el = await item.query_selector(".availability")
                        availability = await availability_el.inner_text() if availability_el else ""

                        detail_page = await browser.new_page()

                        try:

                            full_url = urljoin(page.url, detail_url)
                            await detail_page.goto(full_url) # detail page scraping

                            desc_el = await detail_page.query_selector("#product_description + p")
                            description = await desc_el.inner_text() if desc_el else ""

                            cat_el = await detail_page.query_selector("ul.breadcrumb li:nth-child(3) a")
                            category = await cat_el.inner_text() if cat_el else ""

                        except Exception as e:
                            logger.write_log({
                                'function': 'scrape_books',
                                'stage': 'detail_page',
                                'title': title,
                                'detail_url': detail_url,
                                'exception': str(e),
                                'exception_type': type(e).__name__,
                                'trace': traceback.format_exc()
                            })
                            description = ""
                            category = ""

                        finally:
                            await detail_page.close()

                        books.append({
                            "title": title,
                            "price": price,
                            "rating": rating_class,
                            "availability": availability.strip(),
                            "category": category,
                            "description": description.strip()
                        })

                        await page.wait_for_timeout(500)

                    except Exception as e:
                        logger.write_log({'function': 'scrape_books', 'stage': 'listing_page',
                                          'title': title if 'title' in locals() else 'unknown', 'exception': str(e),
                                          'exception_type': type(e).__name__, 'trace': traceback.format_exc()})
                        continue

                print({
                    'function': 'scrape_books', 'stage': 'page_completed',
                    'page_number': page_number, 'books_collected_so_far': len(books)})
                logger.write_log({
                    'function': 'scrape_books', 'stage': 'page_completed',
                    'page_number': page_number, 'books_collected_so_far': len(books)})
                next_btn = await page.query_selector(".next a") # moves to next page until last

                if next_btn:
                    next_page = await next_btn.get_attribute("href")
                    next_url = urljoin(page.url, next_page)
                    await page.goto(next_url)
                    page_number += 1
                else:
                    break

    except Exception as e:
        logger.write_log({'function': 'scrape_books', 'stage': 'fatal', 'exception': str(e),
                          'exception_type': type(e).__name__, 'trace': traceback.format_exc()})

    finally:
        if browser:
            await browser.close()

        logger.write_log({'function': 'scrape_books', 'status': 'completed', 'total_books': len(books)})

    return books