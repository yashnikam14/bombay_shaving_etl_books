import asyncio
from etl_books.scrapers.book_scraper import scrape_books
from etl_books.transform.book_transform import transform_data
from etl_books.load.book_load import create_table, load_data

def run_pipeline():
    data = asyncio.run(scrape_books())
    df = transform_data(data)
    create_table()
    load_data(df)

if __name__ == "__main__":
    run_pipeline()