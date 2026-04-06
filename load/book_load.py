import traceback
from sqlalchemy import create_engine, text
from etl_books.utils import logger

# DB details
DB_URI = "mysql+pymysql://root:yash@localhost:3308/bombay_shavings"


def create_table():
    logger.write_log({'function': 'create_table', 'status': 'started'})

    try:
        engine = create_engine(DB_URI) # connecting DB

        with engine.begin() as conn:
            # creates table if not exists
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS books (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) UNIQUE,
                price FLOAT,
                rating INT,
                availability VARCHAR(50),
                category VARCHAR(100),
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """))

        logger.write_log({'function': 'create_table', 'status': 'completed'})

    except Exception as e:
        logger.write_log({
            'function': 'create_table',
            'stage': 'fatal',
            'exception': str(e),
            'exception_type': type(e).__name__,
            'trace': traceback.format_exc()
        })


def load_data(df):
    logger.write_log({'function': 'load_data', 'status': 'started'})

    try:
        # 🔹 Validate input
        if df is None or df.empty:
            logger.write_log({
                'function': 'load_data', 'stage': 'validation', 'reason': 'Empty DataFrame'
            })
            return

        engine = create_engine(DB_URI)

        inserted_count = 0
        failed_count = 0

        with engine.begin() as conn:
            for _, row in df.iterrows():
                try:
                    # avoiding duplicates and inserting in DB
                    conn.execute(text("""
                        INSERT IGNORE INTO books 
                        (title, price, rating, availability, category, description)
                        VALUES (:title, :price, :rating, :availability, :category, :description)
                    """), {
                        "title": row.get('title'),
                        "price": row.get('price'),
                        "rating": row.get('rating'),
                        "availability": row.get('availability'),
                        "category": row.get('category'),
                        "description": row.get('description')
                    })

                    inserted_count += 1

                except Exception as e:
                    failed_count += 1

                    logger.write_log({
                        'function': 'load_data', 'stage': 'row_insert',
                        'title': row.get('title', 'unknown'), 'exception': str(e),
                        'exception_type': type(e).__name__, 'trace': traceback.format_exc()
                    })

                    continue

        logger.write_log({
            'function': 'load_data', 'status': 'completed',
            'inserted_records': inserted_count, 'failed_records': failed_count
        })

    except Exception as e:
        logger.write_log({
            'function': 'load_data','stage': 'fatal', 'exception': str(e),
            'exception_type': type(e).__name__, 'trace': traceback.format_exc()})