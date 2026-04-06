import pandas as pd
import traceback
from etl_books.utils import logger


def transform_data(data):
    logger.write_log({'function': 'transform_data', 'status': 'started'})

    try:
        if not data:
            logger.write_log({'function': 'transform_data', 'stage': 'validation', 'reason': 'Empty input data'})
            return pd.DataFrame()

        df = pd.DataFrame(data) # dataframing

        # cleaning the data
        try:
            df['price'] = df['price'].str.replace('£', '', regex=False).astype(float)
        except Exception as e:
            logger.write_log({'function': 'transform_data', 'stage': 'price_cleaning',
                'exception': str(e), 'exception_type': type(e).__name__, 'trace': traceback.format_exc()})
            df['price'] = None

        try:
            rating_map = {
                "One": 1, "Two": 2, "Three": 3,
                "Four": 4, "Five": 5
            }

            df['rating'] = df['rating'].str.extract(r'(One|Two|Three|Four|Five)')
            df['rating'] = df['rating'].map(rating_map)

        except Exception as e:
            logger.write_log({
                'function': 'transform_data',
                'stage': 'rating_cleaning',
                'exception': str(e),
                'exception_type': type(e).__name__,
                'trace': traceback.format_exc()
            })
            df['rating'] = None

        try:
            df['availability'] = df['availability'].astype(str).str.strip()
        except Exception as e:
            logger.write_log({'function': 'transform_data', 'stage': 'availability_cleaning', 'exception': str(e),
                'exception_type': type(e).__name__, 'trace': traceback.format_exc()})

        try:
            df['description'] = df['description'].astype(str).str.strip()
        except Exception as e:
            logger.write_log({'function': 'transform_data', 'stage': 'description_cleaning',
                'exception': str(e), 'exception_type': type(e).__name__, 'trace': traceback.format_exc()})

        # removes duplicate
        try:
            df.drop_duplicates(subset=['title'], inplace=True)
        except Exception as e:
            logger.write_log({'function': 'transform_data','stage': 'deduplication', 'exception': str(e),
                'exception_type': type(e).__name__, 'trace': traceback.format_exc()})

        logger.write_log({'function': 'transform_data', 'status': 'completed', 'total_records': len(df)})

        return df

    except Exception as e:
        logger.write_log({'function': 'transform_data', 'stage': 'fatal', 'exception': str(e),
            'exception_type': type(e).__name__, 'trace': traceback.format_exc()})
        return pd.DataFrame()