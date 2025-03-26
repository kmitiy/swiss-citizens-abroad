import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import random
from typing import Dict
from logs.logging_setup import logger
from custom_type_annotations import JSON


# Fetch data via GET from a given API
def fetch_data(i_url: str, i_headers: Dict[str, str]) -> JSON:
    try:
        response = requests.get(i_url, headers=i_headers)
        response.raise_for_status()  # Raises an HTTPError for 4xx/5xx responses
        logger.info('Successfully fetched source data from BFS API')
        response_json = response.json()
        if response_json == {}:
            logger.error('Source data from BFS API is empty')
        return  response.json()

    except requests.exceptions.HTTPError as e:
        logger.error(f'HTTP error occurred: {e}')
        raise

    except requests.exceptions.RequestException as e:
        # Catch network-related issues, including timeouts, DNS issues, etc.
        logger.error(f'Error during request: {e}')
        raise

    except ValueError:
        # Handle the case where the response body is not valid JSON
        logger.error('Error: Response is not valid JSON')
        raise

# Transform BFS publishing data into a usable df
def transform_data(i_src_json: JSON, i_load_id: int) -> pd.DataFrame:
    # Will be a list of dictionaries, the keys being the headers, the values being the row values
    transformed_data = []
    src_data = i_src_json['data']  # Store in a variable for clarity
    for item in src_data:
        transformed_data.append({
            'load_id': i_load_id,
            'created_ts': datetime.datetime.now(),
            'uuid': item['ids']['uuid'],
            'gnp': item['ids']['gnp'],
            'dam_id': item['ids']['damId'],
            'title': item['description']['titles']['main'],
            'published_ts': item['bfs']['embargo'],
            'institution_lvl_0_id': item['description']['categorization']['institution'][0]['id'],
            'institution_lvl_1_id': item['description']['categorization']['institution'][1]['id'],
            'institution_lvl_0_name': item['description']['categorization']['institution'][0]['name'],
            'institution_lvl_1_name': item['description']['categorization']['institution'][1]['name'],
            'prodima_lvl_0_id': item['description']['categorization']['prodima'][0]['id'],
            'prodima_lvl_1_id': item['description']['categorization']['prodima'][1]['id'],
            'prodima_lvl_0_code': item['description']['categorization']['prodima'][0]['code'],
            'prodima_lvl_1_code': item['description']['categorization']['prodima'][1]['code'],
            'prodima_lvl_0_name': item['description']['categorization']['prodima'][0]['name'],
            'prodima_lvl_1_name': item['description']['categorization']['prodima'][1]['name'],
            'short_text_gnp': item['description']['shortTextGnp']['raw'],
            'languages': (','.join(item['description']['languages']))
        })
    return pd.DataFrame(transformed_data)

# Generate a six digit long ID for each load. Make sure that it is not already assigned in the target table
def generate_unique_id(i_engine: Engine, i_schema_name: str, i_table_name: str, i_col_name: str) -> int:
    while True:
        # Generate a random 6-digit number
        load_id = random.randint(100000, 999999)
        logger.info(f'Load ID {load_id} was generated for this data load')

        # Define your SELECT query to count rows
        query = f'SELECT COUNT(*) FROM {i_schema_name}.{i_table_name} WHERE {i_col_name} = {load_id}'

        # Establish connection and execute the query
        with i_engine.connect() as con:
            result = con.execute(text(query))  # Using text() for raw SQL
            logger.info(f'Executing query: "{query}"')
            count = result.scalar()  # This fetches the first column of the first row (the count)

        # Check if the count is 0
        if count == 0:
            logger.info(f'Generated load ID {load_id} was not found in column {i_col_name.upper()} of target table {i_table_name.upper()} in schema {i_schema_name.upper()}, so it can be used')
            return load_id
        else:
            logger.info(f'Generated load ID {load_id} already exists in column {i_col_name.upper()} of target table {i_table_name.upper()} in schema {i_schema_name.upper()}, so a different one will be generated')

# Executed in "if __name__ == '__main__'" block
def main():

    # Fetch source data from the BFS API
    url = 'https://dam-api.bfs.admin.ch/hub/api/dam/agenda'
    headers = {
        'accept': 'application/json',
        'Accept-Language': 'de',
    }
    src_data = fetch_data(url, headers)

    # Create SQLAlchemy engine as an entrypoint to our BFS DB
    db_username = 'kmitiy'
    db_password = ''
    db_host = 'localhost'
    db_port = '5432'  # Default PostgreSQL port
    db_name = 'bfs'
    try:
        logger.info(f'Trying to connect to the database:\nUsername -> "{db_username}"\nPassword -> [REDACTED]\nHost -> "{db_host}"\nPort -> "{db_port}"\nDB Name -> "{db_name}"')
        engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
    except OperationalError as e:
        logger.error(f'OperationalError: Could not connect to the database. {str(e)}')
        raise
    except Exception as e:
        logger.error(f'Unexpected error: {str(e)}')
        raise

    # Create unique load id and extract relevant source data into df
    schema_name = 'steering'
    table_name = 'bfs_publications'
    load_id = generate_unique_id(engine, schema_name, table_name, 'load_id')
    df = transform_data(src_data, load_id)

    # Write df to our BFS DB
    try:
        df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
        logger.info(f'Successfully inserted {len(df.index)} records into table {table_name.upper()} of schema {schema_name.upper()}')
    except SQLAlchemyError as e:
        logger.error(f'SQLAlchemyError: An error occurred while inserting data into the table. {str(e)}')
        raise
    except Exception as e:
        logger.error(f'Unexpected error: {str(e)}')
        raise

    # Explicitly close the connection
    engine.dispose()
    logger.info(f'Connection to {engine.url.database.upper()} is closed')

if __name__ == '__main__':
    logger.info('Started execution of main block')
    main()
    logger.info('Finished execution of main block')