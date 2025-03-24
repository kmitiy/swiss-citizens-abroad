import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine, text, Engine
import random
from typing import Any, Dict, TypeAlias


# Define custom type alias for JSON data to allow for proper type annotation
JSON: TypeAlias = Dict[str, Any]

# Fetch data via GET from a given API
def fetch_data(i_url: str, i_headers: Dict[str, str]) -> JSON:
    response = requests.get(i_url, headers=i_headers)
    if response.status_code != 200:
        print(f'Failed to fetch data. Status code: {response.status_code}')
    return response.json()

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

        # Define your SELECT query to count rows
        query = f'SELECT COUNT(*) FROM {i_schema_name}.{i_table_name} WHERE {i_col_name} = {load_id}'

        # Establish connection and execute the query
        with i_engine.connect() as con:
            result = con.execute(text(query))  # Using text() for raw SQL
            count = result.scalar()  # This fetches the first column of the first row (the count)

        # Check if the count is 0
        if count == 0:
            return load_id

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
    engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

    # Create unique load id and extract relevant source data into df
    schema_name = 'steering'
    table_name = 'bfs_publications'
    load_id = generate_unique_id(engine, schema_name, table_name, 'load_id')
    df = transform_data(src_data, load_id)

    # Write df to our BFS DB
    df.to_sql(table_name, engine, schema_name, if_exists='append', index=False)

    # Explicitly close the connection
    engine.dispose()

if __name__ == '__main__':
    main()