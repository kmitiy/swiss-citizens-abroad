import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine, text
import psycopg2
import random


def generate_unique_id(i_engine, i_schema, i_table_name, i_col_name):
    while True:
        # Generate a random 6-digit number
        load_id = str(random.randint(100000, 999999))

        # Define your SELECT query to count rows
        query = f'SELECT COUNT(*) FROM {i_schema}.{i_table_name} WHERE {i_col_name} = {load_id}'

        # Establish connection and execute the query
        with i_engine.connect() as con:
            result = con.execute(text(query))  # Using text() for raw SQL
            count = result.scalar()  # This fetches the first column of the first row (the count)

        # Check if the count is 0
        if count == 0:
            return load_id

# Database credentials
db_username = 'kmitiy'
db_password = ''
db_host = 'localhost'
db_port = '5432'  # Default PostgreSQL port
db_name = 'bfs'
schema_name = 'steering'
table_name = 'bfs_publications'

# Create database connection
engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

url = 'https://dam-api.bfs.admin.ch/hub/api/dam/agenda'

headers = {
    'accept': 'application/json',
    'Accept-Language': 'de',
}

params = {
    # 'title': '%Auslandschweizer%',
}

response = requests.get(url=url, params=params, headers=headers)

if response.status_code != 200:
    print(f'Failed to fetch data. Status code: {response.status_code}')

response_json = response.json()

load_id = generate_unique_id(i_engine=engine, i_schema=schema_name, i_table_name=table_name, i_col_name='load_id')

extracted_data = []
for item in response_json['data']:
    extracted_data.append({
        'load_id': load_id,
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

# Convert to Pandas DataFrame
df = pd.DataFrame(extracted_data)

# Write DataFrame to PostgreSQL (replace if table exists)
df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', index=False)

# Optionally, close the connection
engine.dispose()

print('Data successfully written to PostgreSQL!')
