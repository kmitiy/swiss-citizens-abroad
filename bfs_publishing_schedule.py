import requests
import pandas as pd
import datetime


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

extracted_data = []
for item in response_json['data']:
    extracted_data.append({
        'excuted_on': datetime.datetime.now(),
        'uuid': item['ids']['uuid'],
        'gnp': item['ids']['gnp'],
        'dam_id': item['ids']['damId'],        
        'title': item['description']['titles']['main'],
        'publishing_date': item['bfs']['embargo'],
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
        'short_text_gnp': item['description']['shortTextGnp'],
        'languages': (','.join(item['description']['languages']))        
    })

# Convert to Pandas DataFrame
df = pd.DataFrame(extracted_data)

# Display DataFrame
print(df)

