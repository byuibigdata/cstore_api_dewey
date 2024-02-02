# %%
import requests
import polars as pl
import os
from pprint import pprint
from dotenv import load_dotenv
load_dotenv()
API_KEY = os.environ.get("PDI_API_KEY")

ep_names = [
    "transaction_items",
    "daily_transaction_aggregations",
    "store_information",
    "credit_card_ayments",
    "discounts",
    "transaction_sets",
    "shopper_id",
    "gtin_master_file",
    "payments",
    "store_status",
    "store_information"

]

endpoints = [
    "https://app.deweydata.io/external-api/v3/products/5d474ecc-389c-4057-b105-fd5d079e4a5c/files",
    "https://app.deweydata.io/external-api/v3/products/81f00a32-da42-4e45-b072-4a2db6e05a32/files",
    "https://app.deweydata.io/external-api/v3/products/e134b3ff-671c-4566-8e41-17ab64395b0b/files",
    "https://app.deweydata.io/external-api/v3/products/5ad40e0b-afd8-4764-a7f4-d23502511566/files",
    "https://app.deweydata.io/external-api/v3/products/49433fe0-c25e-4348-a8a9-1dd57dfcce4a/files",
    "https://app.deweydata.io/external-api/v3/products/08b48f19-9cbf-441c-abfc-3529c784702f/files",
    "https://app.deweydata.io/external-api/v3/products/b4229432-c324-481d-a416-37386a0b0a8d/files",
    "https://app.deweydata.io/external-api/v3/products/dfc72515-51f0-4363-842f-a9b3b38cdfd0/files",
    "https://app.deweydata.io/external-api/v3/products/f4048bae-bd39-416e-a501-a620e66b285f/files",
    "https://app.deweydata.io/external-api/v3/products/0dbf3f6b-1e5f-4615-b65a-777274f48f50/files",
    "https://app.deweydata.io/external-api/v3/products/e134b3ff-671c-4566-8e41-17ab64395b0b/files"
]
PRODUCT_API_PATH = endpoints[1]

# %%
# Metadata Information
results = requests.get(url=PRODUCT_API_PATH+"/metadata",
                       headers={
                        "X-API-KEY": API_KEY,
                        'accept': 'application/json'
                       })
pprint(results.json())
# {'max_partition_key': '2023-12-01',
#  'min_partition_key': '2019-07-01',
#  'partition_aggregation': 'MONTH',
#  'partition_column': 'DATE_TIME',
#  'total_files': 13524,
#  'total_size': 2830460926441}
# %%
results = requests.get(url=PRODUCT_API_PATH,
                       params={'page': 1, # only getting 1st page of results
                               'partition_key_after': '1900-01-01', # set date value here
                               'partition_key_before': '2099-01-01'}, # set date value here
                       headers={'X-API-KEY': API_KEY,
                                'accept': 'application/json'
                               })
pprint(results.json())
# %%
page = 1
download_count = 0
while True:
    # get results from API endpoint, using API key for authentication
    results = requests.get(url=PRODUCT_API_PATH,
                           params={'page': page,
                                   'partition_key_after': '2023-10-01',   # set date value here
                                   'partition_key_before': '2023-12-31'}, # set date value here
                           headers={'X-API-KEY': API_KEY,
                                    'accept': 'application/json'
                                   })
    response_json = results.json()
    
        # for each result page, loop through download links and save to your computer
    for link_data in response_json['download_links']:
        print(f"Downloading file {link_data['file_name']}...")
        data = requests.get(link_data['link'])
        with open(link_data['file_name'], 'wb') as file:
            file.write(data.content)
        download_count += 1

    # only continue if there are more result pages to process
    total_pages = response_json['total_pages']
    if page >= total_pages:
        break
    page += 1

print(f"Successfully downloaded {download_count} files.")
# %%
