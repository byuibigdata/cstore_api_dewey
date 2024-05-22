# %%
import requests
import polars as pl
import os
from io import StringIO
from itertools import chain
from pprint import pprint
from dotenv import load_dotenv, find_dotenv
load_dotenv("../../.env")
first_portion = "paoaaHN2."
API_KEY = first_portion + os.environ.get("PDI_API_KEY")

# documentation https://github.com/amplifydata/amplifydata-public/blob/main/README.md

aws_names = ["pdi_masterGTIN", "pdi_storestatus", "pdi_stores"]
end_names = ["transaction_items","discounts","transactions_daily_agg", "transaction_sets", "payments"]
end_names_nodate = ["shopper_id"]
endpoints = [
    "https://app.deweydata.io/external-api/v3/products/935801cb-043c-405e-b2de-24a47d36620d/files",
    "https://app.deweydata.io/external-api/v3/products/9f1198d7-f3c8-4351-8557-ab2de0c580c1/files",
    "https://app.deweydata.io/external-api/v3/products/4e393e1c-bde7-4c5a-a6ab-207abeffd733/files",
    "https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files",
    "https://app.deweydata.io/external-api/v3/products/97595252-5da9-4cfe-b063-7794159d997c/files",
]
endpoints_nodate = ["https://app.deweydata.io/external-api/v3/products/3291566f-9454-49e3-a4b3-3b3a557f1bee/files"]
metadata_path = "/metadata"
PRODUCT_API_PATH = endpoints[0]
start_date = '2021-01-01'
end_date = '2024-03-01'

# %%
en_loop = 0
for PRODUCT_API_PATH in endpoints:
  results = requests\
    .get(url=PRODUCT_API_PATH,
      params={'page': 1, # only getting 1st page of results
        'partition_key_after': start_date, # set date value here
        'partition_key_before': end_date}, # set date value here
      headers={'X-API-KEY': API_KEY, 'accept': 'application/json'})\
    .json()

  total_pages = results['total_pages']
  print("Total pages for " + PRODUCT_API_PATH)
  print(total_pages)

  list_links = [requests.get(
      url=PRODUCT_API_PATH,
      params={'page': i, 'partition_key_after': start_date, 'partition_key_before': end_date},
      headers={'X-API-KEY': API_KEY, 'accept': 'application/json'}).json() for i in range(1, total_pages + 1)]

  first = 0
  for i in list_links:
    if first == 0:
      dat = pl.DataFrame(i)
    if first >= 0:
      dat = pl.concat([dat, pl.DataFrame(i)], rechunk=True)
  print(first)
  first += 1      

  if en_loop == 0:
     out = dat.with_columns(pl.lit(PRODUCT_API_PATH).alias("api_path"))
  if en_loop >= 0:
     out = pl.concat([out, dat.with_columns(pl.lit(PRODUCT_API_PATH).alias("api_path"))])
  print("Endpoint Loop")
  print(en_loop)
  en_loop += 1

dat_links_datechunk = out.unnest("download_links")

# %%
en_loop = 0
for PRODUCT_API_PATH in endpoints_nodate:
  results = requests\
    .get(url=PRODUCT_API_PATH,
      params={'page': 1}, # only getting 1st page of results
      headers={'X-API-KEY': API_KEY, 'accept': 'application/json'})\
    .json()

  total_pages = results['total_pages']
  print("Total pages for " + PRODUCT_API_PATH)
  print(total_pages)

  list_links = [requests.get(
      url=PRODUCT_API_PATH,
      params={'page': i},
      headers={'X-API-KEY': API_KEY, 'accept': 'application/json'}).json() for i in range(1, total_pages + 1)]

  first = 0
  for i in list_links:
    if first == 0:
      dat = pl.DataFrame(i)
    if first >= 0:
      dat = pl.concat([dat, pl.DataFrame(i)], rechunk=True)
  print(first)
  first += 1      

  if en_loop == 0:
     out = dat.with_columns(pl.lit(PRODUCT_API_PATH).alias("api_path"))
  if en_loop >= 0:
     out = pl.concat([out, dat.with_columns(pl.lit(PRODUCT_API_PATH).alias("api_path"))])
  print("Endpoint Loop")
  print(en_loop)
  en_loop += 1

dat_links = out.unnest("download_links")


# %%
download_links = pl.concat([dat_links_datechunk, dat_links])\
  .join(pl.DataFrame({"folder_name":end_names + end_names_nodate,
                      "api_path":endpoints + endpoints_nodate}), how="left", on="api_path")\
  .with_columns(pl.concat_str("folder_name", pl.lit("/"),"file_name").alias("write"))\
  .with_columns(pl.struct(["link", "write"]).alias("run"))





# %%
dlinks = download_links.group_by("api_path")\
  .agg(
    pl.col("link").first(),
    pl.col("run").first())

urls = dlinks.select("link").to_series().to_list()
names = dlinks.select("run").to_series().to_list()


# %%
def download_file(url, name):
  data = requests.get(url)
  with open(name, 'wb') as file:
    file.write(data.content)
    return(name)
    
# %%
dlinks.with_columns(pl.col("run").map_elements(lambda cols: download_file(cols["link"], cols["write"])))
# %%
