# %%
import requests
from multiprocessing.pool import ThreadPool
import polars as pl
import numpy as np
import pandas as pd
import time
import os
from io import StringIO
from itertools import chain
from pprint import pprint
from dotenv import load_dotenv, find_dotenv

pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_width_chars(75)
pl.Config.set_fmt_str_lengths(200)

load_dotenv("../../.env")
first_portion = "paoaaHN2."
API_KEY = first_portion + os.environ.get("PDI_API_KEY")

# documentation https://github.com/amplifydata/amplifydata-public/blob/main/README.md

aws_names = ["pdi_masterGTIN", "pdi_storestatus", "pdi_stores"]
end_name_items = ["transaction_items"] 
end_names = ["discounts","transactions_daily_agg", "transaction_sets", "payments"]
end_names_nodate = ["shopper_id"]

# %%
# The functions used for this script

# check folders for proper download
def pid_rerun_check(path = "data/transaction_sets/2023/10/", size = 1000):
  files_ls = os.listdir(path)
  files_size = [os.path.getsize(path + i) for i in files_ls]
  missing = pl.DataFrame({"file_name":files_ls, "file_size":files_size})\
    .filter(pl.col("file_size") < size)
  return missing

# Get urls for downloads
def pid_get_urls(PRODUCT_API_PATH, start_date, end_date, nodate = False):
  # Figure out how many pages availble for downlod links

  if nodate == False:
    results = requests\
      .get(url=PRODUCT_API_PATH,
        params={'page': 1, # only getting 1st page of results
          'partition_key_after': start_date, # set date value here
          'partition_key_before': end_date}, # set date value here
        headers={'X-API-KEY': API_KEY, 'accept': 'application/json'})\
      .json()

    total_pages = results['total_pages']


      # Get all the downlod links from the total pages
    list_links = [requests.get(
      url=PRODUCT_API_PATH,
      params={'page': i, 'partition_key_after': start_date, 'partition_key_before': end_date},
      headers={'X-API-KEY': API_KEY, 'accept': 'application/json'}).json() for i in range(1, total_pages + 1)]

    total_pages = results['total_pages']

  if nodate == True:
    results = requests\
      .get(url=PRODUCT_API_PATH,
        params={'page': 1}, # only getting 1st page of results
        headers={'X-API-KEY': API_KEY, 'accept': 'application/json'})\
      .json()

    total_pages = results['total_pages']


    list_links = [requests.get(
      url=PRODUCT_API_PATH,
      params={'page': i},
      headers={'X-API-KEY': API_KEY, 'accept': 'application/json'}).json() for i in range(1, total_pages + 1)]
    

  
  print(str(total_pages) + " total pages with " + PRODUCT_API_PATH)
  




  # Now put links into Polars dataframe
  dat = pl.DataFrame(list_links[0])
  del list_links[0]

  for i in list_links:
      dat = pl.concat([dat, pl.DataFrame(i)], rechunk=True)
      print(i)

  out = dat.with_columns(pl.lit(PRODUCT_API_PATH).alias("api_path"))\
    .unnest("download_links").unique()
  print(out.select(pl.len()))
  return out

def download_file(textc):
  text_split = textc.split(" --- ")
  url = text_split[0]
  name = text_split[1]
  # time.sleep(2.5) 
  data = requests.get(url)
  with open(name, 'wb') as file:
    file.write(data.content)
    return(name)

def pid_download_list(dat, folder):
  os.makedirs(folder, exist_ok=True)
  return dat.sort("file_name", descending=True).with_columns(pl.concat_str(pl.lit(folder + "/"),"file_name").alias("write"))\
    .with_columns((pl.col("link") + " --- " + pl.col("write")).alias("one_text"))\
    .select("one_text")\
    .to_series().to_list()

def pid_download_links(list_dates, url="https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files", url_data="transaction_sets", threads=3):
  for i in list_dates:
    end_date_list = np.array(i, dtype='datetime64[M]')+ np.array([1], dtype='timedelta64[M]') - np.array([1], dtype='timedelta64[D]')
    end_date = end_date_list[0]
    ymd = i.split("-")
    path_i = "data/" + url_data + "/" + ymd[0] + "/" + ymd[1]
    print(path_i)
    sets_i = pid_get_urls(url, i, end_date) # transaction_sets
    list_files = pid_download_list(sets_i, path_i)
    if __name__ ==  '__main__': 
      pool = ThreadPool(threads)
      results = pool.map_async(download_file, list_files)
      pool.close()
      pool.join()
      print(results)

def pid_redownload_links(list_dates, url="https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files", url_data="transaction_sets", threads=3, byteSize=1000):
  for i in list_dates:
    end_date_list = np.array(i, dtype='datetime64[M]')+ np.array([1], dtype='timedelta64[M]') - np.array([1], dtype='timedelta64[D]')
    end_date = end_date_list[0]
    ymd = i.split("-")
    path_i = "data/" + url_data + "/" + ymd[0] + "/" + ymd[1]
    print(path_i)
    sets_all = pid_get_urls(url, i, end_date) # transaction_sets
    run_again = pid_rerun_check(path = path_i + "/", size = byteSize)
    sets_i = sets_all.join(run_again, on='file_name')
    print(sets_i.select(pl.len()))
    list_files = pid_download_list(sets_i, path_i)
    if __name__ ==  '__main__': 
      pool = ThreadPool(3)
      results = pool.map_async(download_file, list_files)
      pool.close()
      pool.join()
      print(results)

def pid_missingdownload_links(list_dates, url="https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files", url_data="transaction_sets", threads=3, byteSize=1000):
  for i in list_dates:
    end_date_list = np.array(i, dtype='datetime64[M]')+ np.array([1], dtype='timedelta64[M]') - np.array([1], dtype='timedelta64[D]')
    end_date = end_date_list[0]
    ymd = i.split("-")
    path_i = "data/" + url_data + "/" + ymd[0] + "/" + ymd[1]
    print(path_i)
    sets_all = pid_get_urls(url, i, end_date) # transaction_sets
    run_again = pid_rerun_check(path = path_i + "/", size = 90000000000000)
    sets_i = sets_all.join(run_again, on='file_name', how="anti")
    print(sets_i.select(pl.len()))
    list_files = pid_download_list(sets_i, path_i)
    if __name__ ==  '__main__': 
      pool = ThreadPool(3)
      results = pool.map_async(download_file, list_files)
      pool.close()
      pool.join()
      print(results)

# %%
# This code below runs to download all the data

# %%
list_dates = ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01', '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
              '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01', '2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01']

pid_download_links(
  list_dates,
  url="https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files",
  url_data="transaction_sets")

pid_missingdownload_links(
  list_dates,
  url="https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files",
  url_data="transaction_sets")

pid_redownload_links(
  list_dates,
  url="https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files",
  url_data="transaction_sets", byteSize=190000000)


# %%
# '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01',
# list_dates = [ '2023-06-01', '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
#               '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01', '2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01']

# pid_download_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/935801cb-043c-405e-b2de-24a47d36620d/files",
#   url_data="transaction_items")

# list_dates = ['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01', '2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01']

# pid_missingdownload_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/935801cb-043c-405e-b2de-24a47d36620d/files",
#   url_data="transaction_items")

# pid_redownload_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/935801cb-043c-405e-b2de-24a47d36620d/files",
#   url_data="transaction_items", byteSize=190000000)

# %%
# list_dates = ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01', '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
#               '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01', '2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01']

# pid_download_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/9f1198d7-f3c8-4351-8557-ab2de0c580c1/files",
#   url_data="discounts")

# pid_missingdownload_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/9f1198d7-f3c8-4351-8557-ab2de0c580c1/files",
#   url_data="discounts")

# pid_redownload_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/9f1198d7-f3c8-4351-8557-ab2de0c580c1/files",
#   url_data="discounts", byteSize=190000000)

# %%
# list_dates = ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01', '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
#               '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01', '2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01']

# pid_download_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/4e393e1c-bde7-4c5a-a6ab-207abeffd733/files",
#   url_data="transactions_daily_agg")

# pid_redownload_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/4e393e1c-bde7-4c5a-a6ab-207abeffd733/files",
#   url_data="transactions_daily_agg")

# pid_redownload_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/4e393e1c-bde7-4c5a-a6ab-207abeffd733/files",
#   url_data="transactions_daily_agg", byteSize=190000000)

# %%
# list_dates = ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01', '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
#               '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01', '2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01']

# pid_download_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/97595252-5da9-4cfe-b063-7794159d997c/files",
#   url_data="payments")

# pid_missingdownload_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/97595252-5da9-4cfe-b063-7794159d997c/files",
#   url_data="payments")

# pid_redownload_links(
#   list_dates,
#   url="https://app.deweydata.io/external-api/v3/products/97595252-5da9-4cfe-b063-7794159d997c/files",
#   url_data="payments", byteSize=190000000)

# %%
# Next ones
shopper_i = pid_get_urls("https://app.deweydata.io/external-api/v3/products/3291566f-9454-49e3-a4b3-3b3a557f1bee/files", 'x', 'x', nodate=True) # shopperid

# # %%
# sets_i = pid_get_urls("https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files", '2023-05-01', '2023-05-31') # transaction_sets
# list_files = pid_download_list(sets_i, "data/transaction_sets/2023/05")
# if __name__ ==  '__main__': 
#     pool = ThreadPool(3)
#     results = pool.map_async(download_file, list_files)
#     pool.close()
#     pool.join()
#     print(results)

# # %%
# sets_i = pid_get_urls("https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files", '2023-06-01', '2023-06-30') # transaction_sets
# list_files = pid_download_list(sets_i, "data/transaction_sets/2023/06")
# if __name__ ==  '__main__': 
#     pool = ThreadPool(3)
#     results = pool.map_async(download_file, list_files)
#     pool.close()
#     pool.join()
#     print(results)

# # %%
# sets_i = pid_get_urls("https://app.deweydata.io/external-api/v3/products/89c21467-16d0-4302-87cf-4e59a79b7fce/files", '2023-07-01', '2023-07-31') # transaction_sets
# list_files = pid_download_list(sets_i, "data/transaction_sets/2023/07")
# if __name__ ==  '__main__': 
#     pool = ThreadPool(3)
#     results = pool.map_async(download_file, list_files)
#     pool.close()
#     pool.join()
#     print(results)