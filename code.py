# Virtual environment- Crypto
# 1. Creator and all its collection table  <----
# 2. Collection table <---- asset dataset
# 3. Asset table <---- asset dataset
# 4. Event table <---- 
# 5. Creator name and collection <---- 

# -------------------------------------------------------- IMPORT
# import collections
from dataclasses import dataclass
from multiprocessing import parent_process
from tokenize import PlainToken
from tqdm import trange
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from os.path import isfile, join
from os import listdir
import requests
import json
import time
import pandas as pd
import matplotlib.pyplot as plt
import warnings
import ast
import datetime
warnings.filterwarnings("ignore", category=FutureWarning)

api_key = <api_key>

def collections(api_key):
  df = pd.read_csv('data/opensea_collections.csv')
  slug = df.slug.tolist()
  f = {} # dictionary to store all slug that were not processes 
  df1 = pd.DataFrame(columns=['editors', 'payment_tokens', 'primary_asset_contracts',
        'banner_image_url', 'chat_url', 'created_date', 'default_to_fiat',
        'description', 'dev_buyer_fee_basis_points',
        'dev_seller_fee_basis_points', 'discord_url', 'external_url',
        'featured', 'featured_image_url', 'hidden', 'safelist_request_status',
        'image_url', 'is_subject_to_whitelist', 'large_image_url',
        'medium_username', 'name', 'only_proxied_transfers',
        'opensea_buyer_fee_basis_points', 'opensea_seller_fee_basis_points',
        'payout_address', 'require_email', 'short_description', 'slug',
        'telegram_url', 'twitter_username', 'instagram_username', 'wiki_url',
        'is_nsfw', 'stats.one_day_volume', 'stats.one_day_change',
        'stats.one_day_sales', 'stats.one_day_average_price',
        'stats.seven_day_volume', 'stats.seven_day_change',
        'stats.seven_day_sales', 'stats.seven_day_average_price',
        'stats.thirty_day_volume', 'stats.thirty_day_change',
        'stats.thirty_day_sales', 'stats.thirty_day_average_price',
        'stats.total_volume', 'stats.total_sales', 'stats.total_supply',
        'stats.count', 'stats.num_owners', 'stats.average_price',
        'stats.num_reports', 'stats.market_cap', 'stats.floor_price',
        'display_data.card_display_style','display_data.images']) 

  # for i in trange(len(slug)):
  c = 0
  for i in trange(100):
    url = "https://api.opensea.io/api/v1/collection/" + slug[i]
    headers = {"X-API-KEY": api_key }
    response = requests.get(url, headers=headers)
    c+=1
    # if any status code 429
    if c%15 == 0:     # After how many records to sleep
      time.sleep(4)  # time to sleep

    if response.status_code != 200:
      print('Slug: ',slug[i],' Status Code: ', response.status_code)
      # print('wtf')
      f[slug[i]]=response.status_code
    
    else:
      json_data = json.loads(response.text)
      df2 = pd.json_normalize(json_data['collection'])
      # print(len(df2.columns))
      filename = str(slug[i])+'collection.csv'
      # df1.concat(df2)
      df1 =pd.concat([df1,df2])
      # df2.to_csv(filename)
      time.sleep(0.10)
  df1.to_csv('collection_info.csv')
  
  # Store all slugs that got some other status_code than 200
  with open("error.json", "w") as outfile:
    json.dump(f, outfile)

# -----------------------

def jsonData(slug, offset=0, limit=100):
    '''
    Fetch data usingh API
    Give limit offsset and slug as paramter
    Will return a json_data in form of a dataframe, and also return the request status
    '''
    url = "https://api.opensea.io/api/v1/assets?collection_slug="+slug+"&offset="+str(offset)+"&order_direction=desc&limit="+str(limit)+"&include_orders=false"
    headers = {"Accept": "application/json", "X-API-KEY": api_key}
    response = requests.get(url, headers=headers)
    df1 = pd.DataFrame()

    # df1 = pd.DataFrame(columns = ["collection_slug","asset_name"])
    # print(df1)

    # if response.status_code !=200:
    #     print(response.status_code, ' ' , slug)   
    #     df1 = 0

    json_data = json.loads(response.text)

    if json_data.get('assets') != None: 
        # clmn = list(json_data['assets'][0].keys())
        # clmn.append(['collection_slug'])
        # df1 = pd.DataFrame(columns = clmn)
        # df1['collection_slug'] = [slug]

        if len(json_data['assets']) == 0:       
            # df1['asset_name'] = 'NaN'
            # print('No asset of ',slug)
            pass
        
        else:
            df1 = pd.DataFrame(json_data['assets'])
            # df1['asset_name'][0] = str(json_data['assets'])
     
    else:
        print("Assets info null",slug)
        df1 = 1
    return df1,response.status_code

def assets(api_key):
  '''
  This will give assets infoermation of a collection
  Paramter: api_key
  
  Fetch creator name in this funciton only
  Empty asset_data folder before running this -> using python
  '''
  # Limit of fetching the address
  df0 = pd.read_csv('data/opensea_collections.csv')

  slug = df0.slug.tolist()
  empty = []
  # Sub Sample
  for i in range(df0.shape[0]):
    print(i)

    # After every 4 requests wait for 2 seconds
    if i%4==0 and i!=0:
      print('Sleep 3 seconds - batch of 4')
      time.sleep(3)
    
    # After every 36 requests wait for 2 seconds
    if i%35==0 and i!=0:
      print('Sleep 50 seconds - batch of 35')
      time.sleep(50)

    if i%150==0 and i!=0:
      print('Sleep 100 seconds - batch of 150')
      time.sleep(100)

    lmt = 100
    ofst = 0 
    df2,r_status = jsonData(slug[i], limit = lmt, offset = ofst)

    if r_status != 200:
      print(r_status, 'Error: ',i ,slug[i] )
      print('Second sleep')
      time.sleep(240)
      df2,r_status = jsonData(slug[i], limit = lmt, offset = ofst)
      print('After Sleep: ',r_status)
      # break

    df,r_status = jsonData(slug[i], limit = lmt,offset = ofst) #temporary dataset

    # If jsonData function return Data Frmae that means there is no error and collection have assets present
    if isinstance(df, pd.DataFrame):
      count = 1 # sarted count from 1
      while df.shape[0] == lmt:
          print(count)
          count = count + 1
          if count%2==0:
            time.sleep(60)

          ofst += 100
          df, r_status = jsonData(slug[i], limit = lmt,offset = ofst)

          # if asset have only 100 exact items
          if df.shape[0] == 0:
              break
          df2 = pd.concat([df2, df])
          '''Print slug name and number of assets'''
          # print('df2 ',df2.shape,' and ',' df ',df.shape) 

          # fetch data again
          # id dataframe rows not equal to limit
          # while loop runs again
          # and concat this data to general dataframe that is df2

    # If jsonData function return integer 
    elif isinstance(df, int):
      # if df == 0 means response.status!= 200
      '''Implememnted above'''
      if df == 0:
        '''Implement exponential sleep time'''
        pass
      # if df == 1 means no assets for that collection -> just append the data 
      elif df == 1:
        empty.append(slug[i])
        continue # if df is an int go to next iteration of loop

    df2.to_csv('asset_data/'+str(slug[i])+'.csv')
  
  # All empty collections(slug_names) lilst to file
  with open(r'empty.txt', 'w') as f:
    f.write('\n'.join(empty))

  return df2, empty

''' Offset no more working '''
def event_dataNOUSE(slug,start_time,end_time):
  # start_time = datetime.datetime.timestamp(start)
  # end_time = datetime.datetime.timestamp(end)

  offset = 0
  # Limit 50 is fixed for fetching events 
  limit = 50
  # All paramters must be in string format to concat them together
  url = "https://api.opensea.io/api/v1/events?offset="+str(offset)+"limit="+str(limit)+"&only_opensea=true&collection_slug="+slug+"&occurred_before="+str(end_time)+"&occurred_after="+str(start_time)
  headers = {
        "accept": "application/json",
        "X-API-KEY": api_key
    }  
  response = requests.get(url, headers = headers)
  #Its of dictionary type : json_data
  json_data = json.loads(response.text) 
  print(response.status_code)
  leng = json_data['asset_events']

  print('Loop Decide')
  while len(leng) == limit:
    print
    # Save json_data
    # Increment offsest and limit
    offset =  offset + 50
    url = "https://api.opensea.io/api/v1/events?offset="+str(offset)+"limit="+str(limit)+"&only_opensea=true&collection_slug="+slug+"&occurred_before="+str(end_time)+"&occurred_after="+str(start_time)
    # url = "https://api.opensea.io/api/v1/events?limit="+str(limit)+"only_opensea=true&collection_slug="+slug+"&occurred_before="+str(end_time)+"&occurred_after="+str(start_time)
    response = requests.get(url, headers = headers)
    json_das = json.loads(response.text) # it will act as a local dictionary wioll contain only loop data

    # Following line will concat two dictionaries
    json_data.update(json_das) # its the global dictionary that will contain all data

    # Update the length paramter
    leng = json_das['asset_events']
  
  print('Daving data to json file')
  json_object = json.dumps(json_data, indent=4)
  # Writing to sample.json
  with open("sample.json", "w") as outfile:
    outfile.write(json_object)
  return json_data

# def event_data(slug,start_time,end_time):
#   # start_time = datetime.datetime.timestamp(start)
#   # end_time = datetime.datetime.timestamp(end)
#   limit = 300

#   '''Algo'''
#   # if fetched data length 300 
#   #   reduce time period
#   # else:
#   #   move ahead
#   pass


def date_unixx(year,month,date,hour,minutes):
  ''' this function will retun date in unix format '''
  # no zero before month like 01,02 - not allowed
  # year - in full format like 2021,2022 - and not 21,22

  ''' have to pass date as a paramter'''
  date_time = datetime.datetime(year,month,date,hour,minutes)
  unx = time.mktime(date_time.timetuple())
  return unx
  
def event():
  '''
  This functiuon return all events of a collection for the sopecified time
  Parametr : for now Nonw -> in future pass start and end time as a paramter
  Return: for now count of events for that particular time stamp for each collection slug
  '''

  df0 = pd.read_csv('data/opensea_collections.csv')
  slug = df0.slug.tolist()

  # Format - year, day, date , Hour, Minute
  start = date_unixx(2022,1,26,00,00)  #occured after
  end = date_unixx(2022,1,26,23,59) # OOccured_before

  headers = {
        "accept": "application/json",
        "X-API-KEY": api_key
    }  

  limit = 300 # max limit possible for requests

  df_c_Slug = []
  df_c_Count = []

  df_e_Slug = []
  df_e_Error = []

  for i in trange(df0.shape[0]):
    if i%4==0 and i!=0:
      print('Sleep 3 seconds - batch of 4')
      time.sleep(3)
    
    # After every 36 requests wait for 2 seconds
    elif i%35==0 and i!=0:
      print('Sleep 50 seconds - batch of 35')
      time.sleep(50)

    elif i%150==0 and i!=0:
      print('Sleep 100 seconds - batch of 150')
      time.sleep(100)
    
    url = "https://api.opensea.io/api/v1/events?limit="+str(limit)+"&only_opensea=true&collection_slug="+str(slug[i])+"&occurred_before="+str(end)+"&occurred_after="+str(start)
    
    # get url data
    response = requests.get(url, headers = headers)

    # check if error
    if response.status_code != 200:
      print('Error: ',response.status_code)
      df_e_Slug.append(slug[i])
      df_e_Error.append(response.status_code)
      continue
    
    # if data retrieval successful
    elif response.status_code == 200:
      json_das = json.loads(response.text)
      df_c_Slug.append(slug[i])
      df_c_Count.append(len(json_das['asset_events']))

    df_c = pd.DataFrame({'Slug': df_c_Slug, 'Count': df_c_Count})
    df_e = pd.DataFrame({'Slug': df_e_Slug, 'Error': df_e_Error})

    df_c.to_csv('event_count.csv')
    df_e.to_csv('event_error.csv')
  return df_c,df_e

# Jan 26, 2021
# Check collections that have more than 300 eventts in a day - record all events that reach the limit
