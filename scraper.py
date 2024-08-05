from bs4 import BeautifulSoup
import re
import requests
import json
import os
import time, random
from joblib import Parallel, delayed

import pandas as pd
import numpy as np

def scrape_page(url):
    index_response = requests.get(url)
    # HTML content of the webpage
    webpage_content = str(BeautifulSoup(index_response.content, 'html.parser'))
    


    # Initialize an empty dictionary to store key-value pairs
    data_dict = {}

    # Define a regular expression pattern to extract attributes from the autoverify-auto-vehicle-details tag
    #pattern = r"<autoverify-auto-vehicle-details(.*?)></autoverify-auto-vehicle-details>"
    #pattern = r"gptAdTargeting.push\(\{ key: '(.*?)', value: '(.*?)' \}\);"
    pattern = r'"attr":({[^{}]+})'
    # Search for the pattern in the HTML content
    match = re.search(pattern, webpage_content)#, re.DOTALL)

    if match:
        data_dict = eval(match.group(1))
    else:
        print("No match found.")
        print(url)
        print(webpage_content)

    date_pattern1 = r'Posted <time dateTime=\"([^\"]+)\"'  # r'Posted <time dateTime=\"(\d{4}-\d{2}-\d{2})'
    date_pattern2 = r'content=\"([^\"]+)\" itemprop=\"datePosted\">'
    match = re.search(date_pattern1, webpage_content)
    if not (match):
        match = re.search(date_pattern2, webpage_content)
    #match = re.search(r'Posted <span title="(\w+ [0-9][0-9]?, \d{4})', webpage_content)

    if match:
        extracted_date = match.group(1)
        data_dict.update({'date_posted':extracted_date})
    else:
        print("Date not found in the HTML string.")
        print(url)
        data_dict.update({'date_posted':None})
    #['ad_id', 'title', 'vin', 'dealer_id', 'condition', 'make', 'model', 'trim', 'odometer', 'year', 'current_price', 'certified', 'date_posted', 'url']

    price = re.search(r'\"prc\":({[^}]+})', webpage_content)

    if price:
        price_dict = eval(price.group(1))
        data_dict.update(price_dict)

    data_dict['url'] = url    

    return data_dict

def get_listing_url(webpage_content):
    pattern = r'"listing-link" href="(\/v-cars-trucks\/[ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\-._~:\/?#\[\]@!$&\'()*+,;=]+)"'
    #\/v-cars-trucks\/[ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\-._~:\/?#\[\]@!$&\'()*+,;=]+
    #r'"listing-link" href=("\/[ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\-._~:\/?#\[\]@!$&\'()*+,;=]+")'

    #index_response = requests.get(url)
    #webpage_content = str(BeautifulSoup(index_response.content, 'html.parser'))

    # Search for the pattern in the HTML content
    matches = re.findall(pattern, webpage_content, re.DOTALL)

    return matches

def scrape_page_write_json(page_num, first_run):
    index_response = None
    
    page_hashes = None
    
    if first_run != 'Y':
        page_hashes = hash_existing_data()

    if page_num != 1:
        index_response = requests.get('https://www.kijiji.ca/b-cars-trucks/canada/page-{}/c174l0?sort=dateDesc'.format(page_num))
    else: 
        index_response = requests.get('https://www.kijiji.ca/b-cars-trucks/canada/c174l0?sort=dateDesc')
    webpage_content = str(BeautifulSoup(index_response.content, 'html.parser'))
    page_data = []
    listings = get_listing_url(webpage_content)
    if listings:
        if first_run == 'Y':
            for listing in listings:
                listing_url = 'https://www.kijiji.ca' + listing
                new_data = scrape_page(listing_url)
                page_data.append(new_data)
        else:
            listings = 'https://www.kijiji.ca' + pd.Series(listings)
            listing_df = pd.DataFrame({'hashes':pd.util.hash_array(listings.to_numpy()), 'webpage':listings})
            
            for index, row in listing_df.iterrows():
                candidate = np.searchsorted(page_hashes,row['hashes'])
                if row['hashes'] != page_hashes[candidate]:
                    listing_url = row['webpage']
                    new_data = scrape_page(listing_url)
                    page_data.append(new_data)
                


            '''for field in fields:
                if not(field in new_data.keys()):
                    new_data[field] = ""

                if field in data_dict.keys():
                    data_dict[field].append(new_data[field])
                else:
                    data_dict[field] = [new_data[field]]'''
    print(page_data)
    if first_run == 'Y':
        with open('data/page_{}_data.json'.format(page_num), 'w') as fout:
            json.dump(page_data, fout)
    else:
        with open('data_update/page_{}_data.json'.format(page_num), 'w') as fout:
            json.dump(page_data, fout)

    #time.sleep(random.uniform(1,5))

def hash_existing_data(location = 'data_update_in/data_parquet.parquet'):
    data = pd.read_parquet(location)
    hashes = np.unique(pd.util.hash_array(data.url.to_numpy()))
    hashes.sort()
    return hashes

#https://www.kijiji.ca/b-cars-trucks/canada/c174l0


        




if __name__ == '__main__':
    first_run = input('Is this an initial run? (Y/N)\n')
    
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)


    if not(os.path.exists('data/')):
        os.mkdir('data')

    if not(os.path.exists('data_update/')):
        os.mkdir('data_update')

    if not(os.path.exists('data_final/')):
        os.mkdir('data_final')

    if not(os.path.exists('data_update_in/')):
        os.mkdir('data_update_in')

    scrape_page_write_json_loop = lambda k: scrape_page_write_json(k, first_run)
    #magic number alert: Can't find a way to scrape the page count, needs to be added manually
    Parallel(n_jobs=16)(delayed(scrape_page_write_json_loop)(k) for k in range(1,5625))
    