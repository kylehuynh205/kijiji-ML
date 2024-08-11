from bs4 import BeautifulSoup
import re
import requests
import json
import os
import time, random
from joblib import Parallel, delayed
import itertools 
import pandas as pd
import numpy as np
import pickle

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
        #print(webpage_content)

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

def scrape_listings_write_json(listings, first_run, page_hashes, model, year):
    index_response = None
    
    '''page_hashes = None
    
    if first_run != 'Y':
        page_hashes = hash_existing_data()'''

    '''if page_num != 1:
        index_response = requests.get('https://www.kijiji.ca/b-cars-trucks/canada/page-{}/c174l0?sort=dateDesc'.format(page_num))
    else: 
        index_response = requests.get('https://www.kijiji.ca/b-cars-trucks/canada/c174l0?sort=dateDesc')
    webpage_content = str(BeautifulSoup(index_response.content, 'html.parser'))
    page_data = []
    listings = get_listing_url(webpage_content)'''

    page_data = []
    listings = list(listings)
    if listings:
        if first_run == 'Y':
            for listing in listings:
                listing_url = 'https://www.kijiji.ca' + listing
                new_data = scrape_page(listing_url)
                page_data.append(new_data)
        else:
            print(listings)
            listings = 'https://www.kijiji.ca' + pd.Series(listings)
            listing_df = pd.DataFrame({'hashes':pd.util.hash_array(listings.to_numpy()), 'webpage':listings})
            
            for index, row in listing_df.iterrows():
                try:
                    candidate = np.searchsorted(page_hashes,row['hashes'])
                    if candidate >= page_hashes.shape[0]:
                        listing_url = row['webpage']
                        new_data = scrape_page(listing_url)
                        page_data.append(new_data)
                    elif row['hashes'] != page_hashes[candidate]:
                        listing_url = row['webpage']
                        new_data = scrape_page(listing_url)
                        page_data.append(new_data)
                except: 
                    pass
                


            '''for field in fields:
                if not(field in new_data.keys()):
                    new_data[field] = ""

                if field in data_dict.keys():
                    data_dict[field].append(new_data[field])
                else:
                    data_dict[field] = [new_data[field]]'''
    #print(page_data)
    print('Writing data')
    if first_run == 'Y':
        with open('data/{}_{}_data.json'.format(model, year), 'w') as fout:
            json.dump(page_data, fout)
    else:
        with open('data_update/{}_{}_data.json'.format(model, year), 'w') as fout:
            json.dump(page_data, fout)

    #time.sleep(random.uniform(1,5))

def hash_existing_data(location = 'data_update_in/data_parquet.parquet'):
    data = pd.read_parquet(location)
    hashes = np.unique(pd.util.hash_array(data.url.to_numpy()))
    hashes.sort()
    return hashes

#https://www.kijiji.ca/b-cars-trucks/canada/c174l0


def merge_listings_and_scrape(model, year, page_hashes,first_run):
    if os.path.isfile('data_update/{}_{}_data.json'.format(model, year)):
        print('data_update/{}_{}_data.json already exists'.format(model, year))
        return
    
    listings = None
    for k in range(1,101):   
        file = 'pagelists/listings_{}_{}_{}.csv'.format(model,year,k)
        tmp = None
        if os.path.isfile(file):
            tmp = pd.read_csv(file,header = 0)['0']
        if tmp is not None and tmp.shape[0]>0:
            if listings is None:
                listings = tmp
            else:
                listings = pd.concat([listings,tmp])
    if listings is not None:
        listings = listings.drop_duplicates()
        #print(listings)
        scrape_listings_write_json(listings, first_run, page_hashes, model, year)


def get_listing_url(webpage_content):
    pattern = r'"listing-link" href="(\/v-cars-trucks\/[ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\-._~:\/?#\[\]@!$&\'()*+,;=]+)"'
    #\/v-cars-trucks\/[ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\-._~:\/?#\[\]@!$&\'()*+,;=]+
    #r'"listing-link" href=("\/[ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\-._~:\/?#\[\]@!$&\'()*+,;=]+")'

    #index_response = requests.get(url)
    #webpage_content = str(BeautifulSoup(index_response.content, 'html.parser'))

    # Search for the pattern in the HTML content
    matches = re.findall(pattern, webpage_content, re.DOTALL)
    
    return matches



def scrape_listings(model, year, page_num):
    index_response = None
    
    page_hashes = None
    

    if page_num != 1:
        index_response = requests.get('https://www.kijiji.ca/b-cars-trucks/canada/{}-{}/page-{}/c174l0a54a68?sort=dateDesc'.format(model,year,page_num))#'https://www.kijiji.ca/b-cars-trucks/canada/page-{}/c174l0?sort=dateAsc'.format(page_num))
    else: 
        index_response = requests.get('https://www.kijiji.ca/b-cars-trucks/canada/{}-{}/c174l0a54a68?sort=dateDesc'.format(model,year))#'https://www.kijiji.ca/b-cars-trucks/canada/c174l0?sort=dateAsc')
    webpage_content = str(BeautifulSoup(index_response.content, 'html.parser'))

    matches = get_listing_url(webpage_content)
    if matches:
        listing_urls = pd.Series(matches)
        listing_urls.to_csv('pagelists/listings_{}_{}_{}.csv'.format(model,year,page_num))
    #listing_urls.to_csv('pagelists/listings_{}.csv'.format(page_num), index = False)
    '''with open('pagelists/listings_{}.pkl'.format(page_num), 'wb') as f:
        json.dump(listing_urls, f)'''


if __name__ == '__main__':


    listing_pages = []
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    listing_urls = None
    models = list(pd.read_csv('models.csv')['0'])
    for model in models:
        for year in reversed(range(2000,2025)):
            listing_urls = None
            Parallel(n_jobs=24)(delayed(scrape_listings)(model, year, k) for k in range(1,101))
                
            
    first_run = input('Is this an initial run? (Y/N)\n')
    
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    page_hashes = None

    if first_run != 'Y':
        page_hashes = hash_existing_data()

    if not(os.path.exists('data/')):
        os.mkdir('data')

    if not(os.path.exists('data_update/')):
        os.mkdir('data_update')

    if not(os.path.exists('data_final/')):
        os.mkdir('data_final')

    if not(os.path.exists('data_update_in/')):
        os.mkdir('data_update_in')

    models = list(pd.read_csv('models.csv')['0'])

    itr = itertools.product(models, list(range(2000,2025)))

    #scrape_page_write_json_loop = lambda k: scrape_page_write_json(k, first_run)
    #magic number alert: Can't find a way to scrape the page count, needs to be added manually
    #for model, year in itr:
    #    merge_listings_and_scrape(model, year, page_hashes,first_run) 
    Parallel(n_jobs=24)(delayed(merge_listings_and_scrape)(model, year, page_hashes,first_run) for model, year in itr)
    

    

