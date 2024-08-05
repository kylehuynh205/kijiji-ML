import pandas as pd
import os
import json

def json_from_dir(dir):
    data_dictionaries = []

    files = [dir + f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]


    for file in files:
        if file[-4:] == 'json':
            with open(file) as fin:
                new_data = json.load(fin)
            if len(new_data) > 0:
                data_dictionaries.extend(new_data)
        
    
    df = pd.DataFrame(data_dictionaries)

    
    df = df.drop_duplicates(subset = 'url')

    return df

if __name__ == '__main__':
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    first_run = input('Is this an initial run? (Y/N)\n')

    folder = None

    if first_run == 'Y':
        folder = 'data/'
    else:
        folder = 'data_update/'

    final_data = None

    new_data = json_from_dir(folder)

    if first_run == 'Y':
        final_data = new_data
    else:
        old_data = pd.read_parquet('data_update_in/data_parquet.parquet').drop_duplicates(subset = 'url')
        final_data = pd.concat([new_data,old_data]).drop_duplicates(subset = 'url')
        
    final_data.to_parquet('data_final/data_parquet.parquet')