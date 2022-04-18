import logging
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import os
logger = logging.getLogger('nodes.data_gathering')


def update(tup):
    'Receives tuple with params and url'
    params = tup[0]
    url= tup[1]
    try:
        response = requests.get(url)
        html = response.content
        soup = BeautifulSoup(html,'lxml')
        try:
            csv_url = 'https://github.com' + soup.find_all('div', attrs={'class':'BtnGroup'})[-1].find_all('a')[0]['href']
            date = re.findall('\d{2}-\d{2}-\d{4}', csv_url)[0].replace('-','_')
            filename = 'corona_' + date + '.csv'
            df = pd.read_csv(csv_url)
            df.to_csv(f'{params.raw_data}/{filename}',index=False)
            logger.info(f'extract sucessfull')
            #return df , filename
        except IndexError:
            logger.info(f'Something went wrong in extract findall')
        
    except :
        logger.info(f'Something went wrong in extract url connection')


def done(params):
    if params.backfilling == True:
        params.csv_verified = params.csv_files
        logger.info(f'All csv_files will be scraped')
    else:
        csv_verified = []
        for csv in params.csv_files:
            if 'corona_'+csv.split('/')[-1].replace('-','_') not in os.listdir(params.raw_data):
                csv_verified.append(csv)
        params.csv_verified = csv_verified
        logger.info(f'Removed unecessary csv files')
    if len(params.csv_verified) ==0:
        logger.info(f'All files have been read')
        return False
    else:
        logger.info(f'Will run update')
        return True