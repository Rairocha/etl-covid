import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger('nodes.data_preparation')


def run(params):
    try:
        response = requests.get(params.url)
        if response.status_code==200:
            html = response.content
            soup = BeautifulSoup(html,'lxml')
            csv_files = ['https://github.com' + tag['href'] for tag in soup.find_all('a') if tag['href'].endswith('.csv')]
            logger.info(f'get_url sucessfull')
            if params.one_file==True:
                params.csv_files= csv_files[0]
            else:
                params.csv_files= csv_files
        else:
            print(f'Url unavailable {params.url}')
            logger.info(f'Url unavailable {params.url}')
    except:
            logger.info(f'Something went wrong in get_url')