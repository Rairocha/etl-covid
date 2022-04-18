import logging
import os 
import pandas as pd

logger = logging.getLogger('nodes.data_transform')


def update(tup):
    params = tup[0]
    file= tup[1]
    df = pd.read_csv(f'{params.raw_data}/{file}')
    colnames = df.rename({'Province_State':'province', 
                      'Country_Region':'country',
                      'Admin2':'Admin'}, 
                     axis=1).columns

    df.columns = [col.lower() for col in colnames]
    df['last_update'] = pd.to_datetime(df['last_update'])
    df.country = df.country.str.replace('*','',regex=False)
    df['anomesdia'] = df.last_update.apply(lambda x : f'{str(x.year)}-{str(x.month).zfill(2)}-{str(x.day).zfill(2)}')
    df.to_csv(f'{params.processed_data}/{file}',index=False)
	
def done(params):
    if params.backfilling == True:
        params.csv_transform = os.listdir(params.raw_data)
        logger.info(f'All csv_files will be scraped')
    else:
        csv_transform = []
        for csv in os.listdir(params.raw_data):
            if csv not in os.listdir(params.processed_data):
                csv_transform.append(csv)
        params.csv_transform = csv_transform
        logger.info(f'Removed unecessary csv files')
    if len(params.csv_transform) ==0:
        logger.info(f'All files have been read')
        return False
    else:
        logger.info(f'Will run update')
        return True