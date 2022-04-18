import logging
import os
from sqlalchemy import inspect
import pandas as pd
logger = logging.getLogger('nodes.data_storage')


def update(tup):
    client = tup[0]
    params = tup[1]
    file= tup[2]
    df = pd.read_csv(f'{params.processed_data}/{file}')
    table_name = file.split('.')[0]
    try:
        df.to_sql(table_name, client.conn, if_exists='replace', index=False)
        logger.info('storage to database sucessfull')
    except:
        logger.warning('storage to database failed')



def done(client, params):
    if params.backfilling == True:
        params.csv_sql = os.listdir(params.processed_data)
        logger.info(f'All csv_files will be scraped')
    else:
        csv_sql = []
        for csv in os.listdir(params.processed_data):
            if (csv not in inspect(client.engine).get_table_names()) and (csv.endswith('.csv')):
                csv_sql.append(csv)
        params.csv_sql = csv_sql
        logger.info(f'Removed unecessary csv files')
    if len(params.csv_sql) ==0:
        logger.info(f'All files are on SQL')
        return False
    else:
        logger.info(f'Will run update')
        return True