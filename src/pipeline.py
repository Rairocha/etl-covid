import logging
import functools
from multiprocessing import Pool

from client import Client
from nodes import data_gathering
from nodes import data_preparation
from nodes import data_storage
from nodes import data_transform
from nodes import data_viz
from params import Params 


logger = logging.getLogger('pipeline')
def process(client, params):  
    """
    The ETL pipeline.
    
    It contains the main nodes of the extract-transform-load 
    pipeline from the process. 
    
    Parameters
    ----------
    
    client: Client
    parmas: Params
    
    Notes 
    -----
    The main idea is to consider each task as a conceptual **node**. 
    This function, `process` is the **pipeline** that integrates all 
    tasks together. Each node is a .py file imported from the `nodes`
    directory. 
    
    The main idea is that each node can be in one of the following state:
        - up-to-date: the task to be done given the input parameters is 
        already completed. Hence, no rework is needed.

        - out-of-date: the task to be done is not completed and should be 
        run.

    """
    pool= Pool(2)
    data_preparation.run(params)

    if data_gathering.done(params):
        try:
            pool.map(data_gathering.update,zip([params]*len(params.csv_verified),params.csv_verified))
            logger.info('Saved raw csvs')
        except Exception as e:
            logger.warning(f'Failed data-gathering {e}')
            pool.terminate()

    if data_transform.done(params):
        try:
            pool.map(data_transform.update,zip([params]*len(params.csv_transform),params.csv_transform))
            logger.info('Saved processed csvs')
        except Exception as e:
            logger.warning(f'Failed data-transform {e}')
            pool.terminate()

    if data_storage.done(client, params):
        try:
            list(map(data_storage.update,zip([client]*len(params.csv_sql),[params]*len(params.csv_sql),params.csv_sql)))
            logger.info('Saved csv on sql')
        except Exception as e:
            logger.warning(f'Failed data-storage {e}')
            pool.terminate()

    if not data_viz.done(client, params):
        data_viz.update(client, params)

    pool.terminate()
if __name__ == '__main__': 

    params = Params()

    logging.basicConfig(filename=params.log_name,
                    level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

    client = Client(params)
    process(client, params)