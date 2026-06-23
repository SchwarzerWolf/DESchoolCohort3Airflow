import logging

import pandas as pd
import requests
from sqlalchemy import create_engine

BASE_URL = 'https://downloads.thebiogrid.org/Download/BioGRID/Release-Archive/BIOGRID-{version}/BIOGRID-ALL-{version}.tab3.zip'
DATABASE_URL = 'postgresql://postgres:postgres@localhost:5432/postgres'


def load_biogrid(version: str):
    logging.info('Loading biogrid file...')
    response = requests.get(
        BASE_URL.format(version=version),
        params={'downloadformat': 'zip'}
    )

    if response.status_code == 200:
        local_file_name = 'biogrid.tab3.zip'
        with open(local_file_name, 'wb') as f:
            f.write(response.content)
    else:
        logging.error('The specified version is not found')
        return

    logging.info('Biogrid file has loaded')
    logging.info('Starting biogrid processing')

    df = pd.read_csv(local_file_name, delimiter='\t', compression='zip')

    df = df.rename(
        lambda column_name: column_name.lower().replace(' ', '_').replace('#', '_').strip('_'),
        axis='columns'
    )

    df = df[[
        'biogrid_interaction_id',
        'biogrid_id_interactor_a',
        'biogrid_id_interactor_b',
    ]]

    df['version'] = version

    logging.info('Biogrid file has been transformed')
    logging.info('Starting ingestion into database...')

    engine = create_engine(DATABASE_URL)
    df.to_sql('biogrid_data', engine, schema='silver', if_exists='replace')
    logging.info('Data successfully ingested')


load_biogrid(version='4.4.230')
