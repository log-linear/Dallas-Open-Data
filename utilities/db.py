#!/usr/bin/env python3
import logging
import sqlite3
from pathlib import Path
import sys

import pandas as pd

import run_soql


def initial_load():
    con = sqlite3.connect(Path.cwd() / 'data' / 'dallas_open_data.db')

    metadata = pd.read_csv(sys.argv[2])
    dtypes = dict(zip(metadata.field_names, metadata.dtypes))
    df = pd.read_csv(sys.argv[1], dtype=dtypes)

    df.columns = (
        df.columns
            .str.replace(r'\W', ' ')
            .str.strip()
            .str.replace(r' +', '_')
    )

    tbl = sys.argv[1].stem
    df.to_sql(tbl, con=con, if_exists='replace', index=False)
    logging.info(f'{tbl} loaded to database.')


def update_db(tbl, con, in_query):
    results, metadata = run_soql.run_query(in_query)

    results.to_sql(f'{tbl}_STAGING', con=con, if_exists='append')
    logging.info(f'{tbl}_STAGING loaded to database.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s]: %(message)s')

    initial_load()
