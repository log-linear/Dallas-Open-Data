#!/usr/bin/env python3
"""
Author:         Victor Faner
Date:           2019-05-26
Description:    Module/command-line utility for running SoQL queries.
                Enables SQL-like FROM functionality in queries to
                determine domain and endpoint, e.g.

                    SELECT *

                    FROM https://{domain}/resource/{endpoint}.json

                Command-line syntax:

                    run_soql.py queries/police_incidents.sql
"""

import os
import sys
import logging
import re
from pathlib import Path

import pandas as pd
from sodapy import Socrata

from parsers import get_dtypes, parse_metadata, get_results_df


def get_endpoint(query):
    """
    Parse domain and API endpoint from a SoQL query via FROM statement

    :param query:               str, SoQL-formatted query
    :return
        url, endpoint, query:   str objects, domain, endpoint, and
                                original query sans FROM statement
    """
    url = re.search(r'\w+\.\w+\.(\w{2,3})', query)
    endpoint = re.search(r'(\w{4}-\w{4})\.json', query)
    query = re.sub(r'from( +|\t+|\n+).+', '', query, flags=re.I)

    return url.group(), endpoint.group(1), query


def run_query(query):
    """
    Wrapper for running a SoQL query

    :param query:                   str, SoQL query
    :return results, metadata:      dict, list, raw results and metadata
                                    as returned from calls to
                                    Socrata.get() and
                                    Socrata.get_metadata(), respectively

    """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s]: %(message)s')
    app_token = os.environ.get('APPTOKEN')

    try:
        url, endpoint, parsed_query = get_endpoint(query)
    except AttributeError:
        logging.warning('Unknown endpoint.')
        raise SystemExit

    with Socrata(url, app_token) as client:
        logging.info(f'Connected to {client.domain}')

        logging.info('Fetching metadata')
        metadata = client.get_metadata(endpoint)

        logging.info('Running query')
        print(f'\n{query}\n')
        results = client.get(endpoint, query=parsed_query)

        logging.info('Connection closed')

    return results, metadata


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s]: %(message)s')
    try:
        query_file = sys.argv[1]
    except IndexError:
        logging.warning('No query specified')
        raise SystemExit

    with open(query_file) as f:
        query = f.read()

    raw_results, raw_metadata = run_query(query)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s]: %(message)s')
    app_token = os.environ.get('APPTOKEN')

    logging.info('Parsing metadata')
    metadata_df = pd.DataFrame(parse_metadata(raw_metadata))

    logging.info('Parsing datatypes')
    dtypes = get_dtypes(raw_metadata)

    results_df = get_results_df(raw_results, dtypes)

    if len(results_df) < len(metadata_df):
        pd.options.display.max_rows = len(metadata_df)
    elif 1000 > len(results_df) > len(metadata_df):
        pd.options.display.max_rows = len(results_df)
    else:
        pd.options.display.max_rows = 1000

    print('\nMetadata\n')
    print(metadata_df)
    print('\nOutput\n')
    print(results_df)

    csv_name = re.sub(r'\w+\/|.sql|.soql|.txt', '', sys.argv[1])
    results_df.to_csv(Path.cwd() / 'data' / f'{csv_name}.csv', index=False)
