#!/usr/bin/env python3
"""
Author:         Victor Faner
Date:           2019-05-26
Description:    Module/command-line utility for running SoQL queries.
                Implements SQL-like FROM functionality for determining
                domain and endpoint, e.g.

                    SELECT *

                    FROM https://{domain}/resource/{endpoint}.json

                Command-line syntax:

                    run_soql.py queries/police_incidents.sql
"""

import os
import sys
import logging
import re

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
    url = re.search(r'\w+\.\w+\.(com|org|net|gov|us|ca)', query)
    endpoint = re.search(r'(\w{4}-\w{4})\.json', query)
    query = re.sub(r'from( +|\t+|\n+).+', '', query, flags=re.I)

    return url.group(), endpoint.group(1), query


def main(query):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s]: %(message)s')
    app_token = os.environ.get('APPTOKEN')

    try:
        url, endpoint, query = get_endpoint(query)
    except AttributeError:
        logging.debug('Unknown endpoint.')
        raise SystemExit

    with Socrata(url, app_token) as client:
        logging.info(f'Connected to {client.domain}')

        logging.info('Fetching metadata')
        raw_metadata = client.get_metadata(endpoint)

        logging.info('Querying dataset')
        raw_results = client.get(endpoint, query=query)

        logging.info('Connection closed')

    return raw_results, raw_metadata


if __name__ == '__main__':
    try:
        query_file = sys.argv[1]
    except IndexError:
        logging.warn('No query specified')
        raise SystemExit

    with open(query_file) as f:
        query = f.read()

    raw_results, raw_metadata = main(query)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s]: %(message)s')
    app_token = os.environ.get('APPTOKEN')

    logging.info('Parsing metadata')
    metadata_df = pd.DataFrame(parse_metadata(raw_metadata))

    logging.info('Parsing datatypes')
    dtypes = get_dtypes(metadata_df)

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
