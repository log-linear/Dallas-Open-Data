"""
Author:         Victor Faner
Date:           2019-05-26
Description:    Module for running SoQL queries.  Can optionally parse
                SQL-like FROM statements to determine domain and API
                endpoint, e.g.

                    SELECT *

                    FROM https://{domain}/resource/{endpoint}.json

                If running in command line, can also specify a SoQL
                query file as argument.
"""

import sys
import logging
import re
from configparser import ConfigParser
from pathlib import Path

import pandas as pd
from sodapy import Socrata


def get_results(client, endpoint, query):
    """
    Get results from a SoQL query

    :param client:      Socrata client object,
    :param endpoint:    str, API endpoint to query
    :param query:       str, SoQL-formatted query
    :return:            DataFrame, queried results table
    """
    results = client.get(
        endpoint,
        query=query,
    )

    return pd.DataFrame(results)


def get_metadata(client, endpoint):
    """
    Get metadata from a given dataset

    :param client:      Socrata client object
    :param endpoint:    str, API endpoint to query
    :return:            DataFrame, queried metadata
    """
    metadata = client.get_metadata(endpoint)
    metadata_dict = {
        'Field Name': [],
        'Column Name': [],
        'Description': [],
        'Data Type': [],
    }
    for column in metadata['columns']:
        metadata_dict['Field Name'].append(column['fieldName'])
        metadata_dict['Column Name'].append(column['name'])
        metadata_dict['Data Type'].append(column['dataTypeName'])
        try:
            metadata_dict['Description'].append(column['description'])
        except KeyError:
            metadata_dict['Description'].append(None)

    return pd.DataFrame(metadata_dict)


def parse_from_statement(query):
    """
    Parse domain and API endpoint from a SoQL query via FROM statement

    :param query:                   str, SoQL-formatted query
    :return url, endpoint, query:   str objects, domain, endpoint, and
                                    original query sans FROM statement
    """
    url = re.search(r'www.\w+\.(com|org|net|gov|us|ca)', query)
    endpoint = re.search(r'\w{4}-\w{4}\.json', query)
    query = re.sub(r'from( +|\t+|\n+).+', '', query, flags=re.I)

    return url.group(0), endpoint.group(0).replace('.json', ''), query


def main(query):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s]: %(message)s')
    cfg = ConfigParser()
    cfg.read(Path.cwd() / 'config.cfg')
    app_token = cfg['CLIENT']['APPTOKEN']

    # Attempt to parse domain and endpoint via FROM statement
    try:
        url, endpoint, query = parse_from_statement(query)
    except AttributeError:
        url = cfg['CLIENT']['DOMAIN']
        endpoint = cfg['CLIENT']['ENDPOINT']

    with Socrata(url, app_token) as client:
        logging.info(f'Connected to {client.domain}')

        logging.info('Querying dataset')
        results = get_results(client, endpoint, query)

        logging.info('Fetching metadata')
        metadata = get_metadata(client, endpoint)

    logging.info('Connection closed')

    return results, metadata


if __name__ == '__main__':
    try:
        query_file = sys.argv[1]
    except IndexError:
        query_file = Path.cwd() / 'queries' / 'police_incidents.sql'

    if query_file == '--mode=client':
        query_file = Path.cwd() / 'queries' / 'police_incidents.sql'

    with open(query_file) as f:
        query = f.read()

    results, metadata = main(query)

    if len(results) < len(metadata):
        pd.options.display.max_rows = len(metadata)
    elif 1000 > len(results) > len(metadata):
        pd.options.display.max_rows = len(results)
    else:
        pd.options.display.max_rows = 1000

    print('\nMetadata\n')
    print(metadata)
    print('\nOutput\n')
    print(results)
