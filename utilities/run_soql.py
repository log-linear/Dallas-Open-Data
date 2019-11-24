#!/usr/bin/env python3
"""
Author:         Victor Faner
Date:           2019-05-26
Description:    Module/command-line utility for running SoQL queries.
                Uses SQL-like FROM functionality to determine domain and
                endpoint, e.g.

                    SELECT *

                    FROM https://{domain}/resource/{endpoint}.json
"""

import os
import logging
import argparse
import re
from pathlib import Path

import pandas as pd
from sodapy import Socrata

import parsers


def input_loop():
    """
    Subroutine for reading multi-line user input

    :return:    str, raw user input
    """
    print('Enter a SoQL Query. When you are finished, end the last line of your '
          'query with a semicolon character ";".')

    lines = []
    while True:
        line = input()
        if line.endswith(';'):
            break
        else:
            lines.append(line)

    return '\n'.join(lines)


def get_endpoint(query):
    """
    Regex to parse domain and API endpoint from a SoQL query via FROM
    statement

    :param query:               str, SoQL-formatted query
    :return
        url, endpoint, query:   str objects, domain, endpoint, and
                                original query sans FROM statement
    """
    url = re.search(r'\w+\.\w+\.(\w{2,3})', query, flags=re.I)
    endpoint = re.search(r'(\w{4}-\w{4})\.json', query, flags=re.I)
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
    app_token = os.environ.get('APPTOKEN')  # Socrata API token

	# Attempt to parse endpoint from query
    try:
        url, endpoint, parsed_query = get_endpoint(query)
    except AttributeError:
        logging.warning('Unknown endpoint.')
        raise SystemExit

	# Query endpoint
    with Socrata(url, app_token) as client:
        logging.info(f'Connected to {client.domain}')

        logging.info('Fetching metadata')
        metadata = client.get_metadata(endpoint)

        logging.info('Running query')
        print(f'\n{query}\n')
        results = client.get(endpoint, query=parsed_query)

        logging.info('Connection closed')

    return results, metadata


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s]: %(message)s')

    # Define command-line arguments
    desc = """
        Module/command-line utility for running SoQL queries. Uses SQL-like 
        FROM functionality to determine domain and endpoint, e.g.
        
            SELECT *
            
            FROM https://{domain}/resource/{endpoint}.json
    """
    argp = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    argp.add_argument('-i', '--infile', help='path to SoQL input file')
    argp.add_argument('-o', '--outfile', help='name of output .csv file')
    args = argp.parse_args()

    if args.infile:
        with open(args.infile) as f:
            query = f.read()
    else:
        query = input_loop()

	# Run query results into DataFrames
    raw_results, raw_metadata = run_query(query)

    logging.info('Parsing metadata')
    metadata_df = pd.DataFrame(parsers.parse_metadata(raw_metadata))

    logging.info('Parsing datatypes')
    dtypes = parsers.get_dtypes(raw_metadata)

    results_df = parsers.get_results_df(raw_results, dtypes)

    if args.outfile:  # Save results to file
        logging.info(f'Saving {args.outfile}.csv to file')
        
        output = Path.cwd() / 'output'
        output.mkdir(exist_ok=True)
        
        results_df.to_csv(output / f'{args.outfile}.csv',
                          index=False)
        metadata_df.to_csv(output / f'{args.outfile}_metadata.csv',
                           index=False)

    else:  # Print to stdout
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


if __name__ == '__main__':
    main()
