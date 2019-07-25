#!/usr/bin/env python3
import logging
import sqlite3
import glob
import os
import json
from pathlib import Path

import pandas as pd
from sodapy import Socrata


def parse_location(location):
    """
    Get results from a SoQL query

    :param client:      Socrata client object,
    :param endpoint:    str, API endpoint to query
    :param query:       str, SoQL-formatted query
    :return:            DataFrame, queried results table
    """
    parsed_location = {
        'latitude': None,
        'longitude': None,
        'address': None,
        'city': None,
        'state': None,
        'zip': None,
    }
    address_keys = [
        'address',
        'city',
        'state',
        'zip',
    ]
    for key in parsed_location.keys():
        try:
            if key == 'human_address':
                for a_key in address_keys:
                    parsed_location[key] = json.loads(location[key])[a_key]
            else:
                parsed_location[key] = json.loads(location[key])
        except KeyError:
            parsed_location[key] = None
    return parsed_location


def parse_metadata(raw_metadata):
    """
    Get metadata from a given dataset

    :param client:      Socrata client object
    :param endpoint:    str, API endpoint to query
    :return:            DataFrame, queried metadata
    """
    parsed_metadata = {
        'field_name': [],
        'column_name': [],
        'description': [],
        'data_type': [],
    }
    for column in raw_metadata['columns']:
        parsed_metadata['field_name'].append(column['fieldName'])
        parsed_metadata['column_name'].append(column['name'])
        parsed_metadata['data_type'].append(column['dataTypeName'])
        try:
            parsed_metadata['description'].append(column['description'])
        except KeyError:
            parsed_metadata['description'].append(None)

    return parsed_metadata


def get_dtypes(parsed_metadata):
    dtypes = {}
    for i in range(len(parsed_metadata['field_name'])):
        dtypes[parsed_metadata['field_name'][i]] = (
            parsed_metadata['data_type'][i]
        )

    return dtypes


def get_results_df(raw_results, dtypes):
    results_df = pd.DataFrame(raw_results)
    locations = results_df[
        [k for k, v in dtypes.items() if v == 'location']
    ]
    results_df = results_df.drop(
        [k for k, v in dtypes.items() if v == 'location'],
        axis=1
    )
    locations = pd.DataFrame(list(locations.apply(parse_location, axis=1)))

    results_df = results_df.merge(locations, how='left')

    return results_df

