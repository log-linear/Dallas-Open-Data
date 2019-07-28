#!/usr/bin/env python3
"""
Author:         Victor Faner
Date:           2019-07-25
Description:    Various functions for parsing SoQL query results into
                a tabular format
"""
import json

import pandas as pd


def parse_location(location):
    """
    Parse SoQL Location objects of form

        {'human_address': '{"address": "10230 VISTADALE DR",
                            "city": "DALLAS",
                            "state": "TX",
                            "zip": "75238"}',
         'latitude': '32.888392',
         'longitude': '-96.707833'}

    into a tabular format of form

        {'address': '10230 VISTADALE DR',
         'city': 'DALLAS',
         'latitude': '32.888392',
         'longitude': '-96.707833',
         'state': 'TX',
         'zip': '75238'}

    :param location:            dict, SoQL Location object
    :return parsed_location:    dict, parsed SoQL Location object
    """
    parsed_location = {}

    if isinstance(location, dict):
        parsed_location.update(location)
        if 'human_address' in parsed_location:
            parsed_location.update(json.loads(parsed_location['human_address']))

    return parsed_location


def parse_metadata(raw_metadata):
    """
    Parse relevant metadata into a tabular format

    :param raw_metadata:    dict, raw metadata as returned by
                            Socrata.get_metadata()
    :return:                pd.DataFrame, queried metadata
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


def get_dtypes(raw_metadata):
    """
    Get dict of field names with SoQL data types as keys.

    :param parsed_metadata:     dict, parsed metadata from
                                parse_metadata function
    :return dtypes:             dict of field names and data types
    """
    dtypes = {}
    for column in raw_metadata['columns']:
        dtypes[column['fieldName']] = column['dataTypeName']

    return dtypes


def get_results_df(raw_results, dtypes):
    """
    Wrapper function for converting raw query results into a DataFrame

    :param raw_results:     list, raw results as returned from
                            Socrata.get()
    :param dtypes:          dict, SoQL data type dict as returned from
                            get_dtypes()
    :return results_df:     pd.DatFrame
    """
    results_df = pd.DataFrame(raw_results)

    # Handle Location datatype columns
    for k, v in dtypes.items():
        if v == 'location' and k in results_df.columns:
            locations = pd.DataFrame(list(results_df[k].apply(parse_location)))
            results_df = results_df.drop([k], axis=1)
            results_df = results_df.merge(locations,
                                          left_index=True,
                                          right_index=True,
                                          suffixes=('', f'_{k}'))

    return results_df

