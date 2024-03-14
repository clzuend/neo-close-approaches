"""Extract data on near-Earth objects and close approaches from CSV and JSON.

The `load_neos` function extracts NEO data from a CSV file, formatted as
described in the project instructions, into a collection of `NearEarthObject`s.

The `load_approaches` function extracts close approach data from a JSON file,
formatted as described in the project instructions, into a collection of
`CloseApproach` objects.

The main module calls these functions with the arguments provided at the
command line, and uses the resulting collections to build an `NEODatabase`.
"""
import json
import pandas as pd

from models import NearEarthObject, CloseApproach


def load_neos(neo_csv_path: str):
    """Read near-Earth object information from a CSV file.

    :param neo_csv_path: A path to a CSV file containing near-Earth objects.
    :return: A collection of `NearEarthObject`s.
    """
    usecols = ['pdes', 'name', 'diameter', 'pha']
    df = pd.read_csv(neo_csv_path, low_memory=False, usecols=usecols)
    neos = [NearEarthObject(**row) for row in df.to_dict(orient='records')]
    return neos


def load_approaches(cad_json_path):
    """Read close approach data from a JSON file.

    :param cad_json_path: A path to a JSON file containing close approaches.
    :return: A collection of `CloseApproach`es.
    """
    with open(cad_json_path, "r") as file:
        json_data = json.load(file)
        dict_data = [dict(zip(json_data["fields"], data))
                     for data in json_data["data"]]
        approaches = [CloseApproach(d["des"], d["cd"], d["dist"], d["v_rel"])
                      for d in dict_data]
    return approaches
