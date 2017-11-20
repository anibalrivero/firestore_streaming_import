"""
# Firestore Streaming Import

Utilizes ijson python json streaming library along with the official firestore
library to import a large json piecemeal into Google Cloud Firestore (BETA).
"""

import argparse
import time
from concurrent.futures import ProcessPoolExecutor as PoolExecutor
from google.cloud import firestore
import ijson.backends.yajl2_cffi as ijson

def convert_value(value: "any", value_type: str):
    """
    ijson converts all the numbers into Decimal type, this cast them to float
    so they can be inserted in firestore.
    """
    if value_type == 'number':
        return float(value)
    return value


def save_document2(collection: str, document: str, data: dict):
    """
    Saves the data into the firebase database.

    """
    # print("processing {}".format(document))
    # since the db object is not serializable, we need to open the database
    # every time so this method can be parallelized
    firedb = firestore.Client()
    doc_ref = firedb.collection(collection).document(document)
    doc_ref.set(data)


def main(args):
    """
    Entry point. It parses a json file incrementally and inserts the parsed
    objects as documents in firebase
    """
    print("started at {0}".format(time.time()))
    collection = args.collection
    with PoolExecutor() as executor:
        with open(args.json_file, 'rb') as json_file:
            parser = ijson.parse(json_file)
            for prefix, event, value in parser:
                # print(prefix, event, value)
                route = prefix.split(".")
                if prefix == '' and event == 'map_key':
                    document = value
                    values_dict = {}
                if value is not None and event not in ('map_key', ):
                    curr_d = values_dict
                    for key in route[1:-1]:
                        curr_d = curr_d.setdefault(key, {})
                    curr_d[route[-1]] = convert_value(value, event)
                if event == 'end_map' and len(route) == 1 and prefix is not '':
                    executor.submit(
                        save_document2, collection, document, values_dict)
                    values_dict = {}
    print("finished at {0}".format(time.time()))

def cli_setup():
    """
    Set up the command line arguments
    """
    arg_parser = argparse.ArgumentParser(
        description="Import a large json file into Firestore "
        "via json Streaming.")
    arg_parser.add_argument(
        'collection', help="Specify the Firestore base collection")
    arg_parser.add_argument('json_file', help="The JSON file to import.")

    return arg_parser

if __name__ == '__main__':
    main(cli_setup().parse_args())
