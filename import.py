"""
# Firestore Streaming Import

Utilizes ijson python json streaming library along with the official firestore
library to import a large json piecemeal into Google Cloud Firestore (BETA).
"""

import argparse
import time
import ijson.backends.yajl2_cffi as ijson

from concurrent.futures import ProcessPoolExecutor as PoolExecutor
from google.cloud import firestore
from helpers import logging_setup


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
    logger = logging_setup.get_logger()
    logger.debug("processing {}".format(document))
    # since the db object is not serializable, we need to open the database
    # every time so this method can be parallelized
    firedb = firestore.Client()
    doc_ref = firedb.collection(collection).document(document)
    doc_ref.set(data)


def save_documents(collection: str, documents: dict, debug=False):
    """
    Saves a collection of documents into the firebase database.
    This is usually called from a thread.

    Args:
        collection: the name of the collection
        documents: {document_id: {key: value ... } ... }
        debug: for logging. Are we debugging?

    Returns:
        None
    """
    try:
        logger = logging_setup.get_logger("save_documents", debug)
        firedb = firestore.Client()
        batch = firedb.batch()

        for document_id, data in documents.items():
            logger.debug("processing {}".format(document_id))
            doc_ref = firedb.collection(collection).document(document_id)
            batch.set(doc_ref, data)
        batch.commit()
    except Exception as e:
        logger.exception(e)
        logger.error("The following ids failed to insert:")
        logger.error("{}".format(documents.keys()))


def main(args):
    """
    Entry point. It parses a json file incrementally and inserts the parsed
    objects as documents in firebase
    """
    is_debug = args.debug
    logger = logging_setup.get_logger(__name__, is_debug)
    logger.info("started at {0}".format(time.time()))
    collection = args.collection
    max_per_thread = args.max_per_thread
    info_threadshold = 100000
    counter = 0
    with PoolExecutor() as executor:
        with open(args.json_file, 'rb') as json_file:
            parser = ijson.parse(json_file)
            is_array = False
            document_collection = {}
            for prefix, event, value in parser:
                # logger.debug(prefix, event, value)
                route = prefix.split(".")
                # Starting a dictionary:
                if prefix == '' and event == 'map_key':
                    document = value
                    values_dict = {}
                if event == 'start_array':
                    is_array = True
                    logger.debug("Starting array")
                if event == 'end_array':
                    logger.debug("ending array")
                    is_array = False
                # Storing a values in dictionary
                if value is not None and event not in ('map_key',):
                    curr_d = values_dict
                    if is_array:
                        logger.debug(
                            "is array. Route {}, value {}, event {}".format(
                                route, value, event))
                        for key in route[1:-3]:
                            curr_d = curr_d.setdefault(key, {})
                        curr_d = curr_d.setdefault(route[-2], [])
                        curr_d.append(convert_value(value, event))
                    else:
                        for key in route[1:-1]:
                            curr_d = curr_d.setdefault(key, {})
                        curr_d[route[-1]] = convert_value(value, event)
                # Saving the document:
                if event == 'end_map' and len(route) == 1 and prefix is not '':
                    # executor.submit(save_document2,
                    # collection, document, values_dict)
                    document_collection[document] = values_dict
                    values_dict = {}
                if len(document_collection) == max_per_thread:
                    executor.submit(save_documents,
                                    collection,
                                    document_collection,
                                    is_debug)
                    # save_documents(collection, document_collection, is_debug)
                    logger.debug(document_collection)
                    document_collection = {}
                    counter += max_per_thread
                    if counter % info_threadshold == 0:
                        logger.info(
                            "Documents saved so far: {}".format(counter))
            if document_collection:  # we have some documents left
                save_documents(collection, document_collection)
                logger.info(
                    "Documents saved in total: {}".format(
                        len(collection) + counter))
    logger.info("finished at {0}".format(time.time()))


def cli_setup():
    """
    Set up the command line arguments
    """
    arg_parser = argparse.ArgumentParser(
        description="Import a large json file into Firestore "
                    "via json Streaming.")
    arg_parser.add_argument(
        "collection", help="Specify the Firestore base collection")
    arg_parser.add_argument("json_file", help="The JSON file to import.")
    arg_parser.add_argument("-m", "--max_per_thread",

                            default=500,
                            type=int,
                            dest="max_per_thread",
                            help="Maximum number of documents to be stored"
                                 " in a single thread",
                            )
    arg_parser.add_argument("-d", "--debug", action="store_true")

    return arg_parser


if __name__ == "__main__":
    main(cli_setup().parse_args())
