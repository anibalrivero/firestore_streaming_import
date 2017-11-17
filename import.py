import ijson.backends.yajl2_cffi as ijson
from google.cloud import firestore
import argparse
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor as PoolExecutor

def convert_value(value, value_type):
    """
    ijson converts all the numbers into Decimal type, this cast them to float
    so they can be inserted in firestore.
    """
    if value_type == 'number':
        return float(value)
    return value

def save_document2(collection, document, data):
    # print("processing {}".format(document))
    # since the db object is not serializable, we need to open the database
    # every time so this method can be parallelized
    db = firestore.Client() 
    doc_ref = db.collection(collection).document(document)
    doc_ref.set(data)

def main(args):
    print("started at {0}".format(time.time()))
    collection = args.collection
    with PoolExecutor() as executor:
        with open(args.json_file, 'rb') as json_file:
            parser = ijson.parse(json_file)
            values = {}
            for prefix, event, value in parser:
                # print(prefix, event, value)
                route = prefix.split(".")
                if prefix == '' and event == 'map_key':
                    document = value
                if event == 'start_map':
                    values_dict = {route[-1]: {}}
                if value is not None and event not in ('map_key', ):
                    values_dict[route[-2]][route[-1]] = convert_value(value, event)
                if event == 'end_map' and len(route) == 1 and prefix is not '':
                    executor.submit(
                        save_document2, collection, document, values_dict)
    print("finished at {0}".format(time.time()))

if __name__ == '__main__':
    argParser = argparse.ArgumentParser(
        description="Import a large json file into Firestore "
        "via json Streaming.")
    argParser.add_argument(
        'collection', help="Specify the Firestore base collection")
    argParser.add_argument('json_file', help="The JSON file to import.")

    main(argParser.parse_args())
