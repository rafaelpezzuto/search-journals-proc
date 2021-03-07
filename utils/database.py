import logging

from pymongo import MongoClient, uri_parser


def get_mongo_connection(mongo_uri, collection=None):
    try:
        puri = uri_parser.parse_uri(mongo_uri)
        u_collection = puri['collection']
        u_database = puri['database']

        if u_collection:
            return MongoClient(mongo_uri, maxPoolSize=None).get_database(u_database).get_collection(u_collection)
        elif collection:
            return MongoClient(mongo_uri, maxPoolSize=None).get_database(u_database).get_collection(collection)
        return MongoClient(mongo_uri, maxPoolSize=None).get_database(u_database)
    except ConnectionError as ce:
        logging.error(ce)
        exit(1)
