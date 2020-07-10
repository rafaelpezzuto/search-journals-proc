import argparse
import logging
import os
import textwrap
import time

from datetime import datetime
from hashlib import sha3_224
from multiprocessing import Pool
from pymongo import MongoClient, UpdateOne, uri_parser
from mergesearch.utils.field_cleaner import get_cleaned_default, get_cleaned_publication_date, get_cleaned_first_author_name, get_cleaned_journal_title
from xylose.scielodocument import Article, Citation


DEDUPLICATED_CITATIONS_PREFIX = os.environ.get('DEDUPLICATED_CITATIONS_PREFIX', 'dedup_')

COLLECTION_STANDARDIZED = os.environ.get('COLLECTION_STANDARDIZED', 'standardized')

ARTICLE_KEYS = ['cleaned_publication_date',
                'cleaned_first_author',
                'cleaned_title',
                'cleaned_journal_title']

BOOK_KEYS = ['cleaned_publication_date',
             'cleaned_first_author',
             'cleaned_source',
             'cleaned_publisher',
             'cleaned_publisher_address']

chunk_size = 2000

citation_types = set()

mongo_uri_scielo_search = ''

mongo_uri_article_meta = ''


def get_mongo_connection(mongo_uri, collection=None):
    """
    Obtém uma conexão com o MongoDB.

    :param mongo_uri: String de conexão MongoDB
    :param collection: Nome da coleção MongoDB
    :return: Conexão com coleção MongoDB
    """
    try:
        if collection:
            return MongoClient(mongo_uri, maxPoolSize=None).get_database().get_collection(collection)
        else:
            mongo_collection_name = uri_parser.parse_uri(mongo_uri).get('collection')
            if mongo_collection_name:
                return MongoClient(mongo_uri, maxPoolSize=None).get_database().get_collection(mongo_collection_name)
            else:
                return MongoClient(mongo_uri, maxPoolSize=None).get_database()
    except ConnectionError as ce:
        logging.error(ce)
        exit(1)


def _extract_citation_fields_by_list(citation: Citation, fields):
    """
    Extrai de uma citação os campos indicados na variável fields.

    :param citation: Citação da qual serão extraídos os campos
    :param fields: Campos a serem extraídos
    :return: Dicionário composto pelos pares campo: valor do campo
    """
    data = {}

    for f in fields:
        cleaned_v = get_cleaned_default(getattr(citation, f))
        if cleaned_v:
            data['cleaned_' + f] = cleaned_v

    return data


def _extract_citation_authors(citation: Citation):
    """
    Extrai o primeiro autor de uma citação.
    Caso citação seja capitulo de livro, extrai o primeiro autor do livro e o primeiro autor do capitulo.
    Caso citação seja livro ou artigo, extrai o primeiro autor.

    :param citation: Citação da qual o primeiro autor sera extraido
    :return: Dicionário composto pelos pares cleaned_first_author: valor e cleaned_chapter_first_author: valor
    """
    data = {}

    if citation.publication_type == 'article' or not citation.chapter_title:
        cleaned_first_author = get_cleaned_first_author_name(citation.first_author)
        if cleaned_first_author:
            data['cleaned_first_author'] = cleaned_first_author
    else:
        if citation.analytic_authors:
            cleaned_chapter_first_author = get_cleaned_first_author_name(citation.analytic_authors[0])
            if cleaned_chapter_first_author:
                data['cleaned_chapter_first_author'] = cleaned_chapter_first_author

            if citation.monographic_authors:
                cleaned_first_author = get_cleaned_first_author_name(citation.monographic_authors[0])
                if cleaned_first_author:
                    data['cleaned_first_author'] = cleaned_first_author

    return data

