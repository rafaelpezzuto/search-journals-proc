import argparse
import logging
import os
import textwrap
import time
import sys
sys.path.append(os.getcwd())

from datetime import datetime, timedelta
from multiprocessing import Pool
from pymongo import UpdateOne
from utils.database import get_mongo_connection
from utils.field_cleaner import get_cleaned_default, get_cleaned_publication_date, get_cleaned_first_author_name, get_cleaned_journal_title
from xylose.scielodocument import Article, Citation


DEDUPLICATED_CITATIONS_PREFIX = os.environ.get('DEDUPLICATED_CITATIONS_PREFIX', 'gold_')
MONGO_URI_ARTICLEMETA = os.environ.get('MONGO_URI_ARTICLEMETA', 'mongodb://127.0.0.1:27017/articlemeta.articles')
MONGO_URI_STANDARDIZED = os.environ.get('MONGO_URI_STANDARDIZED', 'mongodb://127.0.0.1:27017/citations.standardized')
MONGO_URI_CITATION_HASH = os.environ.get('MONGO_URI_CITATION_HASH', 'mongodb://127.0.0.1:27017/citations')

ARTICLE_KEYS = ['cleaned_publication_date',
                'cleaned_first_author',
                'cleaned_title',
                'cleaned_journal_title']

BOOK_KEYS = ['cleaned_publication_date',
             'cleaned_first_author',
             'cleaned_source',
             'cleaned_publisher',
             'cleaned_publisher_address']

CITATION_TYPES = ('book', 'article')

chunk_size = 2000


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


def extract_citation_data(citation: Citation, cit_standardized_data=None):
    """
    Extrai os dados de uma citação.

    :param citation: Citação da qual os dados serao extraidos
    :param cit_standardized_data: Caso seja artigo, usa o padronizador de título de periódico
    :return: Dicionário composto pelos pares de nomes dos ampos limpos das citações e respectivos valores
    """
    data = _extract_citation_authors(citation)

    cleaned_publication_date = get_cleaned_publication_date(citation.publication_date)
    if cleaned_publication_date:
        data['cleaned_publication_date'] = cleaned_publication_date

    if citation.publication_type == 'article':
        cleaned_journal_title = ''
        if cit_standardized_data:
            cleaned_journal_title = cit_standardized_data['official-journal-title'][0].lower()
            if cleaned_journal_title:
                data['cleaned_journal_title'] = cleaned_journal_title

        if not cleaned_journal_title:
            cleaned_journal_title = get_cleaned_journal_title(citation.source)
            if cleaned_journal_title:
                data['cleaned_journal_title'] = cleaned_journal_title

        cleaned_title = get_cleaned_default(citation.title())
        if cleaned_title:
            data['cleaned_title'] = cleaned_title

    elif citation.publication_type == 'book':
        data.update(_extract_citation_fields_by_list(citation, ['source', 'publisher', 'publisher_address']))

        cleaned_chapter_title = get_cleaned_default(citation.chapter_title)
        if cleaned_chapter_title:
            data['cleaned_chapter_title'] = cleaned_chapter_title

    return data


def mount_citation_id(citation: Citation, collection_acronym):
    """
    Monta o id completo de uma citação.

    :param citation: Citação da qual o id completo sera montado
    :param collection_acronym: Acrônimo da coleção SciELO na qual a citação foi referida
    :return: ID completo da citação formada pelo PID do documento citante, numero da citação e coleção citante
    """
    cit_id = citation.data['v880'][0]['_']
    cit_full_id = '{0}-{1}'.format(cit_id, collection_acronym)
    return cit_full_id


def gen_key(cit_data, keys):
    """
    Cria um codigo dos dados de uma citação, com base na lista de keys.

    :param cit_data: Dicionário de pares de nome de campo e valor de campo de citação
    :param keys: Nomes dos campos a serem considerados para formar o codigo hash
    :return: Codigo hash SHA3_256 para os dados da citação
    """
    data = []
    for k in keys:
        if k in cit_data and cit_data[k]:
            data.append(cit_data[k])
        else:
            return ''

    if data:
        return '|'.join(data)


def extract_citations_ids_keys(document: Article, standardizer):
    """
    Extrai as quadras (id de citação, pares de campos de citação, hash da citação, base) para todos as citações.
    São contemplados livros, capítulos de livros e artigos.

    :param document: Documento do qual a lista de citações será convertida para hash
    :param standardizer: Normalizador de título de periódico citado
    :return: Quadra composta por id de citação, dicionário de nomes de campos e valores, hash de citação e base
    """
    citations_ids_keys = []

    if document.citations:
        for cit in [c for c in document.citations if c.publication_type in CITATION_TYPES]:
            cit_full_id = mount_citation_id(cit, document.collection_acronym)

            if cit.publication_type == 'article':
                cit_standardized_data = standardizer.find_one({'_id': cit_full_id, 'status': {'$gt': 0}})
                cit_data = extract_citation_data(cit, cit_standardized_data)

                article_hash = gen_key(cit_data, ARTICLE_KEYS)
                if article_hash:
                    citations_ids_keys.append((cit_full_id,
                                               {k: cit_data[k] for k in ARTICLE_KEYS if k in cit_data},
                                               article_hash,
                                               'article'))
            else:
                cit_data = extract_citation_data(cit)

                book_hash = gen_key(cit_data, BOOK_KEYS)
                if book_hash:
                    citations_ids_keys.append((cit_full_id,
                                               {k: cit_data[k] for k in BOOK_KEYS if k in cit_data},
                                               book_hash,
                                               'book'))

                    chapter_keys = BOOK_KEYS + ['cleaned_chapter_title', 'cleaned_chapter_first_author']

                    chapter_hash = gen_key(cit_data, chapter_keys)
                    if chapter_hash:
                        citations_ids_keys.append((cit_full_id,
                                                   {k: cit_data[k] for k in chapter_keys if k in cit_data},
                                                   chapter_hash,
                                                   'chapter'))

    return citations_ids_keys


def convert_to_mongodoc(data):
    """
    Converte dados de citação para registro em formato Mongo.

    :param data: Dados a serem convertidos (lista de quadras no formato: id de citacao, dados de citação, hash, base)
    :return: Dados convertidos
    """
    mgdocs = {'article': {}, 'book': {}, 'chapter': {}}

    for doc_id, citations_data in [d for d in data if d]:
        for cit in citations_data:
            cit_full_id = cit[0]
            cit_keys = cit[1]
            cit_sha3_256 = cit[2]
            cit_hash_mode = cit[3]

            if cit_sha3_256 not in mgdocs[cit_hash_mode]:
                mgdocs[cit_hash_mode][cit_sha3_256] = {'cit_full_ids': [], 'citing_docs': [], 'cit_keys': cit_keys}

            mgdocs[cit_hash_mode][cit_sha3_256]['cit_full_ids'].append(cit_full_id)
            mgdocs[cit_hash_mode][cit_sha3_256]['citing_docs'].append(doc_id)
            mgdocs[cit_hash_mode][cit_sha3_256]['update_date'] = datetime.now().strftime('%Y-%m-%d')

    return mgdocs


def persist_on_mongo(data):
    """
    Persiste na base Mongo os dados das chaves de de-duplicação.

    :param data: Dados a serem persistidos
    """
    mongo_data = convert_to_mongodoc(data)

    for k, v in mongo_data.items():
        mongo_uri_ch_collection = MONGO_URI_CITATION_HASH + '.' + DEDUPLICATED_CITATIONS_PREFIX + k
        writer = get_mongo_connection(mongo_uri_ch_collection)

        operations = []
        for cit_sha3_256 in v:
            new_doc = v[cit_sha3_256]
            operations.append(UpdateOne(
                filter={'_id': str(cit_sha3_256)},
                update={
                    '$set': {
                        'cit_keys': new_doc['cit_keys'],
                        'update_date': new_doc['update_date']
                    },
                    '$addToSet': {
                        'cit_full_ids': {'$each': new_doc['cit_full_ids']},
                        'citing_docs': {'$each': new_doc['citing_docs']},
                    }
                },
                upsert=True
            ))

            if len(operations) == 1000:
                writer.bulk_write(operations)
                operations = []

        if len(operations) > 0:
            writer.bulk_write(operations)


def parallel_extract_citations_ids_keys(doc_id):
    """
    Extrai usando técnica de paralelização os hashes associados às citações.

    :param doc_id: PID do documento cuja lista de referências citadas será processada
    """
    standardizer = get_mongo_connection(mongo_uri_scielo_search, COLLECTION_STANDARDIZED)
    article_meta = get_mongo_connection(mongo_uri_article_meta)

    raw = article_meta.find_one({'_id': doc_id})
    doc = Article(raw)

    citations_keys = extract_citations_ids_keys(doc, standardizer)
    if citations_keys:
        return '-'.join([doc.publisher_id, doc.collection_acronym]), citations_keys


def main():
    usage = "Gera chaves de de-duplicação de artigos, livros e capítulos citados."

    parser = argparse.ArgumentParser(textwrap.dedent(usage))

    parser.add_argument(
        '-f', '--from_date',
        type=lambda x: datetime.strptime(x, '%Y-%m-%d'),
        default=(datetime.now() - timedelta(days=7)),
        help='Obtém apenas os PIDs de artigos publicados a partir da data especificada (use o formato YYYY-MM-DD).'
             ' Valor padrão é o dia de uma semana atrás.'
    )

    parser.add_argument(
        '-u', '--until_date',
        type=lambda x: datetime.strptime(x, '%Y-%m-%d'),
        default=datetime.now(),
        help='Obtém apenas os PIDs de artigos publicados até a data especificada (use o formato YYYY-MM-DD).'
             ' Valor padrão é a dia de hoje.'
    )

    parser.add_argument(
        '-c', '--chunk_size',
        help='Tamanho de cada slice Mongo'
    )

    logging.basicConfig(level=logging.INFO)

    args = parser.parse_args()

    global chunk_size
    if args.chunk_size and args.chunk_size.isdigit() and int(args.chunk_size) > 0:
        chunk_size = int(args.chunk_size)

    mongo_filter = {'$and': [{'processing_date': {'$gte': args.from_date}},
                             {'processing_date': {'$lte': args.until_date}}]}

    article_meta = get_mongo_connection(MONGO_URI_ARTICLEMETA)
    logging.info('Getting articles ids...')
    start = time.time()

    docs_ids = [x['_id'] for x in article_meta.find(mongo_filter, {'_id': 1})]
    total_docs = len(docs_ids)

    end = time.time()
    logging.info('\tThere are %d articles to be readed' % total_docs)
    logging.info('\tDone after %.2f seconds' % (end - start))

    logging.info('[2] Generating keys...')
    start = time.time()

    chunks = range(0, total_docs, chunk_size)
    for slice_start in chunks:
        slice_end = slice_start + chunk_size
        if slice_end > total_docs:
            slice_end = total_docs

        logging.info('\t%d to %d' % (slice_start, slice_end))
        with Pool(os.cpu_count()) as p:
            results = p.map(parallel_extract_citations_ids_keys, docs_ids[slice_start:slice_end])

        persist_on_mongo(results)

    end = time.time()
    logging.info('\tDone after %.2f seconds' % (end - start))
