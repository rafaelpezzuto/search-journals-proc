import argparse
import logging
import os
import SolrAPI
import textwrap
import sys
sys.path.append(os.getcwd())

from datetime import datetime
from utils.database import get_mongo_connection


SOLR_URL = os.environ.get('SOLR_URL', 'http://172.18.0.4:8983/solr/articles')
SOLR_ROWS_LIMIT = os.environ.get('SOLR_ROWS_LIMIT', 10000)
MONGO_URI_NORMALIZED_JOURNALS = os.environ.get('MONGO_URI_NORMALIZED_JOURNALS', 'mongodb://127.0.0.1:27017/citations.standardized')
MONGO_URI_CITATIONS = os.environ.get('MONGO_URI_CITATIONS', 'mongodb://127.0.0.1:27017/citations')
MONGO_GOLD_COLLECTIONS = os.environ.get('MONGO_GOLD_COLLECTIONS', 'gold_article,gold_book,gold_chapter').split(',')


class MergeSolr(object):

    def __init__(self,
                 gold_citation_type,
                 solr,
                 mongo,
                 persist_on_solr=False):

        self.gold_citation_type = gold_citation_type
        self.solr = solr
        self.mongo = mongo
        self.persist_on_solr = persist_on_solr

    def dump_data(self, data, filename):
        """
        Persiste dados no disco, em formato JSON

        :param data: Dados a serem persistidos
        :param filename: Nome do arquivo JSON
        """
        str_time = datetime.utcnow().isoformat(sep='_', timespec='milliseconds')
        filepath = '-'.join([self.gold_citation_type, filename, str_time]) + '.json'

        with open(os.path.join('merges', filepath), 'w') as f:
            f.write(data)

    def get_ids_for_merging(self, mongo_filter: dict):
        """
        Coleta identificadores da base de de-duplicação.

        :param mongo_filter: Filtro Mongo para selecionar parte dos identificadores
        :return: Identificadores hash das citações a serem mescladas
        """
        ids_for_merging = []

        for j in self.mongo.find(mongo_filter):

            item = {
                '_id': j['_id'],
                'cit_full_ids': j['cit_full_ids'],
                'citing_docs': j['citing_docs']
            }

            if len(item['cit_full_ids']) > 1:
                ids_for_merging.append(item)

        logging.info('There are data of %d cited references.' % len(ids_for_merging))

        return ids_for_merging

    def _merge_citation_data(self, primary_citation, others):
        """
        Mescla dados de uma citação principal com dados de outras citações.

        :param primary_citation: Citação principal
        :param others: Outras citações similares à principal
        :return: Identificadores Solr a serem removidos
        """

        ids_for_removing = set()

        for cit in others:
            raw_cit = cit.copy()

            # Mescla informação de documentos citantes
            primary_citation['document_fk'].extend(raw_cit['document_fk'])

            # Mescla informação de coleções citantes
            primary_citation['in'].extend(raw_cit['in'])

            # Mescla informação de autores citantes
            if 'document_fk_au' in raw_cit:
                if 'document_fk_au' not in primary_citation:
                    primary_citation['document_fk_au'] = []
                primary_citation['document_fk_au'].extend(raw_cit['document_fk_au'])

            # Mescla informação de periódicos citantes
            if 'document_fk_ta' in raw_cit:
                if 'document_fk_ta' not in primary_citation:
                    primary_citation['document_fk_ta'] = []
                primary_citation['document_fk_ta'].extend(raw_cit['document_fk_ta'])

            # Obtém ids das citações que devem ser removidas dos documentos citantes e do Solr
            ids_for_removing.add(raw_cit['id'])

        # Remove informações duplicadas
        self._remove_duplicated_data(primary_citation)

        # Calcula número de citações recebidas
        primary_citation['total_received'] = str(len(primary_citation['document_fk']))

        return ids_for_removing

    def _remove_duplicated_data(self, data: dict):
        for k in ['in', 'document_fk', 'document_fk_au', 'document_fk_ta']:
            if k in data:
                data[k] = sorted(set(data[k]))

    def request_docs(self, ids):
        """
        Obtém documentos Solr.

        :param ids: Lista de ids de documentos Solr a serem obtidos
        :return: Dicionário contendo documentos Solr
        """

        response = {}

        # O limite de cláusulas booleandas no Solr é 1024 (na configuração corrente)
        # Por isso, é preciso fazer mais de uma query, caso o número de ids seja > 1023
        if len(ids) < 1000:
            query = 'id:(%s)' % ' OR '.join(ids)
            response.update(eval(self.solr.select({'q': query, 'rows': SOLR_ROWS_LIMIT})))
        else:
            response = {'response': {'docs': []}}

            for start_pos in range(0, len(ids), 1000):
                last_pos = start_pos + 1000
                if last_pos > len(ids):
                    last_pos = len(ids)

                i_query = 'id:(%s)' % ' OR '.join(ids[start_pos: last_pos])
                i_response = eval(self.solr.select({'q': i_query, 'rows': SOLR_ROWS_LIMIT}))

                if len(i_response.get('response', {}).get('docs')) > 0:
                    response['response']['docs'].extend(i_response['response']['docs'])

        return response

    def mount_removing_commands(self, cits_for_removing):
        """
        Cria comandos para remoção de documentos Solr.

        :param cits_for_removing: Identificadores de documentos a serem removidos
        :return: Códigos Solr para remoção de documentos
        """
        rm_commands = []
        ids = list(cits_for_removing)

        for start_pos in range(0, len(ids), 1000):
            last_pos = start_pos + 1000
            if last_pos > len(ids):
                last_pos = len(ids)

            command = {'delete': {'query': 'id:(' + ' OR '.join([ids[k] for k in range(start_pos, last_pos)]) + ')'}}
            rm_commands.append(command)
        return rm_commands

    def persist(self, data, data_name):
        """
        Persiste data no disco e no Solr, se indicado.

        :param data: Dados a serem persistidos
        :param data_name: Nome do conjunto de dados a ser persistido
        """
        self.dump_data(str(data), data_name)

        if self.persist_on_solr:
            self.solr.update(str(data).encode('utf-8'), headers={'content-type': 'application/json'})

    def merge_citations(self, deduplicated_citations):
        """
        Mescla documentos Solr. Persiste no próprio Solr ou em disco (para posterior persistência).

        :param deduplicated_citations: Códigos hashes contendo ids de citações a serem mescladas
        """
        logging.info('Merging...')

        cits_for_merging = []
        docs_for_updating = []
        cits_for_removing = set()

        total_citations = len(deduplicated_citations)
        for counter, dc in enumerate(deduplicated_citations):
            logging.debug('%d of %d' % (counter, total_citations))

            cit_full_ids = dc['cit_full_ids']
            citing_docs = dc['citing_docs']

            response_citations = self.request_docs(cit_full_ids)

            # Mescla citações
            if len(response_citations.get('response', {}).get('docs')) > 1:
                citations = response_citations['response']['docs']

                merged_citation = {}
                merged_citation.update(citations[0])

                if '_version_' in merged_citation:
                    del merged_citation['_version_']

                ids_for_removing = self._merge_citation_data(merged_citation, citations[1:])
                cits_for_merging.append(merged_citation)

                response_documents = self.request_docs(citing_docs)

                # Atualiza documentos citantes
                for doc in response_documents.get('response', {}).get('docs', []):
                    updated_doc = {}
                    updated_doc['entity'] = 'document'
                    updated_doc['id'] = doc['id']
                    updated_doc['citation_fk'] = {'remove': list(ids_for_removing), 'add-distinct': merged_citation['id']}

                    docs_for_updating.append(updated_doc)

                # Monta instruções de remoção de citações mescladas
                cits_for_removing = cits_for_removing.union(ids_for_removing)

            # Persiste a cada 50000 comandos de mesclagem
            if len(cits_for_merging) >= 50000:
                self.persist(cits_for_merging, '0_cits_for_merging')
                cits_for_merging = []

                self.persist(docs_for_updating, '1_docs_for_updating')
                docs_for_updating = []

                rm_commands = self.mount_removing_commands(cits_for_removing)
                for counter, rm in enumerate(rm_commands):
                    self.persist(rm, '2_cits_for_removing' + '_' + str(counter))
                cits_for_removing = set()

        if len(cits_for_merging) > 0 or len(docs_for_updating) or len(cits_for_removing) > 0:
            self.persist(cits_for_merging, '0_cits_for_merging')
            self.persist(docs_for_updating, '1_docs_for_updating')

            rm_commands = self.mount_removing_commands(cits_for_removing)
            for counter, rm in enumerate(rm_commands):
                self.persist(rm, '2_cits_for_removing' + '_' + str(counter))

        if self.persist_on_solr:
            self.solr.commit()


def main():
    usage = """\
        Mescla documentos Solr do tipo citation (entity=citation).
        """

    parser = argparse.ArgumentParser(textwrap.dedent(usage))

    parser.add_argument(
        '-f', '--from_date',
        help='Obtém apenas as chaves cuja data de atualização é >= que a data especificada (use o formato YYYY-MM-DD)'
    )

    parser.add_argument(
        '--logging_level',
        '-l',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logggin level'
    )

    parser.add_argument(
        '-s', '--persist_on_solr',
        action='store_true',
        default=False,
        help='Persiste resultados diretamente no Solr'
    )

    args = parser.parse_args()

    logging.basicConfig(level=args.logging_level)

    os.makedirs('merges', exist_ok=True)

    for gc in MONGO_GOLD_COLLECTIONS:
        logging.info('Collecting data from %s...' % gc)

        mongo = get_mongo_connection(MONGO_URI_CITATIONS, gc)

        solr = SolrAPI.Solr(SOLR_URL, timeout=100)
        merger = MergeSolr(gold_citation_type=gc,
                           solr=solr,
                           mongo=mongo,
                           persist_on_solr=args.persist_on_solr)

        if args.from_date:
            mongo_filter = {'update_date': {'$gte': args.from_date}}
        else:
            mongo_filter = {}

        deduplicated_citations = merger.get_ids_for_merging(mongo_filter)
        if deduplicated_citations:
            merger.merge_citations(deduplicated_citations)

