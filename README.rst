======================================
SciELO - Search Journal Proc
======================================

.. image:: https://travis-ci.org/scieloorg/search-journals-proc.svg?branch=master
    :target: https://travis-ci.org/scieloorg/search-journals-proc

=========================
Instalação e configuração
=========================

Instalação: 

``python setup.py install``

Após a instalação é criado no python os scripts de console: 

* update_search (Atualiza o índice com os artigos do Article Meta)
* update_search_preprint (Atualiza o índice com os Preprints oferecidos pelo servidor OAI: https://preprints.scielo.org/index.php/scielo/oai/?verb=ListRecords&metadataPrefix=oai_dc)
* update_search_accesses (Atualiza os acessos dos documentos a partir do servidor de acessos: http://ratchet.scielo.org)
* update_search_citations (Atualiza as citações recebidas e concedidas a partir do servidor de citações: http://citedby.scielo.org)


======================
Como executar
======================

Veja a lista de parâmetros presente nos scripts:

``update_search --help``

::

  usage: Process to index article to SciELO Solr.

  This process collects articles in the Article meta using thrift and index
  in SciELO Solr.

  With this process it is possible to process all the article or some specific
  by collection, issn from date to until another date and a period like 7 days.

         [-h] [-x] [-p PERIOD] [-f [FROM_DATE]] [-n] [-u [UNTIL_DATE]]
         [-c COLLECTION] [-i ISSN] [-d]
         [--logging_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}]

  optional arguments:
    -h, --help            show this help message and exit
    -x, --differential    Update and Remove records according to a comparison
                          between ArticleMeta ID's and the ID' available in the
                          search engine. It will consider the processing date as
                          a compounded matching key
                          collection+pid+processing_date. This option will run
                          over the entire index, the parameters -p -f -u will
                          not take effect when this option is selected.
    -p PERIOD, --period PERIOD
                          index articles from specific period, use number of
                          days.
    -f [FROM_DATE], --from_date [FROM_DATE]
                          index articles from specific date. YYYY-MM-DD.
    -n, --load_indicators
                          Load articles received citations and downloads while
                          including or updating documents. It makes the
                          processing extremelly slow.
    -u [UNTIL_DATE], --until_date [UNTIL_DATE]
                          index articles until this specific date. YYYY-MM-DD
                          (default today).
    -c COLLECTION, --collection COLLECTION
                          use the acronym of the collection eg.: spa, scl, col.
    -i ISSN, --issn ISSN  journal issn.
    -d, --delete          delete query ex.: q=*:* (Lucene Syntax).
    --logging_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}, -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                          Logggin level

``update_search_preprint --help``

::

  usage: Process to index Pre-Prints articles to SciELO Solr.

         [-h] [-t TIME] [-d DELETE] [-solr_url SOLR_URL] [-oai_url OAI_URL] [-v]

  optional arguments:
    -h, --help            show this help message and exit
    -t TIME, --time TIME  index articles from specific period, use number of
                          hours.
    -d DELETE, --delete DELETE
                          delete query ex.: q=type:"preprint (Lucene Syntax).
    -solr_url SOLR_URL, --solr_url SOLR_URL
                          Solr RESTFul URL, processing try to get the variable
                          from environment ``SOLR_URL`` otherwise use --solr_url
                          to set the solr_url (preferable).
    -oai_url OAI_URL, --oai_url OAI_URL
                          OAI URL, processing try to get the variable from
                          environment ``OAI_URL`` otherwise use --oai_url to set
                          the oai_url (preferable).
    -v, --version         show program's version number and exit


======================
Como executar os tests
======================

- Para rodar os tests de unidade: ``python setup.py test``


===========================================
Arquivos: Dockerfile* e docker-compose*.yml
===========================================


- **Dockerfile**: contém as definições para construir a imagem pronta para instalar em **produção**
- **Dockerfile-dev**: contém as definições para construir a imagem pronta para instalar em **desenvolvimento**

- **docker-compose.yml**: contém as definições para iniciar todos os containers necessários para rodar em **produção**
- **docker-compose-dev.yml**: contém as definições para iniciar todos os containers necessários para rodar em **desenvolvimento**


=================================================
Instalação utilizando Docker para desenvolvimento
=================================================


Para executar o ambiente (de desenvolvimento) com Docker, utilizando as definições do arquivo **Dockerfile-dev** e **docker-compose.yml-dev** na raiz do projeto.
Simplesmente executar:

1. executar: ``docker-compose -f docker-compose-dev.yml build`` para construir a imagem do OPAC.
2. executar: ``docker-compose up``  para rodar os containers.

Repare que irá iniciar o processamento dos artigos SciELO e dos preprints, pois dentro do docker-compose.yml está configurado os seguintes comandos: 

``update_search -c sss -p 30``

``update_search_preprint -p 1``


=========================================
Reportar problemas, ou solicitar mudanças
=========================================

Para reportar problemas, bugs, ou simplesmente solicitar alguma nova funcionalidade, pode `criar um ticket <https://github.com/search-journals-proc/opac/issues>`_ com seus pedidos.


