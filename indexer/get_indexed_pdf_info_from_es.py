#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

import indexer_config
from elasticsearch import Elasticsearch
import elasticsearch

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)

c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

logger.addHandler(c_handler)
logger.info(f"Script {__file__} is getting loaded")


es_hosts = indexer_config.ELASTIC_SEARCH_HOSTS


def get_existing_indexed_pdf_from_es(hosts):
    es = Elasticsearch(hosts=es_hosts)
    es.indices.create(index="page_content", ignore=400)
    es.indices.create(index="para_content", ignore=400)

    query_uniq_pdfs = {
        "size": 0,
        "aggs": {"distinct_pdfs": {"terms": "chapter_path", "size": 1000}},
    }
    try:
        uniq_pdfs = es.search(index="page_content", body=query_uniq_pdfs)
    except elasticsearch.exceptions.RequestError as e:
        print(str(e))
        uniq_pdfs = None

    es.close()

    return uniq_pdfs


if __name__ == "__main__":
    uniq_pdfs = get_existing_indexed_pdf_from_es(hosts=es_hosts)
    from pprint import pprint

    pprint(uniq_pdfs)
    print("Done")
