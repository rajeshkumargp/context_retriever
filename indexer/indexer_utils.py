#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os

import indexer_config
import indexer_exceptions
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)

c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

logger.addHandler(c_handler)
logger.info(f"Script {__file__} is getting loaded")


def find_pdf_src_from_txt_src(txt_src):
    page_wise_text_dir = os.path.dirname(txt_src)
    pdf_meta_base_dir = os.path.dirname(page_wise_text_dir)
    pdf_src_file = f"{pdf_meta_base_dir}.pdf"
    return pdf_src_file


def frame_s3_given_key(
    object_key,
    bucket_name="ttb-context-retriever-study-materials",
    region_name="ap-south-1",
):
    return f"https://{bucket_name}.s3.{region_name}/{object_key}"


def load_s3_key_local_file_to_s3_mapper(current_s3_to_local_map_file):
    contents = None
    with open(current_s3_to_local_map_file, "r", encoding="utf-8") as fp:
        contents = fp.read()
    contents = contents.split("\n")
    contents = [a_line.split("\t") for a_line in contents if len(a_line.strip()) > 0]

    local_file_s3_key_s3_url_mapper = dict()

    # Key= Local pdf, value=(S3_key, s3_url)

    local_file_s3_key_s3_url_mapper = {
        os.path.abspath(a_line[1]): (a_line[0], frame_s3_given_key(a_line[0])) for a_line in contents
    }

    return local_file_s3_key_s3_url_mapper


def paragraph_sectioner(contents):
    contents_lines = contents.split("\n")
    paragraphs = list()
    line_index = 0
    curr_para = list()
    while line_index < len(contents_lines):
        curr_line = contents_lines[line_index].strip()
        if len(curr_line) == 0:
            if len(curr_para) > 0:
                paragraphs.append(" ".join(curr_para))
                curr_para = list()
            else:
                # skipping multiple empty lines
                pass
        else:
            curr_para.append(curr_line)

        line_index += 1

    if len(curr_para) > 0:
        paragraphs.append(" ".join(curr_para))

    if len(paragraphs) == 0:
        paragraphs = [""]

    return paragraphs


def get_es_client():
    es_hosts = indexer_config.ELASTIC_SEARCH_HOSTS
    es_client = Elasticsearch(hosts=es_hosts)
    if not es_client.ping():
        raise indexer_exceptions.ES_Connection_Failure(
            "Failed to Connect Elastic Search"
        )

    return es_client


def frame_chapter_wise_info(pdf_chapters_info_file):
    class_chapter_book_info = None
    with open(pdf_chapters_info_file, "r", encoding="utf-8") as fp:
        class_chapter_book_info = json.load(fp)

    class_chapter_book_info_alter = dict()

    for a_class in class_chapter_book_info.keys():
        class_chapter_book_info_alter[a_class] = dict()
        a_class_all_subjects = class_chapter_book_info[a_class]

        for a_subject in a_class_all_subjects:
            chapters_for_curr_subjects = a_subject.get("chapters")

            if chapters_for_curr_subjects is None:
                chapters_for_curr_subjects = []

            for a_chapter in chapters_for_curr_subjects:
                basename = os.path.basename(a_chapter["chapter_url"])
                class_chapter_book_info_alter[a_class][basename] = dict()
                class_chapter_book_info_alter[a_class][basename][
                    "op_language"
                ] = a_subject["language"]
                class_chapter_book_info_alter[a_class][basename][
                    "op_subject"
                ] = a_subject["bookname"]

                class_chapter_book_info_alter[a_class][basename][
                    "op_chapter_name"
                ] = a_chapter["name"]
                class_chapter_book_info_alter[a_class][basename][
                    "op_chapter_index"
                ] = a_chapter["chapter_index"]
                class_chapter_book_info_alter[a_class][basename][
                    "op_chapter_path_url_publisher"
                ] = a_chapter["chapter_url"]
    return class_chapter_book_info_alter


def get_exisiting_uniques(fieldname, index_name, es_client, pagination_size=100):
    existing_unique_values = list()

    pagination_query = {
        "from": 0,
        "aggs": {
            "distinct_field_keys": {
                "composite": {
                    "sources": [
                        {
                            f"{fieldname}.keyword": {
                                "terms": {"field": f"{fieldname}.keyword"}
                            }
                        }
                    ],
                    "size": pagination_size,
                }
            }
        },
        "size": 0,
    }

    pagination_query_after = {
        "from": 0,
        "aggs": {
            "distinct_field_keys": {
                "composite": {
                    "sources": [
                        {
                            f"{fieldname}.keyword": {
                                "terms": {"field": f"{fieldname}.keyword"}
                            }
                        }
                    ],
                    "size": pagination_size,
                    "after": {f"{fieldname}.keyword": ""},
                }
            }
        },
        "size": 0,
    }

    # Initial Query
    uniq_fields = es_client.search(index=index_name, body=pagination_query)
    current_buckets = uniq_fields["aggregations"]["distinct_field_keys"]["buckets"]

    if len(current_buckets) > 0:
        for a_bucket_value in current_buckets:
            key_value = a_bucket_value["key"][f"{fieldname}.keyword"]
            existing_unique_values.append(key_value)

    after_key_value = (
        uniq_fields["aggregations"]["distinct_field_keys"]
        .get("after_key", {})
        .get(f"{fieldname}.keyword", None)
    )

    while after_key_value is not None:
        pagination_query_after["aggs"]["distinct_field_keys"]["composite"]["after"][
            f"{fieldname}.keyword"
        ] = after_key_value

        uniq_fields = es_client.search(index=index_name, body=pagination_query_after)

        current_buckets = uniq_fields["aggregations"]["distinct_field_keys"]["buckets"]

        if len(current_buckets) > 0:
            for a_bucket_value in current_buckets:
                key_value = a_bucket_value["key"][f"{fieldname}.keyword"]
                existing_unique_values.append(key_value)

        after_key_value = (
            uniq_fields["aggregations"]["distinct_field_keys"]
            .get("after_key", {})
            .get(f"{fieldname}.keyword", None)
        )

    return existing_unique_values


def get_exclusion_list(es_fieldname, es_index_name, es_client=None):
    es_client_close_at_the_end = False
    # Create es_client object
    if es_client is None:
        es_client = get_es_client()
        es_client_close_at_the_end = True

    exclusion_list = get_exisiting_uniques(
        fieldname=es_fieldname, index_name=es_index_name, es_client=es_client
    )

    if es_client_close_at_the_end is True:
        es_client.close()
        logger.info("Closed ElasticSearch Client")

    return exclusion_list
