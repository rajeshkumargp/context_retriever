#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os
from datetime import datetime
import glob

from indexer_utils import (
    find_pdf_src_from_txt_src,
    frame_chapter_wise_info,
    get_es_client,
    load_s3_key_local_file_to_s3_mapper,
    paragraph_sectioner,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)

c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

logger.addHandler(c_handler)
logger.info(f"Script {__file__} is getting loaded")


def index_a_page_in_es(
    ocr_text_src_file,
    pdf_src_file,
    s3_key_local_file_url_map,
    books_subjects_chapters_info,
    es_client=None,
):
    es_client_close_at_the_end = False
    # Create es_client object
    if es_client is None:
        es_client = get_es_client()
        es_client_close_at_the_end = True

    indexing_status_failures = list()
    para_counters_success = 0
    para_counters_failures = 0

    # pdf basename
    pdf_basename = os.path.basename(pdf_src_file)

    # Page contents
    page_contents = ""
    with open(ocr_text_src_file, "r", encoding="utf-8") as fp:
        page_contents = fp.read()

    # list of paragraphs
    paragraphs = paragraph_sectioner(page_contents)

    relative_page_number = int(ocr_text_src_file.rsplit("~-")[1].rsplit(".txt")[0])

    s3_key, s3_url = s3_key_local_file_url_map[pdf_src_file]

    material_category, class_info, bookname, chapter_file_name = s3_key.split("/")

    # Framing a page Record
    a_page_record = dict()
    a_page_record["relative_page_number_in_chapter"] = relative_page_number
    a_page_record["page_content"] = page_contents

    a_page_record["class"] = class_info
    a_page_record["subject"] = bookname

    current_pdf_meta_info = books_subjects_chapters_info.get(class_info, {}).get(
        pdf_basename, {}
    )

    a_page_record["op_subject"] = current_pdf_meta_info.get("op_subject", "")
    a_page_record["op_language"] = current_pdf_meta_info.get("op_language", "")

    a_page_record["op_chapter_name"] = current_pdf_meta_info.get("op_chapter_name", "")
    a_page_record["op_chapter_index"] = current_pdf_meta_info.get(
        "op_chapter_index", ""
    )
    a_page_record["op_chapter_path_url_publisher"] = current_pdf_meta_info.get(
        "op_chapter_path_url_publisher", ""
    )

    a_page_record["chapter_path_url_s3"] = s3_url
    a_page_record["chapter_path_key"] = s3_key
    a_page_record["ins_timestamp"] = datetime.now()

    # ingestion in elastic search
    index_status = es_client.index(
        index="page_content", body=a_page_record, doc_type="page_content"
    )
    if index_status["result"] != "created":
        temp = dict()
        temp = a_page_record
        temp["ocr_text_src_file"] = ocr_text_src_file
        indexing_status_failures.append(temp)
        del temp

        logger.error(f"ES Index Failure on text_source={ocr_text_src_file}")

    # Framing template for a_para record
    a_para_record = dict()
    a_para_record["relative_page_number_in_chapter"] = relative_page_number
    # a_para_record["page_content"] = page_contents

    a_para_record["class"] = class_info
    a_para_record["subject"] = bookname

    current_pdf_meta_info = books_subjects_chapters_info.get(class_info, {}).get(
        pdf_basename, {}
    )
    a_para_record["op_subject"] = current_pdf_meta_info.get("op_subject", "")
    a_para_record["op_language"] = current_pdf_meta_info.get("op_language", "")

    a_para_record["op_chapter_name"] = current_pdf_meta_info.get("op_chapter_name", "")
    a_para_record["op_chapter_index"] = current_pdf_meta_info.get(
        "op_chapter_index", ""
    )
    a_para_record["op_chapter_path_url_publisher"] = current_pdf_meta_info.get(
        "op_chapter_path_url_publisher", ""
    )

    a_para_record["chapter_path_url_s3"] = s3_url
    a_para_record["chapter_path_key"] = s3_key

    # To be populated dynamically

    a_para_record["ins_timestamp"] = ""
    a_para_record["relative_para_number_in_page"] = ""
    a_para_record["para_content"] = ""

    for a_para_index, a_para in enumerate(paragraphs, 1):
        a_para_record["ins_timestamp"] = datetime.now()
        a_para_record["relative_para_number_in_page"] = a_para_index
        a_para_record["para_content"] = a_para

        # ingestion in elastic search
        index_status = es_client.index(
            index="para_content", body=a_para_record, doc_type="para_content"
        )
        if index_status["result"] != "created":
            temp = dict()
            temp = a_para_record
            temp["ocr_text_src_file"] = ocr_text_src_file
            indexing_status_failures.append(temp)
            del temp
            para_counters_failures += 1
        else:
            para_counters_success += 1

    logger.info(
        f"Text_file={ocr_text_src_file} success_para={para_counters_success} failure_para={para_counters_failures}"
    )

    if es_client_close_at_the_end is True:
        es_client.close()
        logger.info("Closed ElasticSearch Client ")

    return indexing_status_failures


def batch_index_list_of_pages(
    list_of_ocr_text_src_files,
    s3_key_local_file_url_map_file,
    books_subjects_chapters_info_file,
    es_client=None,
):
    # Check for Length Match

    assert os.path.exists(s3_key_local_file_url_map_file)
    assert os.path.exists(books_subjects_chapters_info_file)

    s3_key_local_file_to_s3_url_map = load_s3_key_local_file_to_s3_mapper(
        current_s3_to_local_map_file=s3_key_local_file_url_map_file
    )

    books_subjects_chapters_info = frame_chapter_wise_info(
        books_subjects_chapters_info_file
    )

    list_of_pdf_src_files = [
        find_pdf_src_from_txt_src(a_ocr_text_file)
        for a_ocr_text_file in list_of_ocr_text_src_files
    ]

    # Indexing into Elastic Search
    es_client_close_at_the_end = False
    # Create es_client object
    if es_client is None:
        es_client = get_es_client()
        es_client_close_at_the_end = True

    indexing_status_failures = list()

    total_files_count = len(list_of_ocr_text_src_files)

    for file_index, (a_ocr_text_file, a_pdf_src_file) in enumerate(
        zip(list_of_ocr_text_src_files, list_of_pdf_src_files), 1
    ):
        indexing_status = index_a_page_in_es(
            ocr_text_src_file=a_ocr_text_file,
            pdf_src_file=a_pdf_src_file,
            s3_key_local_file_url_map=s3_key_local_file_to_s3_url_map,
            books_subjects_chapters_info=books_subjects_chapters_info,
            es_client=es_client,
        )
        if len(indexing_status) > 0:
            indexing_status_failures.append(indexing_status)
            logger.warning(
                f"Indexed pages {file_index}/{total_files_count} page={a_ocr_text_file} with some errors"
            )

        logger.info(
            f"Indexed pages {file_index}/{total_files_count} page={a_ocr_text_file} Done"
        )

    if es_client_close_at_the_end is True:
        es_client.close()
        logger.info("Closed ElasticSearch Client ")

    return indexing_status_failures


if __name__ == "__main__":
    text_info = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books_dummy_v3/"
        "run_log_tesseract.json"
    )
    current_s3_to_local_map_file = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/"
        "books_dummy_v3/current_s3_objects_info_2020_08_15_22_00_14_dummy.txt"
    )

    pdf_chapters_info = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books/"
        "run_2020_08_12_16_09_59.json"
    )

    # ##########################################

    #  CONFIGURE THIS

    pdf_chapters_info = 'current_run/from_s3_books/meta/books/downloaded_classwise_subjects_chapters_status.json'

    text_info = ''

    current_s3_to_local_map_file = "current_run/from_s3_books/current_s3_objects_info_2020_10_13_17_16_33.txt"

    # ##########################################
    #
    # tess_log = None
    # with open(text_info, "r", encoding="utf-8") as fp:
    #     tess_log = json.load(fp)
    #
    # tess_log_success = tess_log["success"]
    #
    # image_src_files = list()
    # ocr_text_files = list()
    #
    # for a_log in tess_log_success:
    #     image_src_files.append(a_log["image_src"])
    #     ocr_text_files.append(a_log["ocr_text"])

    src = '/home/rajeshkumar/ORGANIZED/OSC/context_retriever/current_run/from_s3_books/books/class12'
    ocr_text_files = glob.glob(src + "/*/*/page_wise_text/*.txt", recursive=True)

    indexing_failures = batch_index_list_of_pages(
        list_of_ocr_text_src_files=ocr_text_files,
        s3_key_local_file_url_map_file=current_s3_to_local_map_file,
        books_subjects_chapters_info_file=pdf_chapters_info,
        es_client=None,
    )
    from pprint import pprint

    pprint(indexing_failures)
    print("Script Ended")
