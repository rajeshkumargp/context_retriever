#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os
from datetime import datetime

import indexer_config
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
    contents = [a_line.split("\t") for a_line in contents]

    local_file_s3_key_s3_url_mapper = dict()
    # Key= Local pdf, value=(S3_key, s3_url)
    local_file_s3_key_s3_url_mapper = {
        a_line[1]: (a_line[0], frame_s3_given_key(a_line[0])) for a_line in contents
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

            for a_chapter in a_subject["chapters"]:
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


def index_a_page_in_es(
    ocr_text_src_file,
    pdf_src_file,
    s3_key_local_file_url_map,
    books_subjects_chapters_info,
    es_client=None,
):

    # Create es_client object
    if es_client is None:
        es_client = get_es_client()

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
    a_page_record["relative_page_numbe_in_chapter"] = relative_page_number
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

    # inserting into ES
    # PAGE INFO

    # Framing template for a_para record
    a_para_record = dict()
    a_para_record["relative_page_numbe_in_chapter"] = relative_page_number
    a_para_record["page_content"] = page_contents

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

        # inserting into ES
        # PARA_INFO


if __name__ == "__main__":
    text_info = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books_dummy_v3/"
        "run_log_tesseract.json"
    )
    current_s3_to_local_map_file = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/"
        "books_dummy_v3/current_s3_objects_info_2020_08_15_22_00_14.txt"
    )

    pdf_chapters_info = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books/"
        "run_2020_08_12_16_09_59.json"
    )

    tess_log = None
    with open(text_info, "r", encoding="utf-8") as fp:
        tess_log = json.load(fp)

    tess_log_success = tess_log["success"]

    image_src_files = list()
    ocr_text_files = list()

    for a_log in tess_log_success:
        image_src_files.append(a_log["image_src"])
        ocr_text_files.append(a_log["ocr_text"])

    s3_key_local_file_to_s3_url_map = load_s3_key_local_file_to_s3_mapper(
        current_s3_to_local_map_file=current_s3_to_local_map_file
    )
