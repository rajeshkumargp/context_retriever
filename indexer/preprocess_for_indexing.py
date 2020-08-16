#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import glob
import json
import logging
import os

import indexer_config
from indexer_exceptions import (
    Books_Directory_Absent,
    Exclusion_List_File_Absent,
)
from page_wise_image_converter import batch_process_pdf_to_page_wise_image
from page_wise_ocr import batch_process_ocr_image

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)

c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

logger.addHandler(c_handler)
logger.info(f"Script {__file__} is getting loaded")


tessdata_dir = indexer_config.TESSDATA_DIR


def get_all_pdf_paths(src):
    all_pdfs = glob.glob(os.path.join(src, "**", "*.pdf"), recursive=True)
    return all_pdfs


def frame_meta_dir_images(src_pdf):
    def get_for_a_instance(a_src):
        base_path = a_src.rsplit(".pdf", 1)[0]
        return os.path.join(base_path, "page_wise_images")

    if isinstance(src_pdf, str):
        return get_for_a_instance(a_src=src_pdf)

    if isinstance(src_pdf, list):
        return [get_for_a_instance(a_src=a_src) for a_src in src_pdf]


def filter_pdfs(list_of_pdf, exclusion_list):
    filtered_pdf = list()

    for a_pdf in list_of_pdf:
        found_match = False
        for a_exclusion in exclusion_list:
            if a_pdf.endswith(a_exclusion):
                found_match = True
                break
        if found_match is False:
            filtered_pdf.append(a_pdf)

    return filtered_pdf


def split_by_return_codes(list_of_results, select_fields, success_return_codes=[0]):

    assert isinstance(select_fields, list)
    assert len(select_fields) > 0

    split_up = dict()
    split_up["status"] = dict()
    split_up["status"]["success"] = list()
    split_up["status"]["failure"] = list()
    split_up["success_reformat"] = dict()

    for afield in select_fields:
        split_up["success_reformat"][afield] = list()

    for a_result in list_of_results:
        if a_result.get("return_code", -1) in success_return_codes:
            split_up["status"]["success"].append(a_result)

            for afield in select_fields:
                split_up["success_reformat"][afield].append(a_result.get(afield, ""))
        else:
            split_up["status"]["failure"].append(a_result)

    return split_up


def preprocess_for_indexing(src_study_materials_books, exclusion_list_file=None):

    if not os.path.exists(src_study_materials_books):
        logger.error(f"{src_study_materials_books} - path does not exists")
        raise Books_Directory_Absent(
            f"{src_study_materials_books} - path does not exists"
        )

    exclusion_list = list()
    if exclusion_list_file is not None:
        if not os.path.exists(exclusion_list_file):
            logger.error(f"Exclusion list file={exclusion_list_file} is absent")
            raise Exclusion_List_File_Absent(
                f"Exclusion list file={exclusion_list_file} is absent"
            )

        with open(exclusion_list_file, "r", encoding="utf-8") as excl_file:
            exclusion_list = excl_file.read().split("\n")

        logger.info(f"Parsed exclusion file={exclusion_list_file}")
        exclusion_list = sorted(exclusion_list)

    all_study_materials_books_pdf = get_all_pdf_paths(src_study_materials_books)
    logger.info(
        f"All PDF files are identified from path={src_study_materials_books} having"
        + f"{len(all_study_materials_books_pdf)} pdfs"
    )

    filtered_all_study_materials_books_pdf = filter_pdfs(
        list_of_pdf=all_study_materials_books_pdf, exclusion_list=exclusion_list
    )
    logger.info(
        f"after filtering from exclusion, {len(filtered_all_study_materials_books_pdf)} pdfs are there"
    )

    meta_dir_page_wise_images = frame_meta_dir_images(
        src_pdf=filtered_all_study_materials_books_pdf
    )

    image_results = batch_process_pdf_to_page_wise_image(
        list_of_pdf_file_paths=filtered_all_study_materials_books_pdf,
        list_of_page_wise_dirs=meta_dir_page_wise_images,
    )
    image_results_split_up = split_by_return_codes(
        list_of_results=image_results,
        select_fields=["in_pdf_file", "in_page_wise_image_dir"],
        success_return_codes=[0],
    )

    logger.info(
        f"PDFtoImage conversion done- Success={len(image_results_split_up['status']['success'])}"
        + f"/{len(image_results)} Failure={len(image_results_split_up['status']['failure'])}/{len(image_results)}"
    )

    image_results_split_up_success_img_dir = image_results_split_up["success_reformat"][
        "in_page_wise_image_dir"
    ]

    image_conversion_log = os.path.join(
        os.path.dirname(src_study_materials_books), "run_log_pdftoppm.json"
    )
    with open(image_conversion_log, "w", encoding="utf-8") as out:
        json.dump(image_results_split_up["status"], out, ensure_ascii=False, indent=2)

    in_tess_list_of_images_path = [
        glob.glob(os.path.join(a_dir, "*.png"))
        for a_dir in image_results_split_up_success_img_dir
    ]
    in_tess_list_of_images_path = [
        a_image for a_dir in in_tess_list_of_images_path for a_image in a_dir
    ]

    in_tess_list_of_text_files_path = [
        a_image.replace(".png", ".txt").replace("page_wise_images", "page_wise_text")
        for a_image in in_tess_list_of_images_path
    ]

    logger.info(
        f"OCR inputs - Total number of images={len(in_tess_list_of_images_path)}"
    )

    ocr_results = batch_process_ocr_image(
        list_of_images_path=in_tess_list_of_images_path,
        list_of_text_files_path=in_tess_list_of_text_files_path,
        tessdata_dir=tessdata_dir,
    )
    logger.info(f"OCR Process done for images_count={len(in_tess_list_of_images_path)}")

    ocr_results_split_up = split_by_return_codes(
        list_of_results=ocr_results,
        select_fields=["image_src", "ocr_text"],
        success_return_codes=[0],
    )

    logger.info(
        f"OCR Results Log success={len(ocr_results_split_up['status']['success'])}"
        + f"/{len(ocr_results)} failure={len(ocr_results_split_up['status']['failure'])}/{len(ocr_results)}"
    )

    ocr_conversion_log = os.path.join(
        os.path.dirname(src_study_materials_books), "run_log_tesseract.json"
    )
    with open(ocr_conversion_log, "w", encoding="utf-8") as out:
        json.dump(ocr_results_split_up["status"], out, ensure_ascii=False, indent=2)

    return ocr_results_split_up["success_reformat"]


if __name__ == "__main__":
    src_materials = "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books_dummy_v3/books"
    exclusion_list_file = None
    pp = preprocess_for_indexing(
        src_study_materials_books=src_materials, exclusion_list_file=None
    )
    from pprint import pprint

    pprint(pp)
