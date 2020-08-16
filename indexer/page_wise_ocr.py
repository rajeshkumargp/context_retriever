#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import multiprocessing as mp
import os
import subprocess
from itertools import repeat

import indexer_config
from indexer_exceptions import (
    Batch_Length_Mismatch_Exception,
    Image_Not_Found_Exception,
    OCR_Result_Directory_Absent_Error,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)

c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

logger.addHandler(c_handler)
logger.info(f"Script {__file__} is getting loaded")

tessdata_dir = indexer_config.TESSDATA_DIR


def ocr_single_image(
    image_file_path, ocr_result_text_file_path, tessdata_dir=tessdata_dir
):
    if not os.path.exists(image_file_path):
        logger.error(f"Image file={image_file_path} not exists")
        raise Image_Not_Found_Exception(f"Image file={image_file_path} not exists")

    parent_dir = os.path.dirname(ocr_result_text_file_path)

    if not os.path.exists(parent_dir):
        logger.warning(
            f"Directory not exists for the OCR result file={ocr_result_text_file_path}"
        )
        raise OCR_Result_Directory_Absent_Error(
            f"Directory not exists for the OCR result file={ocr_result_text_file_path}"
        )

    a_page_tess_process = subprocess.Popen(
        [
            "tesseract",
            "--tessdata-dir",
            tessdata_dir,
            "-l",
            "Devanagari",
            image_file_path,
            ocr_result_text_file_path[:-4],
        ],  # skipping .txt in name
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    a_page_tess_process.wait()

    curr_return_code = a_page_tess_process.poll()
    curr_stdout = "\n".join(list(a_page_tess_process.stdout.readlines())).strip()
    curr_stderr = "\n".join(list(a_page_tess_process.stderr.readlines())).strip()

    if curr_return_code != 0:
        logger.error(
            f"OCR Issue return_code={curr_return_code} image_src={image_file_path}"
            + f"ocr_text={ocr_result_text_file_path} stdout={curr_stdout} stderr={curr_stderr}"
        )
    else:
        logger.info(
            f"OCR Issue return_code={curr_return_code} image_src={image_file_path}"
            + f"ocr_text={ocr_result_text_file_path} stdout={curr_stdout} stderr={curr_stderr}"
        )

    result = dict()
    result["return_code"] = curr_return_code
    result["stdout"] = curr_stdout
    result["stderr"] = curr_stderr
    result["image_src"] = image_file_path
    result["ocr_text"] = ocr_result_text_file_path

    return result


def batch_process_ocr_image(
    list_of_images_path,
    list_of_text_files_path,
    tessdata_dir=tessdata_dir,
    process_size=None,
    chunk_size=None,
):
    if len(list_of_images_path) != len(list_of_text_files_path):
        logger.error(
            f"Mismatch in length of image files={len(list_of_images_path)}"
            + f"and text_files_size={len(list_of_text_files_path)}"
        )

        raise Batch_Length_Mismatch_Exception(
            f"Mismatch in length of image files={len(list_of_images_path)}"
            + f"and text_files_size={len(list_of_text_files_path)}"
        )

    directory_list = set([os.path.dirname(afile) for afile in list_of_text_files_path])
    for a_directory in directory_list:
        if not os.path.exists(a_directory):
            os.makedirs(a_directory)

    if chunk_size is None:
        chunk_size = 1

    if process_size is None:
        process_size = 1
        if "sched_getaffinity" in dir(os):
            process_size = len(os.sched_getaffinity(0)) - 2

        process_size = max(process_size, mp.cpu_count() - 2, 1)

    with mp.Pool(processes=process_size) as pool:
        batch_results = pool.starmap(
            func=ocr_single_image,
            iterable=zip(
                list_of_images_path,
                list_of_text_files_path,
                repeat(tessdata_dir, len(list_of_text_files_path)),
            ),
            chunksize=chunk_size,
        )

    return batch_results


if __name__ == "__main__":
    import glob
    from pprint import pprint

    src_image_path = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books/class12/"
        "Bhogol main peryojnatmak/lhbs102_temp/lhbs102~-01.png"
    )
    ocr_text_path = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books/class12/"
        "Bhogol main peryojnatmak/lhbs102_temp/lhbs102~-01.png.txt"
    )
    result = ocr_single_image(
        image_file_path=src_image_path,
        ocr_result_text_file_path=ocr_text_path,
        tessdata_dir=tessdata_dir,
    )

    pprint(result)

    images_path = glob.glob(
        os.path.join(
            "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/"
            + "fetcher_meta_data/books/class12/Bhogol main peryojnatmak/image_dir",
            "*.png",
        )
    )

    temp_dir = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books/class12/"
        + "Bhogol main peryojnatmak/images_dir_temp"
    )

    ocr_path = [os.path.basename(a_image)[:-4] + ".txt" for a_image in images_path]

    ocr_path = [os.path.join(temp_dir, text_file_name) for text_file_name in ocr_path]

    batch_results = batch_process_ocr_image(
        list_of_images_path=images_path,
        list_of_text_files_path=ocr_path,
        process_size=3,
        chunk_size=3,
    )
    pprint(batch_results)
    print("Done")
