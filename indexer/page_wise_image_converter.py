#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import multiprocessing as mp
import os
import subprocess
from itertools import repeat

from indexer_exceptions import (
    Batch_Length_Mismatch_Exception,
    PDF_Not_Found_Exception,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)

c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

logger.addHandler(c_handler)
logger.info(f"Script {__file__} is getting loaded")


def convert_pdf_to_page_wise_images(
    pdf_file_path, meta_dir_page_image_dir, name_separator="~"
):
    if not os.path.exists(pdf_file_path):
        logger.error(f"Source PDF file={pdf_file_path} does not exists")
        return {
            "return_code": -1,
            "return_message": f"Source PDF file={pdf_file_path} does not exists",
            "in_pdf_file": pdf_file_path,
            "in_page_wise_image_dir": meta_dir_page_image_dir,
        }

        raise PDF_Not_Found_Exception(f"PDF not exists file={pdf_file_path}")

    if not os.path.exists(meta_dir_page_image_dir):
        os.makedirs(meta_dir_page_image_dir)

    pdftoppm_return_codes = {
        0: "No error",
        1: "Error opening a PDF file",
        2: "Error opening an output file.",
        3: "Error related to PDF permissions.",
        99: "Other error.",
    }

    pdf_basename = os.path.basename(pdf_file_path)
    pdf_basename = pdf_basename.rsplit(".pdf", 1)[0]

    # Image conversion process
    img_converter_process = subprocess.Popen(
        [
            "pdftoppm",
            "-r",
            "300",
            "-png",
            pdf_file_path,
            os.path.join(meta_dir_page_image_dir, pdf_basename + name_separator),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    img_converter_process.wait()

    return_code = img_converter_process.poll()
    message = pdftoppm_return_codes.get(return_code, "Unknown error")

    logger.info(
        f"PDF file={pdf_file_path} converted to page wise images with return_code={return_code} message={message}"
    )

    return {
        "return_code": return_code,
        "return_message": message,
        "in_pdf_file": pdf_file_path,
        "in_page_wise_image_dir": meta_dir_page_image_dir,
        "temp_pid": os.getppid(),
        "temp_ppid": os.getpid(),
        "temp_sub_pid": img_converter_process.pid,
    }


def batch_process_pdf_to_page_wise_image(
    list_of_pdf_file_paths,
    list_of_page_wise_dirs,
    name_separator="~",
    process_size=None,
    chunk_size=None,
):
    if len(list_of_pdf_file_paths) != len(list_of_page_wise_dirs):
        logger.error(
            f"Mismatch in length of pdf files={len(list_of_pdf_file_paths)}"
            + f"and meta_directories size={len(list_of_page_wise_dirs)}"
        )
        raise Batch_Length_Mismatch_Exception(
            f"Mismatch in length of pdf files={len(list_of_pdf_file_paths)}"
            + f"and meta_directories size={len(list_of_page_wise_dirs)}"
        )

    batch_results = [None] * len(list_of_pdf_file_paths)

    if chunk_size is None:
        chunk_size = 1

    if process_size is None:
        process_size = 1
        if "sched_getaffinity" in dir(os):
            process_size = len(os.sched_getaffinity(0)) - 2

        process_size = max(process_size, mp.cpu_count() - 2, 1)

    with mp.Pool(processes=process_size) as pool:
        batch_results = pool.starmap(
            func=convert_pdf_to_page_wise_images,
            iterable=zip(
                list_of_pdf_file_paths,
                list_of_page_wise_dirs,
                repeat(name_separator, len(list_of_pdf_file_paths)),
            ),
            chunksize=chunk_size,
        )
    return batch_results


if __name__ == "__main__":
    # Watch parallel processing using "watch -n 1 "ps u f  -C python,pdftoppm"

    pdf_file_path = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books/class12/"
        "Bhogol main peryojnatmak/lhgy301.pdf"
    )
    meta_dir_page_image_dir = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books/class12/"
        "Bhogol main peryojnatmak/tttggg"
    )

    result = convert_pdf_to_page_wise_images(
        pdf_file_path, meta_dir_page_image_dir, name_separator="~"
    )

    from pprint import pprint

    pprint(result)

    import glob

    src = (
        "/home/rajeshkumar/ORGANIZED/OSC/context_retriever/data/fetcher_meta_data/books/class12/"
        "Bhogol main peryojnatmak"
    )
    list_of_pdf_file_paths = glob.glob(os.path.join(src, "*.pdf"))
    list_of_page_wise_dirs = [
        a_pdf.rsplit(".pdf", 1)[0] + "_temp" for a_pdf in list_of_pdf_file_paths
    ]

    # process_size = 4
    # chunk_size = 2

    batch_results = batch_process_pdf_to_page_wise_image(
        list_of_pdf_file_paths=list_of_pdf_file_paths,
        list_of_page_wise_dirs=list_of_page_wise_dirs,
        # process_size=4,
        # chunk_size=1,
    )
    pprint(batch_results)
    print("Done")
