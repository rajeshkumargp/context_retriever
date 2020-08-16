#!/usr/bin/env python3
# -*- coding: utf-8 -*-

ELASTIC_SEARCH_HOSTS = ["localhost:9200"]

# Setting for page_wise_ocr.py
TESSDATA_DIR = (
    "/home/rajeshkumar/Desktop/hindi_OCR/TESSDATA_EXPTS/tessdata_best/tessdata"
)

# Setting for page_wise_image_converter in preprocess_for_indexing.py
RUN_LOG_PDFTOPPM = "run_log_pdftoppm.json"
RUN_LOG_PDFTOPPM_STAGING = "run_log_pdftoppm_staging.json"

RUN_LOG_TESSOCR = "run_log_tesseract.json"
RUN_LOG_TESSOCR_STAGING = "run_log_tesseract_staging.json"
