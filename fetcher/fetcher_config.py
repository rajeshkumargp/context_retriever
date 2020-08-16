#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This will have settings

# Settings for  find_chapterwise_urls.py
BOOK_URL = "http://bstbpc.gov.in/Default.aspx"
FIREFOX_EXECUTABLE_PATH = (
    "/home/rajeshkumar/TOOLz/STANDALONE/geckodriver-v0.27.0-linux64/geckodriver"
)


# Settings for chapter_wise_url_retirever.py
CLASS_A_BOOKS_URL = "http://bstbpc.gov.in/ClassXIIth.aspx"
FIREFOX_EXECUTABLE_PATH_OTHER = (
    "/home/rajeshkumar/TOOLz/STANDALONE/geckodriver-v0.27.0-linux64/geckodriver"
)
RESULT_JSON = "data/fetcher_meta_data/semi_auto/class12_books_chapters_info.json"

# Settings for refine_extracted_urls.py
REFINE_SRC = "data/fetcher_meta_data/semi_auto/class12_books_chapters_info_3.json"
REFINE_DEST = (
    "data/fetcher_meta_data/refinement/class12_books_chapters_info_3_refined.json"
)

# Settings for get_books.py
TIMEOUT = 30
DOWNLOADED_BOOKS_STATUS_LOG_FILE_NAME = (
    "downloaded_classwise_subjects_chapters_status.json"
)

# Setting Get from s3
CURRENT_S3_KEY_TO_LOCAL_FILE_MAP = "current_s3_objects_info.txt"
