from indexer_utils import frame_chapter_wise_info
from indexer_utils import load_s3_key_local_file_to_s3_mapper

from pprint import pprint

if __name__ == '__main__':
    # src = 'current_run/from_s3_books/meta/books/downloaded_classwise_subjects_chapters_status.json'
    # dest = frame_chapter_wise_info(pdf_chapters_info_file=src)
    # pprint(dest)

    src = '/home/rajeshkumar/ORGANIZED/OSC/context_retriever/current_run/from_s3_books/current_s3_objects_info_2020_10_13_17_16_33.txt'

    dest = load_s3_key_local_file_to_s3_mapper(current_s3_to_local_map_file=src)
    pprint(dest)
