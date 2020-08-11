#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import shutil
import urllib.request
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile


def load_meta_json(source_meta_file):
    meta_info = None
    with open(source_meta_file, "r", encoding="utf-8") as fp:
        meta_info = json.load(fp)
    return meta_info


def download_file(url, dest_path, timeout=30):
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)

    current_file_name = Path(url).name
    current_file_path = os.path.join(dest_path, current_file_name)

    if os.path.exists(current_file_path):
        pass

    try:
        with urllib.request.urlopen(url=url, timeout=timeout) as response, open(
            current_file_path, "wb"
        ) as out_file:
            shutil.copyfileobj(response, out_file)

    except Exception as e:
        current_file_path = None
        print(f"url={url} -  {str(e)}")
        # e has to be logged in logger
        # raise e

    return current_file_path


def get_book_and_chapters(book_meta, book_dir):
    current_book_url = book_meta.get("complete_book", None)
    curent_book_status = None
    current_chapters_status = None

    if current_book_url is not None:
        curent_book_status = False

        downloaded_file_path = download_file(
            url=current_book_url, dest_path=book_dir, timeout=30
        )

        if downloaded_file_path is not None:
            basename = os.path.basename(downloaded_file_path)
            filename, extension = os.path.splitext(basename)

            if extension == ".zip":
                with ZipFile(downloaded_file_path, "r") as zip_obj:
                    compressed_files = zip_obj.namelist()
                    for a_file in compressed_files:
                        if not a_file.endswith("/"):
                            extract_file_path = os.path.join(
                                book_dir, os.path.basename(a_file)
                            )
                            zip_obj.extract(member=a_file, path=extract_file_path)
                os.remove(downloaded_file_path)

            curent_book_status = True

        curent_book_status = {
            "complete_book_url": book_meta.get("complete_book"),
            "status": curent_book_status,
        }

    else:
        curent_book_status = None

    current_chapters_url_info = book_meta.get("chapters", [])
    if current_chapters_url_info is not None:
        _current_chapters_status = list()
        current_chapters_status = list()
        for a_chapter_url_info in sorted(
            current_chapters_url_info, key=lambda i: i.get("name", "")
        ):
            downloaded_file_path = download_file(
                url=a_chapter_url_info["src"], dest_path=book_dir, timeout=30
            )
            _current_chapters_status.append(downloaded_file_path)

        for a_chapter_url_info, a_chapter_status in zip(
            sorted(current_chapters_url_info, key=lambda i: i.get("name", "")),
            _current_chapters_status,
        ):
            current_chapters_status.append(
                {
                    "chapter_url": a_chapter_url_info["src"],
                    "status": a_chapter_status is not None,
                    "chapter_index": a_chapter_url_info["chapter_index"],
                    "name": a_chapter_url_info["name"],
                }
            )

    else:
        current_chapters_status = None

    return curent_book_status, current_chapters_status


def fetch_books_to_local(dest_dir, meta_file_path):

    if not os.path.exists(meta_file_path):
        raise Exception(f"META FILE NOT FOUND={meta_file_path}")

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    meta_info = load_meta_json(source_meta_file=meta_file_path)

    fetcher_info = dict()

    for a_class in sorted(meta_info.keys()):
        fetcher_info[a_class] = list()

        current_class_info = meta_info[a_class]

        current_class_dir = os.path.join(dest_dir, a_class)

        if not os.path.exists(current_class_dir):
            os.makedirs(current_class_dir)

        for a_book_index, a_book in enumerate(current_class_info, 1):

            current_class_book_name = a_book["bookname"]

            current_class_book_dir = os.path.join(
                current_class_dir, current_class_book_name
            )

            if not os.path.exists(current_class_book_dir):
                os.makedirs(current_class_book_dir)

            current_book_status, current_chapters_status = get_book_and_chapters(
                book_meta=a_book, book_dir=current_class_book_dir
            )

            temp_d = dict()
            temp_d["bookname"] = a_book["bookname"]
            temp_d["complete_book"] = current_book_status
            temp_d["chapters"] = current_chapters_status
            temp_d["language"] = a_book["language"]

            fetcher_info[a_class].append(temp_d)
            del temp_d

    current_year_mon_day_hr_min_sec = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    status_tracker_file_name = f"run_{current_year_mon_day_hr_min_sec}.json".format(
        current_year_mon_day_hr_min_sec
    )
    with open(
        os.path.join(dest_dir, status_tracker_file_name), "w", encoding="utf-8"
    ) as status_file_handler:
        json.dump(fetcher_info, status_file_handler, ensure_ascii=False, indent=2)

    return fetcher_info


if __name__ == "__main__":
    from pprint import pprint

    parser = argparse.ArgumentParser(description="Get books from JSON.")
    parser.add_argument(
        "--meta_filepath",
        "-f",
        type=str,
        help="Path of json file having meta information default: data/fetcher_meta_data/class11books_sample.json",
        default="data/fetcher_meta_data/class11books_sample.json",
    )

    parser.add_argument(
        "--dest_dir",
        "-dd",
        type=str,
        help="Path of directory where resulting resources will be stored ",
        default="/tmp/trailAB",
    )

    args = parser.parse_args()
    meta_file_path = args.meta_filepath
    dest_dir = args.dest_dir

    print(f"Passed meta-file={meta_file_path}")
    print(f"Passed dest-dir={dest_dir}")

    status = fetch_books_to_local(dest_dir=dest_dir, meta_file_path=meta_file_path)
    pprint(status)
