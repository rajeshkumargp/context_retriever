#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json

import fetcher_config
from url_normalize import url_normalize

src = "data/fetcher_meta_data/semi_auto/class12_books_chapters_info_3.json"
dest = "data/fetcher_meta_data/refinement/class12_books_chapters_info_3_refined.json"

src = fetcher_config.REFINE_SRC
dest = fetcher_config.REFINE_DEST

src_dict = None
with open(src, "r", encoding="utf-8") as fp:
    src_dict = json.load(fp)

dest_dict = dict()
dest_dict["class12"] = list()

for a_book in src_dict:
    a_book_refined = dict()
    a_book_refined["language"] = a_book["Language"]
    a_book_refined["bookname"] = a_book["Book"]
    a_book_refined["href"] = a_book["href"]
    a_book_refined["chapters"] = None

    curr_chapters_info = a_book.get("chapter_info", None)
    if curr_chapters_info is None:
        pass
    else:
        a_book_refined["chapters"] = list()

        for chap_index in range(len(curr_chapters_info)):
            curr_chapters_info[chap_index]["src"] = url_normalize(
                curr_chapters_info[chap_index]["src"]
            )
            if curr_chapters_info[chap_index]["name"] == "Complete Book Download":
                a_book_refined["complete_book"] = url_normalize(
                    curr_chapters_info[chap_index]["src"]
                )
            else:
                a_book_refined["chapters"].append(curr_chapters_info[chap_index])

    dest_dict["class12"].append(a_book_refined)


with open(dest, "w", encoding="utf-8") as fp:
    json.dump(dest_dict, fp, ensure_ascii=False, indent=2)

print("Done")
