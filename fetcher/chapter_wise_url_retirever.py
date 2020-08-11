#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pprint import pprint

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

BOOK_URL = "http://bstbpc.gov.in/ClassXIIth.aspx"

capabilities = DesiredCapabilities().FIREFOX
capabilities["pageLoadStrategy"] = "eager"

driver = webdriver.Firefox(
    capabilities=capabilities,
    executable_path="/home/rajeshkumar/TOOLz/STANDALONE/geckodriver-v0.27.0-linux64/geckodriver",
)
driver.get(BOOK_URL)
assert "WELCOME" in driver.title


ALL_SUBJECTS_XPATH = (
    "/html/body/form/div[3]/div[5]/table/tbody/tr/td/div[1]/div/table/tbody/tr[2]/td/div/div/div/"
    + "table/tbody"
)

tbody = driver.find_elements(By.XPATH, ALL_SUBJECTS_XPATH)

if tbody is not None and len(tbody) == 1:
    tbody = tbody[0]

all_books_html = tbody.get_attribute("outerHTML")
soup = BeautifulSoup(all_books_html, "lxml")

all_subject_references = soup.find_all("tr")

headers = soup.find_all("th")
headers = [a_header.text for a_header in headers]

records_list = list()

for a_subject in all_subject_references:
    records = a_subject.find_all("td")
    if records is not None and len(records) != 0:
        # print(len(records) == len(headers))
        a_subject_meta = dict()
        for curr_header, curr_tag in zip(headers, records):
            a_subject_meta[curr_header] = curr_tag.text.strip()

        a_tag = a_subject.find("a")
        if a_tag is not None:
            a_subject_meta["href"] = a_tag.get("href", None)

        records_list.append(a_subject_meta)

print("232323232")
pprint(records_list)
print("232323232")

# #####################################################################

_TEMP_CURR_BOOK_URL = "http://bstbpc.gov.in/"

print("232323232")


for a_book_index, a_book_meta in enumerate(records_list, 1):
    CURR_BOOK_URL = f"{_TEMP_CURR_BOOK_URL}/{a_book_meta['href']}"
    driver.get(CURR_BOOK_URL)
    CHAPTER_URL_XPATH_CLICK_ELEM = "/html/body/form/div[3]/div[5]/table/tbody/tr/td/div[1]/table/tbody/tr/td/center/a"

    chapter_elements = driver.find_elements(By.XPATH, CHAPTER_URL_XPATH_CLICK_ELEM)

    chapter_info = list()
    chapter_elements_stable = list()

    number_of_elements = len(chapter_elements)

    for elem_index in range(0, number_of_elements):
        chapter_elements = driver.find_elements(By.XPATH, CHAPTER_URL_XPATH_CLICK_ELEM)

        a_chapter_elem = chapter_elements[elem_index]

        a_chapter_inner_text = a_chapter_elem.get_attribute("innerText")
        a_chapter_url = a_chapter_elem.get_attribute("href")

        if a_chapter_url.endswith(".zip"):
            pass
        else:
            a_chapter_elem.click()
            iframe_elem = driver.find_element(
                By.XPATH,
                "/html/body/form/div[3]/div[5]/table/tbody/tr/td/div[1]/table/tbody/tr[1]/td[2]/iframe",
            )
            a_chapter_url = iframe_elem.get_attribute("src")

        chapter_index = elem_index + 1

        # print({'chapter_index': chapter_index ,'name': a_chapter_inner_text, 'src': a_chapter_url})
        chapter_info.append(
            {
                "chapter_index": chapter_index,
                "name": a_chapter_inner_text,
                "src": a_chapter_url,
            }
        )

        print(f"a_book_index={a_book_index} chapter_index={chapter_index}")

    print(f"a_book_index={a_book_index} done")
    records_list[a_book_index - 1]["chapter_info"] = chapter_info

    with open(
        f"data/fetcher_meta_data/semi_auto/class12_books_chapters_info_{a_book_index}.json",
        "w",
        encoding="utf-8",
    ) as fp:
        json.dump(records_list, fp, ensure_ascii=False, indent=2)


print("232323232")

with open(
    "data/fetcher_meta_data/semi_auto/class12_books_chapters_info.json",
    "w",
    encoding="utf-8",
) as fp:
    json.dump(records_list, fp, ensure_ascii=False, indent=2)

print("Done")
