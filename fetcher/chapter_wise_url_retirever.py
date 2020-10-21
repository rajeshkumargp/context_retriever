#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
from urllib.parse import urlparse

import fetcher_config
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)

c_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

logger.addHandler(c_handler)

logger.info("Program started")

# Loading settings

BOOK_URL = fetcher_config.CLASS_A_BOOKS_URL
executable_path = fetcher_config.FIREFOX_EXECUTABLE_PATH_OTHER
result_json = fetcher_config.RESULT_JSON


capabilities = DesiredCapabilities().FIREFOX
capabilities["pageLoadStrategy"] = "eager"

options = Options()
options.add_argument("-headless")

driver = webdriver.Firefox(
    capabilities=capabilities, executable_path=executable_path, options=options,
)
driver.get(BOOK_URL)
# assert "WELCOME" in driver.title

logger.info("Loaded Selenium Driver")

ALL_SUBJECTS_XPATH = (
    "/html/body/form/div[3]/div[5]/table/tbody/tr/td/div[1]/div/table/tbody/tr[2]/td/div/div/div/"
    + "table/tbody"
)

tbody = driver.find_elements(By.XPATH, ALL_SUBJECTS_XPATH)

if tbody is not None and len(tbody) == 1:
    tbody = tbody[0]
else:
    print(len(tbody))

all_books_html = tbody.get_attribute("outerHTML")
soup = BeautifulSoup(all_books_html, "lxml")

all_subject_references = soup.find_all("tr")

headers = soup.find_all("th")
headers = [a_header.text for a_header in headers]

records_list = list()

for a_subject in all_subject_references:
    records = a_subject.find_all("td")
    if records is not None and len(records) != 0:
        a_subject_meta = dict()
        for curr_header, curr_tag in zip(headers, records):
            a_subject_meta[curr_header] = curr_tag.text.strip()

        a_tag = a_subject.find("a")
        if a_tag is not None:
            a_subject_meta["href"] = a_tag.get("href", None)

        records_list.append(a_subject_meta)

logger.info("Retrieved Subjects and webpage info for each subject")

# #####################################################################

_temp = urlparse(BOOK_URL)
_TEMP_CURR_BOOK_URL = f"{_temp.scheme}://{_temp.netloc}"

logger.info("Retrieval of Subjects- name and url done")

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

        logger.info(
            f"a_book_index={a_book_index} bookname={a_book_meta['Book']} chapter_index={chapter_index}"
        )

    logger.info(f"a_book_index={a_book_index} bookname={a_book_meta['Book']} done")

    records_list[a_book_index - 1]["chapter_info"] = chapter_info

    temp_json = f"{result_json.rsplit('.json', 1)[0]}_{a_book_index}.json"

    with open(temp_json, "w", encoding="utf-8",) as fp:
        json.dump(records_list, fp, ensure_ascii=False, indent=2)

    logger.info(f"Writing info to the file {temp_json}")


logger.info(f"Writing info to the file {result_json}")

with open(result_json, "w", encoding="utf-8",) as fp:
    json.dump(records_list, fp, ensure_ascii=False, indent=2)

print("Done")
