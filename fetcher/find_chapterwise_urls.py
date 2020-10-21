#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pprint import pprint

import fetcher_config
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import os

BOOK_URL = fetcher_config.BOOK_URL
executable_path = fetcher_config.FIREFOX_EXECUTABLE_PATH
CHAP_WISE_URLS_JSON = 'current_run/fetcher_meta_data/class_wise_urls.json'

if not os.path.exists(os.path.dirname(CHAP_WISE_URLS_JSON)):
    os.makedirs(os.path.dirname(CHAP_WISE_URLS_JSON))

capabilities = DesiredCapabilities().FIREFOX
capabilities["pageLoadStrategy"] = "eager"

options = Options()
options.add_argument("-headless")


driver = webdriver.Firefox(
    capabilities=capabilities, executable_path=executable_path, options=options,
)
driver.get(BOOK_URL)
assert "WELCOME" in driver.title

BOOKS_MENU = "/html/body/form/div[3]/div[2]/div/nav/div/div[2]/ul/li[3]/a"


book_menu = driver.find_elements(By.XPATH, BOOKS_MENU)

WebDriverWait(driver, 20).until(
    EC.element_to_be_clickable((By.XPATH, BOOKS_MENU))
).click()

ALL_CLASSES_PATH = "/html/body/form/div[3]/div[2]/div/nav/div/div[2]/ul/li[3]/ul/li/a"

all_classes_elem = driver.find_elements(By.XPATH, ALL_CLASSES_PATH)

#######################

class_wise_home_pages = list()
#######################


for a_class, a_class_elem in enumerate(all_classes_elem, 1):
    a_class_url = a_class_elem.get_attribute("href")
    a_class_name = a_class_elem.get_attribute("innerText")
    class_wise_home_pages.append({"class_name": a_class_name, "class_url": a_class_url})

pprint(class_wise_home_pages)
print(CHAP_WISE_URLS_JSON)
with open(CHAP_WISE_URLS_JSON, "w", encoding="utf-8") as fp:
    json.dump(class_wise_home_pages, fp, ensure_ascii=False, indent=2)

print("Done")
