#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

BOOK_URL = "http://bstbpc.gov.in/Default.aspx"

driver = webdriver.Firefox(
    executable_path="/home/rajeshkumar/TOOLz/STANDALONE/geckodriver-v0.27.0-linux64/geckodriver"
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
    class_wise_home_pages.append(a_class_url)
