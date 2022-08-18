#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import csv
import pathlib
import time

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By


def download_file(url: str, path: pathlib.Path):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with path.open("wb") as fp:
            for chunk in response.iter_content(chunk_size=8192):
                fp.write(chunk)


def scrape(parcel_number: str, driver):
    driver.get("https://gisweb.charlottesville.org/GisViewer/")

    # Search by parcel
    prop_id = driver.find_element(By.XPATH, "//input[@name='propID']")
    prop_id.send_keys(parcel_number)
    prop_id.send_keys(Keys.RETURN)

    # View search result in map
    driver.find_element(By.XPATH, "//td[contains(., 'View in Map')]").click()

    # Click print button
    driver.find_element(By.XPATH, "//span[contains(., 'Print')]").click()

    # Select jpg export
    driver.find_element(By.XPATH, "//select/option[@value='jpg100']").click()

    # Request export
    driver.find_element(By.XPATH, "//input[@value='Export']").click()

    # Extract download link
    view_link = driver.find_element(By.XPATH, "//a[contains(@href, '/GisViewer/Output')]")
    return view_link.get_attribute("href")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--buckets", type=int, default=1)
    parser.add_argument("--bucket-index", type=int, default=0)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    options = Options()
    options.headless = True
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(15)

    download_directory = pathlib.Path("images")

    with open("Parcel_Area_Details.csv") as fp:
        details = list(csv.DictReader(fp))
    details = details[args.bucket_index:len(details):args.buckets]

    for row in details:
        parcel_number = row["ParcelNumber"]
        download_path = download_directory.joinpath(f"{parcel_number}.jpg")
        if download_path.exists() and not args.overwrite:
            print(f"Skipping parcel number {parcel_number}")
        else:
            print(f"Scraping parcel number {parcel_number}")
            try:
                t0 = time.time()
                view_link = scrape(parcel_number, driver)
                print(time.time() - t0)
                download_file(view_link, download_path)
            except Exception as exc:
                print("Exception:", exc)
