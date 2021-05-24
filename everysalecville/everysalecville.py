#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import io
import os
import pathlib
import shelve
import urllib
from typing import Dict, List, Optional

import humanize
import lxml.html
import requests
import tweepy
import twitter_bot_utils


SALES_URL = "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_2/MapServer/3/query"
DETAILS_URL = "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_1/MapServer/72/query"
IMAGE_URL = "https://gisweb.charlottesville.org/GisViewer/ParcelViewer/Details"

SHELF_PATH = "shelf.db"
GIS_IMAGE_PATH = pathlib.Path("images")


def main(shelf, client, start_date):
    sales = get_sales(start_date)
    for sale in sales:
        parcel_number = sale["ParcelNumber"]
        if parcel_number in shelf:
            continue
        if sale["SaleAmount"] == 0:
            continue

        details = get_details(parcel_number)
        address = f"{details['StreetNumber']} {details['StreetName']}"
        if details["Unit"]:
            address = f"{address} Unit {details['Unit']}"
        sale_date = datetime.datetime.fromtimestamp(sale["SaleDate"] / 1000).date()
        sale_amount = humanize.intcomma(sale["SaleAmount"])
        assessment = humanize.intcomma(details["Assessment"])
        status = f"{address}, sold on {sale_date} for ${sale_amount}. Zoned {details['Zoning']}, assessed at ${assessment}."

        media_ids = []
        photo_image = get_image(parcel_number)
        if photo_image:
            photo_upload = client.media_upload(filename=f"{parcel_number}.jpg", file=photo_image)
            media_ids.append(photo_upload.media_id)
        gis_image = GIS_IMAGE_PATH.joinpath(f"{parcel_number}.jpg")
        if gis_image.exists():
            gis_upload = client.media_upload(str(gis_image))
            media_ids.append(gis_upload.media_id)

        status = client.update_status(status=status, media_ids=media_ids)
        shelf[parcel_number] = status.id
        shelf.sync()


def get_sales(start_date: Optional[datetime.date] = None) -> List[Dict]:
    start_date = start_date or datetime.date.today() - datetime.timedelta(days=1)
    start_query = start_date.strftime("%Y-%m-%d %H:%M%S")
    params = {
        "where": f"SaleDate >= TIMESTAMP '{start_query}'",
        "outFields": "*",
        "f": "json",
    }
    response = requests.post(SALES_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return [each["attributes"] for each in data["features"]]


def get_details(parcel_number: str):
    params = {
        "where": f"ParcelNumber = '{parcel_number}'",
        "outFields": "*",
        "f": "json",
    }
    response = requests.get(DETAILS_URL, params=params)
    response.raise_for_status()
    data = response.json()
    assert len(data["features"]) == 1
    return data["features"][0]["attributes"]


def get_image(parcel_number: str) -> Optional[io.BytesIO]:
    params = {
        "Key": parcel_number,
        "SearchOptionIndex": 0,
        "DetailsTabIndex": 0,
    }
    details_response = requests.get(IMAGE_URL, params=params)
    details_response.raise_for_status()
    page = lxml.html.fromstring(details_response.content)
    urls = page.xpath('//img[contains(@src, "realestate.charlottesville.org")]/@src')
    if urls:
        image_response = requests.get(urls[0])
        image_response.raise_for_status()
        return io.BytesIO(image_response.content)
    else:
        return None


if __name__ == "__main__":
    client = twitter_bot_utils.API(screen_name="everysalecville", config_file=os.getenv("TWITTER_CONFIG_PATH"))
    start_date = datetime.date.today() - datetime.timedelta(days=5)
    with shelve.open(SHELF_PATH) as shelf:
        main(shelf, client, start_date)
