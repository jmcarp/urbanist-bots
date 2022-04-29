#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import datetime
import io
import logging
import os
import pathlib
import shelve
from typing import Dict, List, Optional

import humanize
import lxml.html
import requests
import tweepy
import twitter_bot_utils
from PIL import Image

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SALES_URL = "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_2/MapServer/3/query"
DETAILS_URL = "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_1/MapServer/72/query"
REAL_ESTATE_URL = "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_2/MapServer/17/query"
IMAGE_URL = "https://gisweb.charlottesville.org/GisViewer/ParcelViewer/Details"

BASE_PATH = pathlib.Path(__file__).parent.absolute()
SHELF_PATH = BASE_PATH.joinpath("shelf.db")
GIS_IMAGE_PATH = BASE_PATH.joinpath("images")


def main(shelf, client, start_date):
    sales = get_sales(start_date)

    # Group sales by book page, then post each group as a thread
    sale_groups = collections.defaultdict(list)
    for sale in sales:
        sale_groups[sale["BookPage"]].append(sale)

    for sale_group in sale_groups.values():
        last_tweet = None
        group_count = len(sale_group)
        sorted_sales = sorted(sale_group, key=lambda sale: sale["ParcelNumber"])

        for index, sale in enumerate(sorted_sales):
            parcel_number = sale["ParcelNumber"]
            logger.info("Processing parcel number %s", parcel_number)
            if parcel_number in shelf:
                logger.info("Skipping already-processed parcel")
                last_tweet = shelf[parcel_number]
                continue
            if sale["SaleAmount"] == 0:
                logger.info("Skipping parcel with missing sale price")
                continue

            details = get_details(parcel_number)

            # Get price per square foot for single-property transactions. Otherwise skip,
            # since showing price per square foot over multiple properties could be
            # confusing.
            price_per_square_foot = None
            if group_count == 1:
                square_feet = get_square_feet(parcel_number)
                if square_feet:
                    price_per_square_foot = humanize.intcomma(
                        round(sale["SaleAmount"] / square_feet)
                    )

            address = f"{details['StreetNumber']} {details['StreetName']}"
            if details["Unit"]:
                address = f"{address} Unit {details['Unit']}"
            sale_date = datetime.datetime.fromtimestamp(sale["SaleDate"] / 1000).date()
            sale_amount = humanize.intcomma(sale["SaleAmount"])
            assessment = humanize.intcomma(details["Assessment"])
            sold_detail = (
                f"sold to {details['OwnerName']}"
                if is_probable_business(details["OwnerName"])
                else "sold"
            )
            status = f"{address}, {sold_detail} on {sale_date} for ${sale_amount}. Zoned {details['Zoning']}, assessed at ${assessment}."
            if price_per_square_foot:
                status = f"{status} ${price_per_square_foot} per square foot."
            if group_count > 1:
                status = f"{status} Parcel {index + 1} of {group_count}."
            else:
                previous_sale = get_previous_sale(parcel_number, sale_date)
                if previous_sale is not None:
                    status = f"{status} {format_previous_sale(previous_sale)}"

            media_ids = []
            photo_image = get_image(parcel_number)
            if photo_image:
                photo_upload = client.media_upload(
                    filename=f"{parcel_number}.jpg", file=photo_image
                )
                media_ids.append(photo_upload.media_id)
            gis_image = GIS_IMAGE_PATH.joinpath(f"{parcel_number}.jpg")
            if gis_image.exists():
                gis_upload = client.media_upload(str(gis_image))
                media_ids.append(gis_upload.media_id)

            status = client.update_status(
                status=status,
                in_reply_to_status_id=last_tweet,
                auto_populate_reply_metadata=True,
                media_ids=media_ids,
            )
            last_tweet = status.id
            shelf[parcel_number] = status.id
            shelf.sync()


def get_sales(start_date: Optional[datetime.date] = None) -> List[Dict]:
    start_date = start_date or datetime.date.today() - datetime.timedelta(days=1)
    start_query = start_date.strftime("%Y-%m-%d %H:%M:%S")
    params = {
        "where": f"SaleDate >= TIMESTAMP '{start_query}'",
        "outFields": "*",
        "f": "json",
    }
    response = requests.post(SALES_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return [each["attributes"] for each in data["features"]]


def get_previous_sale(
    parcel_number: str, sale_date: datetime.datetime
) -> Optional[Dict]:
    date_query = sale_date.strftime("%Y-%m-%d %H:%M:%S")
    params = {
        "where": " AND ".join(
            [
                f"ParcelNumber = '{parcel_number}'",
                f"SaleDate < TIMESTAMP '{date_query}'",
                "SaleAmount > 0",
            ]
        ),
        "orderByFields": "SaleDate desc",
        "outFields": "*",
        "f": "json",
    }
    response = requests.post(SALES_URL, params=params)
    response.raise_for_status()
    data = response.json()
    if len(data["features"]) > 0:
        return data["features"][0]["attributes"]
    else:
        return None


def format_previous_sale(sale: Dict) -> str:
    sale_date = datetime.datetime.fromtimestamp(sale["SaleDate"] / 1000)
    sale_amount = humanize.intcomma(sale["SaleAmount"])
    return f"Last sold in {sale_date.year} for ${sale_amount}."


def get_details(parcel_number: str) -> Dict:
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


def get_real_estate(parcel_number: str) -> List[Dict]:
    params = {
        "where": f"ParcelNumber = '{parcel_number}'",
        "outFields": "*",
        "f": "json",
    }
    response = requests.get(REAL_ESTATE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return [feature["attributes"] for feature in data["features"]]


def get_square_feet(parcel_number: str) -> Optional[int]:
    """Calculate total finished square feet.

    Note: some parcels have multiple real estate records with different details. To be
    safe, skip parcels with ambiguous records.
    """
    # Skip parcels that have multiple non-zero finished square foot records.
    price_per_square_foot = None
    real_estate = get_real_estate(parcel_number)
    with_square_feet = [
        record
        for record in real_estate
        if record["SquareFootageFinishedLiving"].isnumeric()
        and int(record["SquareFootageFinishedLiving"]) > 0
    ]
    if len(with_square_feet) == 1:
        square_feet = int(with_square_feet[0]["SquareFootageFinishedLiving"])
        if with_square_feet[0]["FinishedBasement"].isnumeric():
            square_feet += int(with_square_feet[0]["FinishedBasement"])
        return square_feet
    else:
        return None


def is_probable_business(owner: str) -> bool:
    return (
        owner.endswith(" LLC")
        or owner.endswith(" INC")
        or owner.endswith(" CORPORATION")
        or owner.endswith(" FOUNDATION")
    )


def get_image(parcel_number: str) -> Optional[io.BytesIO]:
    params = {
        "Key": parcel_number,
        "SearchOptionIndex": "0",
        "DetailsTabIndex": "0",
    }
    details_response = requests.get(IMAGE_URL, params=params)
    details_response.raise_for_status()
    page = lxml.html.fromstring(details_response.content)
    urls = page.xpath('//img[contains(@src, "realestate.charlottesville.org")]/@src')
    if urls:
        image_response = requests.get(urls[0])
        if image_response.status_code != 200:
            return None
        try:
            return maybe_compress_image(io.BytesIO(image_response.content))
        except ImageTooLarge:
            return None
    else:
        return None


MAX_IMAGE_SIZE_BYTES = 5242880


class ImageTooLarge(Exception):
    pass


def maybe_compress_image(
    in_buffer, min_quality: int = 10, max_size: int = MAX_IMAGE_SIZE_BYTES
):
    """Lower image quality until it's small enough for Twitter."""
    in_buffer.seek(0, os.SEEK_END)
    in_size = in_buffer.tell()
    in_buffer.seek(0)
    if in_size <= max_size:
        return in_buffer
    image = Image.open(in_buffer)
    quality = 90
    while quality >= min_quality:
        out_buffer = io.BytesIO()
        image.save(out_buffer, "JPEG", quality=quality)
        if out_buffer.tell() < max_size:
            out_buffer.seek(0)
            return out_buffer
        quality -= 10
    raise ImageTooLarge()


if __name__ == "__main__":
    client = twitter_bot_utils.API(
        screen_name="everysalecville", config_file=os.getenv("TWITTER_CONFIG_PATH")
    )
    start_date = datetime.date.today() - datetime.timedelta(days=14)
    with shelve.open(str(SHELF_PATH)) as shelf:
        main(shelf, client, start_date)
