#!/usr/bin/env python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "atproto",
#     "geopandas",
#     "httpx",
#     "humanize",
#     "lxml",
#     "pillow",
#     "python-dotenv",
#     "shapely",
# ]
# ///

import collections
import datetime
import io
import json
import logging
import os
import pathlib
import shelve
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, TypeAlias

import atproto
import geopandas as gpd
import humanize
import lxml.html
import httpx
import shapely
from dotenv import load_dotenv
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

BLUESKY_USERNAME = "everysale.cvilledata.org"

CreateRecordResponse: TypeAlias = atproto.models.app.bsky.feed.post.CreateRecordResponse


@dataclass
class Post:
    """Model a post that describes a transaction.

    We track the progress of the scraper using a shelve.Shelf that maps parcel
    numbers and book numbers to post details. This format must tell us whether
    or not a given transaction has already been posted, and if so, the details
    of the post and thread (if part of a multi-parcel transaction).
    """

    parcel_number: str
    book_page: str
    post: CreateRecordResponse
    thread_root: Optional[CreateRecordResponse]
    thread_parent: Optional[CreateRecordResponse]


class OverlayClassifier:
    """Load overlay layers and categorize parcel shapes against them.

    To fetch layers, run `make history`.

    Note: using this class throws a `UserWarning` on calculating areas, but
    since we only care about relative areas to calculate overlap, we can ignore
    these warnings for now.
    """

    def __init__(self):
        self.adc_district_df = gpd.read_file(
            str(BASE_PATH.joinpath("adc-districts.geojson"))
        )
        self.adc_district_contributing_df = gpd.read_file(
            str(BASE_PATH.joinpath("adc-districts-contributing-structure.geojson"))
        )
        self.protected_property_df = gpd.read_file(
            str(BASE_PATH.joinpath("individually-protected-property.geojson"))
        )

    def adc_district(self, shape: shapely.Geometry) -> Optional[str]:
        gdf = self.adc_district_df.copy()
        gdf["overlap"] = gdf.intersection(shape).area / shape.area
        by_overlap = gdf[gdf.overlap > 0].sort_values(by="overlap", ascending=False)
        if not by_overlap.empty:
            return by_overlap.iloc[0].NAME
        return None

    def is_adc_contributing(self, shape) -> bool:
        gdf = self.adc_district_contributing_df.copy()
        gdf["overlap"] = gdf.intersection(shape).area / shape.area
        by_overlap = gdf[gdf.overlap > 0].sort_values(by="overlap", ascending=False)
        return not by_overlap.empty

    def is_protected(self, shape):
        gdf = self.protected_property_df.copy()
        gdf["overlap"] = gdf.intersection(shape).area / shape.area
        by_overlap = gdf[gdf.overlap > 0].sort_values(by="overlap", ascending=False)
        return not by_overlap.empty


def main(
    shelf: shelve.Shelf,
    client: atproto.Client,
    overlay_classifier: OverlayClassifier,
    start_date: datetime.date,
) -> int:
    sales = get_sales(start_date)
    post_count = 0

    # Group sales by book page, then post each group as a thread
    sale_groups = collections.defaultdict(list)
    for sale in sales:
        sale_groups[sale["BookPage"]].append(sale)

    for sale_group in sale_groups.values():
        group_count = len(sale_group)
        sorted_sales = sorted(sale_group, key=lambda sale: sale["ParcelNumber"])
        thread_root, thread_parent = None, None

        for index, sale in enumerate(sorted_sales):
            parcel_number = sale["ParcelNumber"]
            book_page = sale["BookPage"]
            key = f"{parcel_number}::{book_page}"
            logger.info("Processing parcel %s::%s", parcel_number, book_page)
            if key in shelf:
                logger.info("Skipping already-processed parcel")
                thread_parent = shelf[key].post
                thread_root = shelf[key].thread_root
                continue
            if sale["SaleAmount"] == 0:
                logger.info("Skipping parcel with missing sale price")
                continue

            try:
                status, address = get_status(
                    sale, group_count, index, overlay_classifier
                )
            except Exception as exc:
                logger.warn("Error getting status: {exc}")

            images, image_alts = [], []
            photo_image = get_gis_photo(parcel_number)
            if photo_image:
                images.append(photo_image.read())
                image_alts.append(f"Photo of {address} from GIS database.")
            # Get annotated map image from GIS if available on disk. This is a
            # gratuitous process that we could replace with the google maps
            # api, but the official GIS images look cool.
            gis_image_path = GIS_IMAGE_PATH.joinpath(f"{parcel_number}.jpg")
            if gis_image_path.exists():
                with gis_image_path.open("rb") as fp:
                    gis_image = fp.read()
                images.append(gis_image)
                image_alts.append(f"Map view of {address} from GIS database.")

            if thread_parent is not None:
                reply_to = atproto.models.AppBskyFeedPost.ReplyRef(
                    parent=atproto.models.create_strong_ref(thread_parent),
                    root=atproto.models.create_strong_ref(thread_root),
                )
            else:
                reply_to = None
            resp = client.send_images(
                text=status,
                reply_to=reply_to,
                images=images,
                image_alts=image_alts,
            )

            thread_root = thread_root or resp
            shelf[key] = Post(
                parcel_number=parcel_number,
                book_page=book_page,
                post=resp,
                thread_parent=thread_parent,
                thread_root=thread_root,
            )
            thread_parent = resp

            shelf.sync()
            post_count += 1
    return post_count


def get_status(
    sale: Dict, group_count: int, index: int, overlay_classifier: OverlayClassifier
) -> Tuple[str, str]:
    parcel_number = sale["ParcelNumber"]
    detailses = get_details(parcel_number)
    assert len(detailses) == 1, f"Expected 1 detail record; got {len(detailses)}"

    details = detailses[0]
    properties = details["properties"]
    shape = shapely.from_geojson(json.dumps(details))

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

    address = f"{properties['StreetNumber']} {properties['StreetName']}"
    if properties["Unit"]:
        address = f"{address} Unit {properties['Unit']}"
    sale_date = datetime.datetime.fromtimestamp(sale["SaleDate"] / 1000).date()
    sale_amount = humanize.intcomma(sale["SaleAmount"])
    assessment = humanize.intcomma(properties["Assessment"])
    sold_detail = (
        f"sold to {properties['OwnerName']}"
        if is_probable_business(properties["OwnerName"])
        else "sold"
    )
    status = f"{address}, {sold_detail} on {sale_date} for ${sale_amount}. Zoned {properties['Zoning']}, assessed at ${assessment}."
    if price_per_square_foot:
        status = f"{status} ${price_per_square_foot} per square foot."
    if group_count > 1:
        status = f"{status} Parcel {index + 1} of {group_count}."
    else:
        previous_sale, previous_parcel_count = get_previous_sale(
            parcel_number, sale_date
        )
        if previous_sale is not None:
            status = (
                f"{status} {format_previous_sale(previous_sale, previous_parcel_count)}"
            )

    # Describe historic districts if applicable.
    adc_district = overlay_classifier.adc_district(shape)
    if adc_district is not None:
        adc_status = adc_district.replace("ADC District", "").strip() + " ADC"
        if overlay_classifier.is_adc_contributing(shape):
            adc_status += "; contributing structure"
        status = f"{status} {adc_status}."
    if overlay_classifier.is_protected(shape):
        status = f"{status} IPP."

    return status, address


def get_sales(start_date: Optional[datetime.date] = None) -> List[Dict]:
    start_date = start_date or datetime.date.today() - datetime.timedelta(days=1)
    start_query = start_date.strftime("%Y-%m-%d %H:%M:%S")
    params = {
        "where": f"SaleDate >= TIMESTAMP '{start_query}'",
        "outFields": "*",
        "f": "json",
    }
    response = httpx.post(SALES_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return [each["attributes"] for each in data["features"]]


def get_previous_sale(
    parcel_number: str, sale_date: datetime.date
) -> Tuple[Optional[Dict], int]:
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
    response = httpx.post(SALES_URL, params=params)
    response.raise_for_status()
    data = response.json()
    if len(data["features"]) > 0:
        attributes = data["features"][0]["attributes"]
        # Skip if nil BookPage.
        if attributes["BookPage"] == "0:0":
            return None, 0
        sales_by_page = get_sales_by_page(attributes["BookPage"])
        return attributes, len(sales_by_page)
    else:
        return None, 0


def get_sales_by_page(book_page: str) -> List[Dict]:
    params = {
        "where": f"BookPage = '{book_page}'",
        "outFields": "*",
        "f": "json",
    }
    response = httpx.post(SALES_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return [feature["attributes"] for feature in data["features"]]


def format_previous_sale(sale: Dict, parcel_count: int) -> str:
    sale_date = datetime.datetime.fromtimestamp(sale["SaleDate"] / 1000)
    sale_amount = humanize.intcomma(sale["SaleAmount"])
    out = f"Last sold in {sale_date.year} for ${sale_amount}"
    if parcel_count > 1:
        out += f" ({parcel_count} parcels)"
    return out + "."


def get_details(parcel_number: str) -> List[Dict]:
    params = {
        "where": f"ParcelNumber = '{parcel_number}'",
        "outFields": "*",
        "f": "geojson",
    }
    response = httpx.get(DETAILS_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return data["features"]


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
        if record["SquareFootageFinishedLiving"]
        and record["SquareFootageFinishedLiving"].isnumeric()
        and int(record["SquareFootageFinishedLiving"]) > 0
    ]
    if len(with_square_feet) == 1:
        square_feet = int(with_square_feet[0]["SquareFootageFinishedLiving"])
        if with_square_feet[0]["FinishedBasement"].isnumeric():
            square_feet += int(with_square_feet[0]["FinishedBasement"])
        return square_feet
    else:
        return None


def get_real_estate(parcel_number: str) -> List[Dict]:
    params = {
        "where": f"ParcelNumber = '{parcel_number}'",
        "outFields": "*",
        "f": "json",
    }
    response = httpx.get(REAL_ESTATE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return [feature["attributes"] for feature in data["features"]]


def is_probable_business(owner: str) -> bool:
    """Guess whether a parcel is a business based on its owner. We don't want
    to publish individual names, even though they're a matter of public record,
    but business names are fair game.
    """
    return (
        owner.endswith(" LLC")
        or owner.endswith(" LTD")
        or owner.endswith(" INC")
        or owner.endswith(" CORPORATION")
        or owner.endswith(" FOUNDATION")
        or owner
        in {
            "CITY OF CHARLOTTESVILLE",
            "CITY OF CHARLOTTESVILLE & COUNTY OF ALBEMARLE",
            "COUNTY OF ALBEMARLE",
            "THE RECTOR & VISITORS OF THE UNIVERSITY OF VIRGINIA",
            "CHARLOTTESVILLE REDEVELOPMENT & HOUSING AUTHORITY",
            "VELIKY, LC",
        }
    )


def get_gis_photo(parcel_number: str) -> Optional[io.BytesIO]:
    """Get parcel image from GIS, compressing if necessary."""
    params = {
        "Key": parcel_number,
        "SearchOptionIndex": "0",
        "DetailsTabIndex": "0",
    }
    details_response = httpx.get(IMAGE_URL, params=params)
    details_response.raise_for_status()
    page = lxml.html.fromstring(details_response.content)
    urls = page.xpath('//img[contains(@src, "realestate.charlottesville.org")]/@src')
    if urls:
        image_response = httpx.get(urls[0])  # type: ignore
        if image_response.status_code != 200:
            return None
        try:
            return maybe_compress_image(io.BytesIO(image_response.content))
        except ImageTooLarge:
            return None
    else:
        return None


MAX_IMAGE_SIZE_BYTES = 1_000_000


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
    load_dotenv()
    bsky_client = atproto.Client()
    bsky_client.login(BLUESKY_USERNAME, os.getenv("BLUESKY_PASSWORD"))
    overlay_classifier = OverlayClassifier()
    start_date = datetime.date.today() - datetime.timedelta(days=60)

    with shelve.open(str(SHELF_PATH)) as shelf:
        post_count = main(shelf, bsky_client, overlay_classifier, start_date)

    httpx.get(os.environ["HEALTHCHECK_ENDPOINT"])
