#!/usr/bin/env python

import collections
import io
import logging
import math
import os
import pathlib
import re
import tempfile
from datetime import datetime, timedelta
from typing import List

import dotenv
import geopandas as gpd
import googlemaps
import humanize
import lxml.html
import numpy as np
import pandas as pd
import pytz
import requests
import tweepy
from fuzzywuzzy import fuzz

GIS_URL = "https://gisweb.albemarle.org/gisdata"
PARCELS_URL = f"{GIS_URL}/CAMA/GIS_View_Redacted_ParcelInfo_TXT.zip"
TRANSACTIONS_URL = f"{GIS_URL}/CAMA/GIS_View_Redacted_VisionSales_TXT.zip"
SHAPEFILE_URL = f"{GIS_URL}/Parcels/shape/parcels_shape_current.zip"
ZONING_URL = f"{GIS_URL}/Zoning/ZONING.zip"

PANEL_URL = "https://gisweb.albemarle.org/gpv_51/Services/SelectionPanel.ashx"

TIMEZONE = pytz.timezone("US/Eastern")

JOIN_COLUMNS = [
    "mapblolot",
    "currowner",
    "saledate1",
    "saleprice",
    "deedbook",
    "deedpage",
    "validitycode",
]

BASE_PATH = pathlib.Path(__file__).parent.absolute()
POSTS_PATH = BASE_PATH.joinpath("posts.csv")

dotenv.load_dotenv()

TWITTER_CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]
TWITTER_ACCESS_TOKEN = os.environ["TWITTER_ACCESS_TOKEN"]
TWITTER_ACCESS_TOKEN_SECRET = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]
GOOGLEMAPS_ACCESS_KEY = os.environ["GOOGLEMAPS_ACCESS_KEY"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_panel(tab, id):
    response = requests.post(
        PANEL_URL,
        data={
            "m": "GetDataListHtml",
            "datatab": tab,
            "id": id,
        },
    )
    response.raise_for_status()
    return response.content


def get_parcel_photos(id):
    content = get_panel("ParcelPhoto", id)
    doc = lxml.html.fromstring(content)
    photos = doc.xpath(
        "//div[@class='RowSetHeader'][text()='Parcel Photos']/..//a/@href"
    )
    sketches = doc.xpath(
        "//div[@class='RowSetHeader'][text()='Parcel Sketches']/..//a/@href"
    )
    scans = doc.xpath("//div[@class='RowSetHeader'][text()='Parcel Scans']/..//a/@href")
    return {
        "photos": photos,
        "sketches": sketches,
        "scans": scans,
    }


def ensure_posts_df(transactions_df):
    transaction_types = transactions_df.dtypes.to_dict()
    try:
        return pd.read_csv(POSTS_PATH, parse_dates=["saledate1"]).astype(
            transaction_types
        )
    except FileNotFoundError:
        return pd.DataFrame(
            data=None,
            columns=list(transactions_df.columns) + ["postid"],
        ).astype(transaction_types)


def create_post(
    twitter_client, transaction, media_ids, group_index, group_count, last_tweet_id=None
):
    sale_date = transaction.saledate1.date()
    sale_amount = humanize.intcomma(round(transaction.saleprice))
    assessment = humanize.intcomma(round(transaction.TotalValue))
    address = (
        f"{transaction.PropStreet}, {transaction.City}"
        if not pd.isnull(transaction.PropStreet)
        else "no address"
    )
    sold_detail = (
        f"sold to {transaction.currowner}"
        if isinstance(transaction.currowner, str)
        and is_probable_business(transaction.currowner)
        else "sold"
    )
    zoning = (
        "Zoned {transaction.Zoning}"
        if not np.isnull(transaction.Zoning)
        else "Zoning unknown"
    )
    message = f"Parcel {transaction.PIN_SHORT_x}, {address}, {sold_detail} on {sale_date} for ${sale_amount}. {zoning}, assessed at ${assessment}, {transaction.LotSize} acres."
    if group_count > 1:
        message = f"{message} Parcel {group_index} of {group_count}."
    response = twitter_client.create_tweet(
        text=message, media_ids=media_ids, in_reply_to_tweet_id=last_tweet_id
    )
    return response.data["id"]


def tokenize(value: str) -> List[str]:
    return re.sub(r"\W", " ", value).split()


def is_probable_business(owner: str) -> bool:
    return any(
        token in tokenize(owner)
        for token in [
            "LLC",
            "INC",
            "INCORPORATED",
            "CORP",
            "CORPORATION",
            "COMPANY",
            "FOUNDATION",
        ]
    )


def append_post(posts_df, transaction, postid):
    post = [getattr(transaction, column) for column in JOIN_COLUMNS] + [postid]
    posts_df.loc[len(posts_df)] = post
    with tempfile.TemporaryDirectory() as temp:
        temp_path = pathlib.Path(temp).joinpath("posts.csv")
        posts_df.to_csv(temp_path, index=False)
        temp_path.rename(POSTS_PATH)


def get_map(client, shape):
    if shape.type == "Polygon":
        polygons = [shape.exterior.coords]
    elif shape.type == "MultiPolygon":
        polygons = [geom.exterior.coords for geom in shape.geoms]
    else:
        raise RuntimeError(f"Got unexpected shape type {shape.type}")

    paths = [
        googlemaps.maps.StaticMapPath(
            points=[{"lat": lat, "lng": lng} for lng, lat in polygon]
        )
        for polygon in polygons
    ]

    bounds = scale_bounds(shape.bounds, 3)
    zoom = calculate_zoom(bounds, [1000, 1000])
    resp = client.static_map(
        size=1000,
        center={"lat": shape.centroid.y, "lng": shape.centroid.x},
        path=paths,
        maptype="roadmap",
        zoom=zoom,
    )

    im = io.BytesIO()
    for chunk in resp:
        if chunk:
            im.write(chunk)
    im.seek(0)
    return im


def scale_bounds(bounds, scale_factor):
    center = [
        (bounds[2] + bounds[0]) / 2,
        (bounds[3] + bounds[1]) / 2,
    ]
    dimensions = [
        bounds[2] - bounds[0],
        bounds[3] - bounds[1],
    ]
    return [
        center[0] - dimensions[0] * scale_factor / 2,
        center[1] - dimensions[1] * scale_factor / 2,
        center[0] + dimensions[0] * scale_factor / 2,
        center[1] + dimensions[1] * scale_factor / 2,
    ]


def calculate_zoom(bounds, mapDim):
    """Adapted from https://stackoverflow.com/a/13274361."""
    WORLD_DIM = {"height": 256, "width": 256}
    ZOOM_MAX = 20

    def latRad(lat):
        sin = math.sin(lat * math.pi / 180)
        radX2 = math.log((1 + sin) / (1 - sin)) / 2
        return max(min(radX2, math.pi), -math.pi) / 2

    def zoom(mapPx, worldPx, fraction):
        return math.floor(math.log(mapPx / worldPx / fraction) / math.log(2))

    latFraction = (latRad(bounds[3]) - latRad(bounds[1])) / math.pi

    lngDiff = bounds[2] - bounds[0]
    lngFraction = (lngDiff + 360 if lngDiff < 0 else lngDiff) / 360

    latZoom = zoom(mapDim[1], WORLD_DIM["height"], latFraction)
    lngZoom = zoom(mapDim[0], WORLD_DIM["width"], lngFraction)

    return min(latZoom, lngZoom, ZOOM_MAX)


def urls_to_media_id(api, urls):
    """Upload the image from the first working URL to Twitter.

    Note: we check multiple URLs to handle the occasional dead link.
    """
    for url in urls:
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning(f"Got unexpected status {response.status_code}")
            continue
        upload = api.media_upload(filename=url, file=io.BytesIO(response.content))
        return upload.media_id
    return None


def main():
    logger.info("Loading GIS data")
    parcels_df = pd.read_csv(PARCELS_URL)
    parcels_df.GPIN = parcels_df.GPIN.apply(
        lambda gpin: str(int(gpin)) if not np.isnan(gpin) else gpin
    )

    transactions_df = pd.read_csv(TRANSACTIONS_URL, parse_dates=["saledate1"])
    shapefile_df = gpd.read_file(SHAPEFILE_URL).to_crs("EPSG:4326")
    zoning_df = gpd.read_file(ZONING_URL)

    posts_df = ensure_posts_df(transactions_df)

    twitter_client = tweepy.Client(
        consumer_key=TWITTER_CONSUMER_KEY,
        consumer_secret=TWITTER_CONSUMER_SECRET,
        access_token=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    )
    twitter_auth = tweepy.OAuth1UserHandler(
        consumer_key=TWITTER_CONSUMER_KEY,
        consumer_secret=TWITTER_CONSUMER_SECRET,
        access_token=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    )
    twitter_api = tweepy.API(twitter_auth)
    maps_client = googlemaps.Client(GOOGLEMAPS_ACCESS_KEY)

    max_date = datetime.now(TIMEZONE).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
    )
    min_date = max_date - timedelta(days=45)

    to_post = transactions_df[
        (transactions_df.saleprice > 0)
        & (transactions_df.saledate1 >= min_date)
        & (transactions_df.saledate1 < max_date)
    ].sort_values(["saledate1"])

    to_post = (
        to_post.merge(parcels_df, left_on="mapblolot", right_on="ParcelID", how="left")
        .merge(shapefile_df, on="GPIN", how="left")
        .merge(zoning_df.drop(["geometry"], axis=1), on="GPIN", how="left")
    )

    # Group transactions by price, date, and owner, allowing fuzzy matches on owner. This
    # handles occasional typos and inconsistencies for owner names. The county also
    # provides an "instrument number" via the web interface, but grouping by instrument
    # number yields the occasional false negative or positive; use this simple heuristic
    # instead.
    post_groups = collections.defaultdict(list)
    for row in to_post.itertuples():
        key = (row.saleprice, row.saledate1, row.currowner)
        if key not in post_groups:
            similar_keys = [
                key
                for key in post_groups
                if key[0] == row.saleprice
                and key[1] == row.saledate1
                and fuzz.ratio(key[2], row.currowner) >= 95
            ]
            if len(similar_keys) == 1:
                post_groups[similar_keys[0]].append(row)
                continue
            if len(similar_keys) > 1:
                logger.warning(f"Got multiple potential matches for row {row}")
        post_groups[key].append(row)

    for group in post_groups.values():
        group_count = len(group)
        last_tweet_id = None

        for index, transaction in enumerate(group):
            logger.info(f"Processing parcel {transaction.mapblolot}")
            # Update last tweet id and skip if previously posted
            prev_posts = posts_df[posts_df.mapblolot == transaction.mapblolot]
            if len(prev_posts):
                logger.info("Skipping already-processed parcel")
                last_tweet_id = prev_posts.iloc[0].postid
                continue

            media_ids = []
            if transaction.geometry:
                maps_image = get_map(maps_client, transaction.geometry)
                maps_upload = twitter_api.media_upload(
                    filename="map.png", file=maps_image
                )
                media_ids.append(maps_upload.media_id)
            photos = get_parcel_photos(transaction.mapblolot)
            if photos["photos"]:
                media_ids.append(urls_to_media_id(twitter_api, photos["photos"]))
            if photos["sketches"]:
                media_ids.append(urls_to_media_id(twitter_api, photos["sketches"]))
            if photos["scans"]:
                media_ids.append(urls_to_media_id(twitter_api, photos["scans"]))
            media_ids = [media_id for media_id in media_ids if media_id is not None]

            last_tweet_id = create_post(
                twitter_client,
                transaction,
                media_ids,
                group_index=index + 1,
                group_count=group_count,
                last_tweet_id=last_tweet_id,
            )

            append_post(posts_df, transaction, last_tweet_id)


if __name__ == "__main__":
    main()
