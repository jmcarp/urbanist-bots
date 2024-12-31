import datetime
import io
import os
import pathlib
import sqlite3
from typing import Dict, List, Optional, Tuple

import atproto
import geopandas as gpd
import httpx
import humanize
import lxml.html
import shapely
from dotenv import load_dotenv
from PIL import Image

SALES_URL = "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_2/MapServer/3/query"
DETAILS_URL = "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_1/MapServer/72/query"
REAL_ESTATE_URL = "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_2/MapServer/17/query"
IMAGE_URL = "https://gisweb.charlottesville.org/GisViewer/ParcelViewer/Details"

BASE_PATH = pathlib.Path(__file__).parent.absolute()
SQLITE_PATH = BASE_PATH.joinpath("everylot.db")
GIS_IMAGE_PATH = BASE_PATH.joinpath("images")


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
        # Note: consider any overlap as contributing, since a contributing
        # structure may be a small subset of a parcel. We could look up
        # structures per parcel if this turns out to cause problems.
        by_overlap = gdf[gdf.overlap > 0].sort_values(by="overlap", ascending=False)
        return not by_overlap.empty

    def is_protected(self, shape):
        gdf = self.protected_property_df.copy()
        gdf["overlap"] = gdf.intersection(shape).area / shape.area
        # Note: IPP shapes seem to be the same as parcel shapes, so we can
        # require substantial overlap with the overlay to consider a parcel as
        # IPP.
        by_overlap = gdf[gdf.overlap > 0.5].sort_values(by="overlap", ascending=False)
        return not by_overlap.empty


def main(
    conn: sqlite3.Connection,
    client: atproto.Client,
    overlay_classifier: OverlayClassifier,
) -> None:
    parcel = next_parcel(conn)
    parcel_number = parcel["ParcelNumber"]

    status, address = get_status(parcel, overlay_classifier)

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

    client.send_images(
        text=status,
        images=images,
        image_alts=image_alts,
    )

    conn.execute(
        "update parcels set posted = true where ParcelNumber = ?",
        (parcel_number,),
    )
    conn.commit()


def next_parcel(conn: sqlite3.Connection) -> Dict:
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        "select * from parcels where not posted order by parcelnumber limit 1"
    )
    return cursor.fetchone()


def get_status(parcel: Dict, overlay_classifier: OverlayClassifier) -> Tuple[str, str]:
    parcel_number = parcel["ParcelNumber"]

    detailses = get_details(parcel_number)
    assert len(detailses) == 1, f"Expected 1 detail record; got {len(detailses)}"

    details = detailses[0]
    properties = details["properties"]
    shape = shapely.geometry.shape(details["geometry"])

    address = f"{properties['StreetNumber']} {properties['StreetName']}"
    if properties["Unit"]:
        address = f"{address} Unit {properties['Unit']}"

    status = f"{address}."

    facts = [f"Zoned {properties['Zoning']}"]
    acres = parcel["Acreage"]
    if acres:
        facts.append(f"{acres} acres")
    square_feet = get_square_feet(parcel_number)
    if square_feet:
        square_feet_pretty = humanize.intcomma(square_feet)
        facts.append(f"{square_feet_pretty} square feet")
    assessment = humanize.intcomma(properties["Assessment"])
    facts.append(f"assessed at ${assessment}")
    status = f"{status} {', '.join(facts)}."

    previous_sale, previous_parcel_count = get_previous_sale(parcel_number)
    if previous_sale is not None:
        previous_sale_pretty = format_previous_sale(
            properties, previous_sale, previous_parcel_count
        )
        status = f"{status} {previous_sale_pretty}"

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


def get_previous_sale(
    parcel_number: str,
) -> Tuple[Optional[Dict], int]:
    params = {
        "where": " AND ".join(
            [
                f"ParcelNumber = '{parcel_number}'",
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


def format_previous_sale(properties: Dict, sale: Dict, parcel_count: int) -> str:
    sold_detail = (
        f"sold to {properties['OwnerName']}"
        if is_probable_business(properties["OwnerName"])
        else "sold"
    )
    sale_date = datetime.datetime.fromtimestamp(sale["SaleDate"] / 1000)
    sale_amount = humanize.intcomma(sale["SaleAmount"])
    out = f"Last {sold_detail} in {sale_date.year} for ${sale_amount}"
    if parcel_count > 1:
        out += f" ({parcel_count} parcels)"
    return out + "."


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


MAX_IMAGE_SIZE_BYTES = 2**20


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

    conn = sqlite3.connect(SQLITE_PATH)

    bsky_client = atproto.Client()
    bsky_client.login("everylot.cvilledata.org", os.getenv("BLUESKY_PASSWORD"))

    overlay_classifier = OverlayClassifier()

    main(conn, bsky_client, overlay_classifier)
