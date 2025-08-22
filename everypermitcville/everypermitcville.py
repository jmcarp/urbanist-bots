#!/usr/bin/env python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "atproto",
#     "httpx",
#     "lxml",
#     "python-dotenv",
# ]
# ///

"""
Collect Charlottesville city building permits and post summaries to bluesky.

TODO: ask city staff to drop the US-only allowlist so that we don't have to use
a proxy to access the portal.

TODO: ask city staff to send permit data to the city data portal. This might
allow us to replace some or all of the current html parsing logic, although the
deprecated data portal resource is missing a number of interesting fields:
https://opendata.charlottesville.org/datasets/a05d31b96c26406788942aabd7b7e581_33/explore.
"""

import datetime
import logging
import os
import pathlib
import random
import shelve
from dataclasses import dataclass
from typing import Dict, List, Tuple

import atproto
import dotenv
import httpx
import lxml.html

LOGIN_URL = "https://permits.charlottesville.gov/portal"
SEARCH_URL = "https://permits.charlottesville.gov/portal/SearchByNumber/Search"
PERMIT_URL = "https://permits.charlottesville.gov/portal/PermitInfo/Index"

# The permit portal requires a user-agent to be set. It seems to accept any
# value.
HEADERS = {"User-Agent": "everypermitcville.bsky.social"}

BASE_PATH = pathlib.Path(__file__).parent.absolute()
SHELF_PATH = BASE_PATH.joinpath("shelf.db")

LOOKBACK_DAYS = 30
MAX_POST_LENGTH = 300
MAX_MAX_DETAILS = 5
MAX_DETAILS_LENGTH = 50

BLUESKY_USERNAME = "everypermit.cvilledata.org"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Post:
    permit_id: str
    project_number: str


def list_proxies() -> List[Dict]:
    resp = httpx.get("https://free-proxy-list.net/")
    resp.raise_for_status()
    doc = lxml.html.fromstring(resp.content)

    rows = doc.xpath("//*[@id='list']//tr")
    headers = rows[0].xpath("./th/text()")
    proxies = [dict(zip(headers, row.xpath("./td/text()"))) for row in rows[1:]]

    return [proxy for proxy in proxies if proxy["Code"] in {"US", "CA"}]


def choose_proxy(proxies: List[Dict], check_func) -> str:
    """Choose a working proxy.

    Free proxy services are unreliable, so check them using a user-supplied
    function. Return the first proxy that passes the check.
    """
    for proxy in proxies:
        proxy_addr = f"http://{proxy['IP Address']}:{proxy['Port']}"
        logger.info(f"Checking proxy {proxy_addr}")
        try:
            check_func(httpx.Client(proxy=proxy_addr, timeout=30))
            return proxy_addr
        except:
            logger.info("\tProxy failed")


def login(client: httpx.Client, username: str, password: str) -> None:
    resp = client.post(
        LOGIN_URL,
        headers=HEADERS,
        data={
            "LoginName": username,
            "Password": password,
        },
    )
    assert resp.status_code == 302, (
        f"Got unexpected status code {resp.status_code} from login"
    )


def get_permits(
    client: httpx.Client, start_date: datetime.date, end_date: datetime.date
) -> List[Dict]:
    resp = client.get(
        SEARCH_URL,
        params={
            "keyword": "",
            "fromDateInput": start_date.strftime("%m-%d-%Y"),
            "toDateInput": end_date.strftime("%m-%d-%Y"),
        },
        headers=HEADERS,
    )
    resp.raise_for_status()
    doc = lxml.html.fromstring(resp.content)
    headings = [
        each.text_content().strip()
        for each in doc.xpath("//table[@id='search-table']/thead/tr/th")
    ]
    rows = doc.xpath("//table[@id='search-table']/tbody/tr")
    permits = []
    for row in rows:
        values = [each.text_content().strip() for each in row.xpath("./td")]
        permits.append(dict(zip(headings, values)))
    return permits


def get_permit(client: httpx.Client, permit_id: str) -> Tuple[str, dict, dict]:
    resp = client.get(PERMIT_URL, params={"caObjectId": permit_id}, headers=HEADERS)
    resp.raise_for_status()
    doc = lxml.html.fromstring(resp.content)
    doc.make_links_absolute(PERMIT_URL)  # type: ignore

    info_rows = doc.xpath(
        "//h5[contains(text(), 'Permit/License Info')]/parent::div//p[@class='font-13']"
    )
    info = {}
    for row in info_rows:
        parts = row.text_content().split(":", 1)
        if len(parts) == 1:
            parts.append("")
        if len(parts) > 2:
            parts = parts[:2]
        parts = [part.strip() for part in parts]
        info[parts[0]] = parts[1]

    details = {}
    detail_table = doc.xpath(
        "//h5[contains(text(), 'Permit/License Details')]/parent::div//table"
    )
    if len(detail_table) > 0:
        detail_headings = detail_table[0].xpath("./thead/tr/th/text()")
        detail_rows = detail_table[0].xpath("./tbody/tr")
        for row in detail_rows:
            detail = dict(
                zip(
                    detail_headings, [each.strip() for each in row.xpath("./td/text()")]
                )
            )
            details[detail["Description"]] = detail["Data"]

    return str(resp.url), info, details


def main(http_client: httpx.Client, bsky_client: atproto.Client, shelf: shelve.Shelf):
    end_date = datetime.date.today()
    permits = get_permits(
        http_client, end_date - datetime.timedelta(days=LOOKBACK_DAYS), end_date
    )
    for permit in permits:
        permit_id = str(int(float(permit["Id"])))
        project_number = permit["Project Number"]
        post = Post(permit_id, project_number)
        logger.info("Processing permit %s::%s", permit_id, project_number)
        if permit_id in shelf:
            logger.info("Skipping already-processed permit")
            continue
        permit_url, permit_info, permit_details = get_permit(http_client, permit_id)

        # Build a message less than or equal to the maximum post length.
        # Include up to five details if space allows; else decrement the number
        # of details until the post is short enough.
        max_details = MAX_MAX_DETAILS
        while True:
            message = format_message(
                permit, permit_info, permit_details, permit_url, max_details
            )
            if len(message) <= MAX_POST_LENGTH:
                break
            assert max_details > 0, f"Message for {permit_id} too long"
            max_details -= 1

        bsky_client.send_post(
            text=message,
            facets=[
                atproto.models.app.bsky.richtext.facet.Main(
                    features=[atproto.models.AppBskyRichtextFacet.Link(uri=permit_url)],
                    index=atproto.models.AppBskyRichtextFacet.ByteSlice(
                        byte_start=message.index(permit_url),
                        byte_end=message.index(permit_url) + len(permit_url),
                    ),
                )
            ],
        )
        shelf[permit_id] = post
        shelf.sync()


def format_message(
    permit: dict,
    permit_info: dict,
    permit_details: dict,
    permit_url: str,
    max_details: int,
) -> str:
    project_type = permit["Type"]
    if permit["Sub-Type"] != project_type:
        project_type = f"{project_type}/{permit['Sub-Type']}"
    project_number = permit["Project Number"]
    address = permit["Site Address"]

    message_parts = [
        f"{project_number}: {project_type} @ {address}",
    ]

    if max_details > 0:
        detail_keys = list(permit_details.keys())[:max_details]
        detail_items = []
        for key in detail_keys:
            detail_value = permit_details[key]
            if len(detail_value) > MAX_DETAILS_LENGTH:
                detail_value = detail_value[:MAX_DETAILS_LENGTH] + "..."
            detail_items.append(f"{key}: {detail_value}")
        if len(permit_details) > len(detail_keys):
            detail_items.append("...")
        detail_message = "\n".join(detail_items)
        message_parts.append(detail_message)

    message_parts.append(permit_url)

    return "\n\n".join(message_parts)


if __name__ == "__main__":
    dotenv.load_dotenv()

    def check_proxy(client: httpx.Client) -> None:
        login(client, os.getenv("PERMIT_USERNAME"), os.getenv("PERMIT_PASSWORD"))

    # The permit portal only accepts requests from US addresses, and appears to
    # block VPS services like the one that hosts this application. If
    # requested, route requests through a US-based proxy.
    if os.getenv("USE_PROXY", "") != "":
        proxy_addr = choose_proxy(list_proxies(), check_proxy)
        http_client = httpx.Client(timeout=60, proxy=proxy_addr)
    else:
        http_client = httpx.Client(timeout=60)

    login(http_client, os.getenv("PERMIT_USERNAME"), os.getenv("PERMIT_PASSWORD"))

    bsky_client = atproto.Client()
    bsky_client.login(BLUESKY_USERNAME, os.getenv("BLUESKY_PASSWORD"))

    with shelve.open(str(SHELF_PATH)) as shelf:
        main(http_client, bsky_client, shelf)
