import datetime

import pytest

from everysalecville import *


@pytest.mark.parametrize(
    ["sale", "parcel_count", "expected"],
    [
        (
            {
                "SaleDate": datetime.datetime(2024, 10, 1).timestamp() * 1000,
                "SaleAmount": 123456,
            },
            1,
            "Last sold in 2024 for $123,456.",
        ),
        (
            {
                "SaleDate": datetime.datetime(2024, 10, 1).timestamp() * 1000,
                "SaleAmount": 123456,
            },
            3,
            "Last sold in 2024 for $123,456 (3 parcels).",
        ),
    ],
)
def test_format_previous_sale(sale, parcel_count, expected):
    assert format_previous_sale(sale, parcel_count) == expected
