#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from enum import Enum


class AffirmCountry(str, Enum):
    US = "US"
    CA = "CA"
    AU = "AU"


class AffirmMerchantType(str, Enum):
    E_COMMERCE = "e-commerce"
    IN_STORE = "in-store"


class AffirmSettlementReportType(str, Enum):
    SUMMARY = "summary"
    DETAILS = "details"


def get_affirm_settlement_base_url(affirm_country: AffirmCountry) -> str:
    base_urls = {
        AffirmCountry.US: "https://api.affirm.com/api/v1",
        AffirmCountry.CA: "https://api.affirm.ca/api/v1",
        AffirmCountry.AU: "https://au.affirm.com/api/v1"
    }
    return base_urls[affirm_country]
