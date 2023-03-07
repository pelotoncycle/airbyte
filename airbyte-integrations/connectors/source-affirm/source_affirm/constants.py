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


def get_affirm_settlement_base_url(affirm_country: AffirmCountry, report_type: AffirmSettlementReportType) -> str:
    base_urls = {
        AffirmCountry.US: {
            AffirmSettlementReportType.DETAILS: "https://api.affirm.com/api/v1/settlements/events",
            AffirmSettlementReportType.SUMMARY: "https://api.affirm.com/api/v1/settlements/daily",
        },
        AffirmCountry.CA: {
            AffirmSettlementReportType.DETAILS: "https://api.affirm.ca/api/v1/settlements/events",
            AffirmSettlementReportType.SUMMARY: "https://api.affirm.ca/api/v1/settlements/daily",
        },
        AffirmCountry.AU: {
            AffirmSettlementReportType.DETAILS: "https://au.affirm.com/api/v1/settlements/events",
            AffirmSettlementReportType.SUMMARY: "https://au.affirm.com/api/v1/settlements/daily"
        }
    }
    return base_urls[affirm_country][report_type]
