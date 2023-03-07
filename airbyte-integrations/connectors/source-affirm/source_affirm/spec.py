#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.models import AdvancedAuth, AuthFlowType, OAuthConfigSpecification
from pydantic import BaseModel, Field
from .constants import AffirmCountry, AffirmMerchantType, AffirmSettlementReportType


class AffirmSettlementReportsConfig(BaseModel):
    class Config:
        title = "Affirm Settlement Reports Spec"
        schema_extra = {"additionalProperties": True}

    auth_type: str = Field(default="oauth2.0", const=True, order=1)

    start_date: str = Field(
        description="date in the format 2015-01-01. Any data before this date will not be replicated.",
        title="Start Date",
        pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
        examples=["2015-01-01"],
    )

    end_date: str = Field(
        None,
        description="date in the format 2015-01-01. Any data after this date will not be replicated.",
        title="End Date",
        pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}$|^$",
        examples=["2015-01-01"],
    )

    period_in_days: int = Field(
        30,
        description="Will be used for stream slicing for initial full_refresh sync when no updated state is present for reports that support sliced incremental sync.",
        examples=["30", "365"],
    )

    affirm_country: AffirmCountry = Field(
        description="Select country of settlement reports.", title="Country", examples=["US", "CA", "AU"]
    )
    merchant_type: AffirmMerchantType = Field(
        description="Select e-commerce or in-store", title="Merchant Type", examples=["e-commerce", "in-store"]
    )
    report_type: AffirmSettlementReportType = Field(
        description="Select details or summary", title="Report Type", examples=["details", "summary"]
    )
    lookback_window_days: int = Field(
        description="When set, the connector will always re-export data from the past N days",
        title="Lookback Window in Days",
        default=3
    )

