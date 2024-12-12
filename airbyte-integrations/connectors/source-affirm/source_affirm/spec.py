#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from pydantic import BaseModel, Field


class AffirmSettlementReportsConfig(BaseModel):
    class Config:
        title = "Affirm Settlement Reports Spec"
        schema_extra = {"additionalProperties": True}

    user: str = Field(
        description="User for API authenticator",
        title="USER",
        airbyte_secret=True
    )

    password: str = Field(
        description="Password for API authenticator",
        title="PASSWORD",
        airbyte_secret=True
    )

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
        description="Not used in current version. Reserved for future enhancements. Will be used for stream slicing for initial full_refresh sync when no updated state is present for reports that support sliced incremental sync.",
        examples=["30", "365"],
    )

    affirm_country: str = Field(
        description="Select country of settlement reports.", title="Country", examples=["US", "CA"]
    )
    merchant_type: str = Field(
        description="Select e-commerce or in-store", title="Merchant Type", examples=["e-commerce", "in-store"]
    )
    merchant_id: str = Field(
        description="Merchant ID", title="Merchant ID", airbyte_secret=True
    )
    summary_lookback_window_days: int = Field(
        description="Lookback window for summary stream. Data from the lookback window will be replicated everytime the sync runs",
        title="Lookback Window in Days for Summary",
        default=30
    )
    api_page_limit: int = Field(
        description="page limit for API calls",
        title="Page Limit",
        default=200
    )