from typing import Mapping, Any, Optional, Iterable, MutableMapping, List, Tuple, Callable
import urllib.parse

import requests
from airbyte_cdk.sources.streams.http.http import HttpStream
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.core import Stream, StreamData
from datetime import datetime, timedelta

from source_affirm.constants import AffirmCountry


logger = AirbyteLogger()


class SourceAffirmStream(HttpStream):
    primary_key = 'id'
    cursor_field = "date"

    def __init__(self, country, merchant_id, page_limit, start_date, end_date, **kwargs):
        self.affirm_country: str = country
        self.merchant_id: str = merchant_id
        self.api_page_limit: int = page_limit
        self.start_date: str = start_date
        self.end_date: str = end_date
        self._cursor_value: datetime = None
        super(SourceAffirmStream, self).__init__(**kwargs)

    @property
    def state(self) -> Mapping[str, str]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
        else:
            return {self.cursor_field: self.start_date}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        # _cursor_value is a datetime object
        self._cursor_value = datetime.strptime(value[self.cursor_field], '%Y-%m-%d')

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, str]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date <= datetime.now():
            dates.append({self.cursor_field: start_date.strftime('%Y-%m-%d')})
            start_date += timedelta(days=1)
        return dates

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, str] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        if stream_state and self.cursor_field in stream_state:
            start_date_str = stream_state.get(self.cursor_field)
        else:
            start_date_str = self.start_date
        start_date_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
        return self._chunk_date_range(start_date_dt)

    @property
    def url_base(self) -> str:
        base_urls = {
            AffirmCountry.US: "https://api.affirm.com/api/v1/",
            AffirmCountry.CA: "https://api.affirm.ca/api/v1/",
            AffirmCountry.AU: "https://au.affirm.com/api/v1/"
        }
        return base_urls[self.affirm_country]

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        response_json = response.json()
        try:
            yield from response_json["data"]
        except KeyError:
            logger.warn("No entries found in requested report. Setting it to null.")
            yield from []

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            return next_page_token
        if stream_slice:
            return {
                'merchant_id': self.merchant_id,
                'before': stream_slice.get(self.cursor_field),
                'after': stream_slice.get(self.cursor_field),
                'limit': self.api_page_limit
            }
        return {
            'merchant_id': self.merchant_id,
            'before': self.end_date,
            'after': self.start_date,
            'limit': self.api_page_limit
        }

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"accept": "*/*"}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        next_page_url = response_json.get("next_page", "")
        if not next_page_url:
            return None
        next_query_string = urllib.parse.urlsplit(next_page_url).query
        next_page_params = dict(urllib.parse.parse_qsl(next_query_string))
        return next_page_params


class AffirmSettlementEventsStream(SourceAffirmStream):
    name = 'events'

    def path(
        self,
        *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return 'settlements/events'


class AffirmSettlementSummaryStream(SourceAffirmStream):
    name = 'summary'

    def path(
        self,
        *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return 'settlements/daily'
