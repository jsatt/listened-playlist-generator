import logging

import requests

logger = logging.getLogger(__name__)


class LastFmException(Exception):
    ...


class LastFmClient:
    def __init__(self, api_key, user, api_url='https://ws.audioscrobbler.com/2.0/'):
        self.api_key = api_key
        self.user = user
        self.api_url = api_url

    def request(self, method, result_key, record_key, limit=None, **params):
        logger.debug(f"Requesting '{method}': {params}")
        params = {
            **params,
            'method': method,
            'api_key': self.api_key,
            'user': self.user,
            'format': 'json',
            'limit': limit or 50,
        }
        resp = requests.get(self.api_url, params=params)
        data = resp.json()
        if resp.status_code != 200:
            raise LastFmException(data['message'])
        results = data.get(result_key, {})
        attrs = results['@attr']
        records = results[record_key]
        return records, attrs

    def request_all_results(
            self, method, result_key, record_key, log_increment=10, max_pages=None, **params):
        page = 1
        while True:
            if page == 1 or page % log_increment == 0:
                logger.info(f"Requesting all records '{method}', page: {page}")
            records, attrs = self.request(method, result_key, record_key, page=page, **params)
            for record in records:
                yield record

            if attrs['page'] == attrs['totalPages'] or not records or page == max_pages:
                logger.info(f"Requesting all records '{method}' complete")
                break

            page += 1

    def get_user_top_tracks(self, period=None, **params):
        period = period or 'overall'
        return self.request_all_results(
            'user.gettoptracks', 'toptracks', 'track', period=period, **params)
