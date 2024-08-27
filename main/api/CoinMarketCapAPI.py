import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

from utils.Settings import Setting

setting = Setting()

"""
Reference: https://coinmarketcap.com/api/documentation/v1/
"""

api_map = {
    "cmc_id_map": "/v1/cryptocurrency/map",
    "listing_latest": "/v1/cryptocurrency/listings/latest"
}


def call_api(api_name, params):
    try:
        api_url = setting.CMC_BASE_URL + api_map[api_name]
        response = requests.get(api_url, params=params, headers={"Accepts": "application/json", "X-CMC_PRO_API_KEY": setting.CMC_API_KEY})
        return response.json()
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)


def get_top_crypto_ranking(start):
    params = {
        "start": start,
        "limit": 5000,
        "sort": "cmc_rank"
    }
    return call_api("cmc_id_map", params)


def get_latest_crypto_listing(start):
    params = {
        "start": start,
        "limit": 5000,
        "sort": "market_cap",
        "aux": "num_market_pairs,cmc_rank,date_added,tags,platform,max_supply,circulating_supply,total_supply,market_cap_by_total_supply,volume_24h_reported,volume_7d,volume_7d_reported,volume_30d,volume_30d_reported,is_market_cap_included_in_calc"
    }
    return call_api("listing_latest", params)


if __name__ == '__main__':
    print(get_top_crypto_ranking(1))
