import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import os
from utils import Utils as ut
from utils.Settings import Setting

setting = Setting()


def call_api(api_url, params):
    try:
        response = requests.get(api_url, params=params, headers={"Accepts": "application/json"})
        return response.json()
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)


def get_tokens_coingecko(output_path):
    # refer https://www.coingecko.com/en/api/documentation
    url = "https://api.coingecko.com/api/v3/coins/list"
    params = {
        "include_platform": "true"
    }
    result = call_api(url, params)
    ut.write_json(output_path, result)


def get_tokens_ethplorer(output_path):
    # refer https://www.coingecko.com/en/api/documentation
    url = "https://api.ethplorer.io/getTopTokens"
    params = {
        "apiKey": "freekey"
    }
    result = call_api(url, params)
    ut.write_json(output_path, result)


