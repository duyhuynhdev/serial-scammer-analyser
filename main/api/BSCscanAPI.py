import requests

from utils.Settings import Setting

setting = Setting()

"""
Reference: https://docs.bscscan.com/
"""


def build_url(module, action, params, apikey=setting.BSCSCAN_API_KEY):
    url = setting.BSCSCAN_BASE_URL
    url = url + '?module=' + module
    url = url + '&action=' + action
    for key, value in params.items():
        url = url + '&' + key + '=' + value
    url = url + '&apikey=' + apikey
    return url


def get_normal_transactions(address, fromBlock, toBlock, page=1, offset=10000, apikey=setting.BSCSCAN_API_KEY):
    module = "account"
    action = "txlist"
    params = {"address": address,
              "startblock": str(fromBlock),
              "endblock": str(toBlock),
              "page": str(page),
              "offset": str(offset),
              "sort": "asc"}
    return call_api(module, action, params, apikey)


def get_internal_transactions(address, fromBlock, toBlock, page=1, offset=10000, apikey=setting.BSCSCAN_API_KEY):
    module = "account"
    action = "txlistinternal"
    params = {"address": address,
              "startblock": str(fromBlock),
              "endblock": str(toBlock),
              "page": str(page),
              "offset": str(offset),
              "sort": "asc"}
    return call_api(module, action, params, apikey)


def call_api(module, action, params, apikey=setting.BSCSCAN_API_KEY):
    api_url = build_url(module, action, params, apikey)
    response = requests.get(api_url, headers={"Content-Type": "application/json"})
    response_data = response.json()
    print(response_data)

    assert (response_data["message"] == 'Unexpected error, timeout or server too busy. Please try again later' or response_data["status"] == "1" or isinstance(response_data["result"], list))
    if response_data["message"] == "Unexpected error, timeout or server too busy. Please try again later":
        return call_api(module, action, params, apikey)
    return response_data["result"]


def get_contract_creation_info(address_list, apikey=setting.BSCSCAN_API_KEY):
    addresses = ",".join(address_list)
    module = "contract"
    action = "getcontractcreation"
    params = {"contractaddresses": addresses}
    return call_api(module, action, params, apikey)


def get_contract_verified_source_code(address, apikey=setting.BSCSCAN_API_KEY):
    module = "contract"
    action = "getsourcecode"
    params = {"address": address}
    return call_api(module, action, params, apikey)


def get_event_logs(address, fromBlock, toBlock, topic, page=1, offset=1000, apikey=setting.BSCSCAN_API_KEY):
    module = "logs"
    action = "getLogs"
    params = {"address": address,
              "fromBlock": str(fromBlock),
              "toBlock": str(toBlock),
              "topic0": topic,
              "page": str(page),
              "offset": str(offset)}
    return call_api(module, action, params, apikey)
