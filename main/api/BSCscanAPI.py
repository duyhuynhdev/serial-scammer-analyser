import requests

from utils.Settings import Setting

setting = Setting()

"""
Reference: https://docs.bscscan.com/
"""

def build_url(module, action, params, apikey = setting.BSCSCAN_API_KEY):
    url = setting.ETHERSCAN_BASE_URL
    url = url + '?module=' + module
    url = url + '&action=' + action
    for key, value in params.items():
        url = url + '&' + key + '=' + value
    url = url + '&apikey=' + apikey
    return url


def call_api(module, action, params, apikey = setting.BSCSCAN_API_KEY):
    api_url = build_url(module, action, params, apikey)
    response = requests.get(api_url, headers={"Content-Type": "application/json"})
    return response.json()["result"]


def get_contract_creation_info(address_list, apikey = setting.BSCSCAN_API_KEY):
    addresses = ",".join(address_list)
    module = "contract"
    action = "getcontractcreation"
    params = {"contractaddresses": addresses}
    return call_api(module, action, params, apikey)


def get_contract_verified_source_code(address, apikey = setting.BSCSCAN_API_KEY):
    module = "contract"
    action = "getsourcecode"
    params = {"address": address}
    return call_api(module, action, params, apikey)

def get_event_logs(address, fromBlock, toBlock, topic, page=1, offset=1000, apikey = setting.BSCSCAN_API_KEY):
    module = "logs"
    action = "getLogs"
    params = {"address": address,
              "fromBlock": str(fromBlock),
              "toBlock": str(toBlock),
              "topic0": topic,
              "page": str(page),
              "offset": str(offset)}
    return call_api(module, action, params, apikey)

