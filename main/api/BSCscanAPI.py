from time import sleep

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


def get_normal_transactions(address, fromBlock, toBlock, page=1, offset=10000, apikey=setting.ETHERSCAN_API_KEY):
    module = "account"
    action = "txlist"
    params = {"address": address,
              "startblock": str(fromBlock),
              "endblock": str(toBlock),
              "page": str(page),
              "offset": str(offset),
              "sort": "asc"}
    return call_api(module, action, params, apikey)


def get_internal_transactions(address, fromBlock, toBlock, page=1, offset=10000, apikey=setting.ETHERSCAN_API_KEY):
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
    retry = 10
    response_data = None
    while retry > 0:
        try:
            response = requests.get(api_url, headers={"Content-Type": "application/json"})
            response_data = response.json()
            if (response_data is not None) and (response_data["status"] == "1" or isinstance(response_data["result"], list) or response_data["message"] == 'No data found'):
                break
        except Exception as e:
            print(api_url)
            print("SLEEP 3 SECONDS AND RETRY")
            sleep(3)
            retry -= 1
    # print(response_data)
    assert (response_data is not None) and (response_data["status"] == "1" or isinstance(response_data["result"], list) or response_data["message"] == 'No data found')
    return response_data["result"] if response_data["result"] is not None else []

def geth_api(module, action, params, apikey=setting.ETHERSCAN_API_KEY):
    api_url = build_url(module, action, params, apikey)
    print(api_url)
    response = requests.get(api_url, headers={"Content-Type": "application/json"})
    response_data = response.json()
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

def get_tx_by_hash(txhash, apikey=setting.BSCSCAN_API_KEY):
    module = "proxy"
    action = "eth_getTransactionByHash"
    params = {"txhash": txhash}
    return geth_api(module, action, params, apikey)