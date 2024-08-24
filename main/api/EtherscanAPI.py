import requests

from utils.Settings import Setting

setting = Setting()

"""
Reference: https://docs.etherscan.io/api-endpoints
"""


def build_url(module, action, params):
    url = setting.ETHERSCAN_BASE_URL
    url = url + '?module=' + module
    url = url + '&action=' + action
    for key, value in params.items():
        url = url + '&' + key + '=' + value
    url = url + '&apikey=' + setting.ETHERSCAN_API_KEY
    return url


def call_api(module, action, params):
    api_url = build_url(module, action, params)
    response = requests.get(api_url, headers={"Content-Type": "application/json"})
    return response.json()["result"]


def get_contract_creation_info(address_list):
    """
    Getting given contract creator and its creation transaction hash
    :param address_list: list of address (up to 5 at a time)
    :return:
    """
    addresses = ",".join(address_list)
    module = "contract"
    action = "getcontractcreation"
    params = {"contractaddresses": addresses}
    return call_api(module, action, params)


def get_contract_verified_source_code(address):
    module = "contract"
    action = "getsourcecode"
    params = {"address": address}
    return call_api(module, action, params)
