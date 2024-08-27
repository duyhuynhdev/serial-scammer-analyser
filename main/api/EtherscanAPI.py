import requests

from utils.Settings import Setting

setting = Setting()

"""
Reference: https://docs.etherscan.io/api-endpoints
"""


def build_url(module, action, params, apikey = setting.ETHERSCAN_API_KEY):
    url = setting.ETHERSCAN_BASE_URL
    url = url + '?module=' + module
    url = url + '&action=' + action
    for key, value in params.items():
        url = url + '&' + key + '=' + value
    url = url + '&apikey=' + apikey
    # print(url)
    return url


def call_api(module, action, params, apikey = setting.ETHERSCAN_API_KEY):
    api_url = build_url(module, action, params, apikey)
    response = requests.get(api_url, headers={"Content-Type": "application/json"})
    return response.json()["result"]


def get_contract_creation_info(address_list, apikey = setting.ETHERSCAN_API_KEY):
    addresses = ",".join(address_list)
    module = "contract"
    action = "getcontractcreation"
    params = {"contractaddresses": addresses}
    return call_api(module, action, params, apikey)


def get_contract_verified_source_code(address, apikey = setting.ETHERSCAN_API_KEY):
    module = "contract"
    action = "getsourcecode"
    params = {"address": address}
    return call_api(module, action, params, apikey)

def get_event_logs(address, fromBlock, toBlock, topic, page=1, offset=1000, apikey = setting.ETHERSCAN_API_KEY):
    module = "logs"
    action = "getLogs"
    params = {"address": address,
              "fromBlock": str(fromBlock),
              "toBlock": str(toBlock),
              "topic0": topic,
              "page": str(page),
              "offset": str(offset)}
    return call_api(module, action, params, apikey)


if __name__ == '__main__':
    print(get_event_logs("0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", 0, 15074139, "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"))