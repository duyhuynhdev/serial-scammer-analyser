import configparser
import os
from web3 import Web3
from utils.Path import Path

path = Path()


class Setting(object):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Setting, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        # loading config.ini to global variables

        config = configparser.ConfigParser()
        config.read(os.path.join(path.resource_root_path, "config.ini"))

        self.INFURA_ETH_NODE_URL = config.get("INFURA", "ETH_NODE_URL")
        self.INFURA_BSC_NODE_URL = config.get("INFURA", "BSC_NODE_URL")
        self.INFURA_API_KEYS = config.get("INFURA", "API_KEYS").split(" ")
        self.INFURA_API_MAIN_KEY = self.INFURA_API_KEYS[0]

        self.ETHERSCAN_BASE_URL = config.get("ETHERSCAN", "ETHERSCAN_BASE_URL")
        self.ETHERSCAN_API_KEYS = config.get("ETHERSCAN", "API_KEYS").split(" ")
        self.ETHERSCAN_API_KEY = self.ETHERSCAN_API_KEYS[0]

        self.BSCSCAN_BASE_URL = config.get("BSCSCAN", "BSCSCAN_BASE_URL")
        self.BSCSCAN_API_KEYS = config.get("BSCSCAN", "API_KEYS").split(" ")
        self.BSCSCAN_API_KEY = self.ETHERSCAN_API_KEYS[0]

        self.MORALIS_BASE_URL = config.get("MORALIS", "MORALIS_BASE_URL")
        self.MORALIS_API_KEY = config.get("MORALIS", "API_KEY")

        self.CMC_BASE_URL = config.get("CMC", "CMC_BASE_URL")
        self.CMC_API_KEY = config.get("CMC", "API_KEY")

        self.UNIV2_FACTORY_ADDRESS = config.get("UNISWAPV2", "FACTORY_ADDRESS")
        self.UNIV2_NUM_OF_PAIRS = int(config.get("UNISWAPV2", "NUM_OF_PAIRS"))

        self.PANV2_FACTORY_ADDRESS = config.get("PANCAKESWAP", "FACTORY_ADDRESS")
        self.PANV2_NUM_OF_PAIRS  = int(config.get("PANCAKESWAP", "NUM_OF_PAIRS"))

        self.ETH_TOKEN_ABI = open(os.path.join(path.abi_path, "eth_token_abi.json")).read()
        self.UNIV2_FACTORY_ABI = open(os.path.join(path.abi_path, "uniswap_v2_factory_abi.json")).read()
        self.UNIV2_POOL_ABI = open(os.path.join(path.abi_path, "uniswap_v2_pool_abi.json")).read()

        self.BSC_TOKEN_ABI = open(os.path.join(path.abi_path, "bsc_token_abi.json")).read()
        self.PANV2_FACTORY_ABI = open(os.path.join(path.abi_path, "pancakeswap_v2_factory_abi.json")).read()
        self.PANV2_POOL_ABI = open(os.path.join(path.abi_path, "panackeswap_v2_pool_abi.json")).read()

        # create web3 instance
        self.infura_web3 = Web3(Web3.HTTPProvider(self.INFURA_ETH_NODE_URL + self.INFURA_API_MAIN_KEY))
