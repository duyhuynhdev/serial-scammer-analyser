import os


class Path(object):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Path, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        # GLOBAL DATA
        self.data_root_path = os.path.join("/media", "dustin", "Storage", "Data", "blockchain", "DEX")
        self.univ2_base_path = os.path.join(self.data_root_path, "uniswap")
        self.univ2_tokens_path = os.path.join(self.univ2_base_path, "token")
        self.univ2_pools_path = os.path.join(self.univ2_base_path, "pool")
        self.univ2_address_path = os.path.join(self.univ2_pools_path, "address")
        self.univ2_pool_events_path = os.path.join(self.univ2_pools_path, "events")
        self.univ2_token_events_path = os.path.join(self.univ2_tokens_path, "events")

        self.panv2_base_path = os.path.join(self.data_root_path, "pancakeswap")
        self.panv2_pools_path = os.path.join(self.panv2_base_path, "pool")
        self.panv2_address_path = os.path.join(self.panv2_pools_path, "address")
        self.panv2_pool_events_path = os.path.join(self.panv2_pools_path, "events")
        self.panv2_tokens_path = os.path.join(self.panv2_base_path, "token")
        self.panv2_token_events_path = os.path.join(self.panv2_tokens_path, "events")

        # LOCAL DATA
        ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        # Resource
        self.resource_root_path = os.path.join(ROOT_FOLDER, "resources")
        self.abi_path = os.path.join(self.resource_root_path, "abi")

        # Example contract
        self.example_contracts_path = os.path.join(self.data_root_path, "example_contracts")
        self.example_tokens_path = os.path.join(self.example_contracts_path, "tokens")
        self.example_tokens_after_numbering_path = os.path.join(self.example_contracts_path, "tokens_after_numbering")

        # Trapdoor path
        self.trapdoor_data_root_path = os.path.join(ROOT_FOLDER, "trapdoor_data")
