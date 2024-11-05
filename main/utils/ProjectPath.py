import os


class ProjectPath(object):
    instance = None

    def __new__(cls):
        if not hasattr(cls, 'instance') or cls.instance is None:
            cls.instance = super(ProjectPath, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        # GLOBAL DATA
        ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.resource_root_path = os.path.join(ROOT_FOLDER, "resources")
        self.data_root_path = os.path.join("/mnt", "Storage", "Data", "Blockchain", "DEX")
        if not os.path.exists(self.data_root_path):
            self.data_root_path = os.path.join(self.resource_root_path, "data")
        print("ENVIRONMENT VARIABLES")
        print("DATA PATH:", self.data_root_path)
        self.popular_tokens = os.path.join(self.data_root_path, "popular_tokens")
        self.univ2_base_path = os.path.join(self.data_root_path, "uniswap")
        self.univ2_account_path = os.path.join(self.univ2_base_path, "account")
        self.univ2_normal_tx_path = os.path.join(self.univ2_account_path, "normal_tx")
        self.univ2_internal_tx_path = os.path.join(self.univ2_account_path, "internal_tx")
        self.univ2_pool_path = os.path.join(self.univ2_base_path, "pool")
        self.univ2_address_path = os.path.join(self.univ2_pool_path, "address")
        self.univ2_pool_events_path = os.path.join(self.univ2_pool_path, "events")
        self.univ2_token_path = os.path.join(self.univ2_base_path, "token")
        self.univ2_token_events_path = os.path.join(self.univ2_token_path, "events")
        self.univ2_token_source_code_path = os.path.join(self.univ2_token_path, "solidity")
        self.univ2_token_ast_path = os.path.join(self.univ2_token_path, "ast")
        self.univ2_tokenization_path = os.path.join(self.univ2_token_path, "tokenization")
        self.univ2_intra_similarity_path = os.path.join(self.univ2_token_path, "intra_similarity")
        self.univ2_inter_similarity_path = os.path.join(self.univ2_token_path, "inter_similarity")
        self.univ2_individual_similarity_path = os.path.join(self.univ2_token_path, "individual_similarity")
        self.univ2_processed_path = os.path.join(self.univ2_base_path, "processed")
        self.univ2_public_addresses_path = os.path.join(self.univ2_processed_path, "public_addresses")
        self.univ2_cluster_path = os.path.join(self.univ2_processed_path, "cluster")
        self.univ2_scammer_chain_path = os.path.join(self.univ2_base_path, "scammer_chain")
        self.univ2_star_shape_path = os.path.join(self.univ2_base_path, "star_shape")
        self.univ2_visited_scammer_path = os.path.join(self.univ2_processed_path, "visited_scammers")

        self.panv2_base_path = os.path.join(self.data_root_path, "pancakeswap")
        self.panv2_account_path = os.path.join(self.panv2_base_path, "account")
        self.panv2_normal_tx_path = os.path.join(self.panv2_account_path, "normal_tx")
        self.panv2_internal_tx_path = os.path.join(self.panv2_account_path, "internal_tx")
        self.panv2_pool_path = os.path.join(self.panv2_base_path, "pool")
        self.panv2_address_path = os.path.join(self.panv2_pool_path, "address")
        self.panv2_pool_events_path = os.path.join(self.panv2_pool_path, "events")
        self.panv2_token_path = os.path.join(self.panv2_base_path, "token")
        self.panv2_token_events_path = os.path.join(self.panv2_token_path, "events")
        self.panv2_token_source_code_path = os.path.join(self.panv2_token_path, "solidity")
        self.panv2_token_ast_path = os.path.join(self.panv2_token_path, "ast")
        self.panv2_tokenization_path = os.path.join(self.panv2_token_path, "tokenization")
        self.panv2_intra_similarity_path = os.path.join(self.panv2_token_path, "intra_similarity")
        self.panv2_inter_similarity_path = os.path.join(self.panv2_token_path, "inter_similarity")
        self.panv2_individual_similarity_path = os.path.join(self.panv2_token_path, "individual_similarity")
        self.panv2_processed_path = os.path.join(self.panv2_base_path, "processed")
        self.panv2_scammer_chain_path = os.path.join(self.panv2_base_path, "scammer_chain")
        self.panv2_star_shape_path = os.path.join(self.panv2_base_path, "star_shape")
        self.panv2_public_addresses_path = os.path.join(self.panv2_processed_path, "public_addresses")
        self.panv2_cluster_path = os.path.join(self.panv2_processed_path, "cluster")
        self.panv2_visited_scammer_path = os.path.join(self.panv2_processed_path, "visited_scammers")
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
