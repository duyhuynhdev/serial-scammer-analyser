from utils.DataLoader import DataLoader as DataLoaderClass
from entity.blockchain.Address import Pool
from typing import Set
from utils import DataLoader


class PoolsProfitCalculator:
    def __init__(
        self, _dataloader: DataLoaderClass, scammer_pools: Set[Pool]
    ):  # Type hint for dataloader
        self.dataloader = _dataloader
        self.scammer_pools = scammer_pools

    def calculate(self) -> float:
        return sum(
            self.calculate_per_pool(scammer_pool) for scammer_pool in self.scammer_pools
        )

    def calculate_per_pool(self, pool: Pool) -> float:
        high_value_token_position = pool.get_high_value_position()
        scam_token_position = 1 - high_value_token_position

        y = 0
        match pool.burns:
            # If the number of burn events is 1, the type of rug-pull is simple
            case [_]:  # Wildcard to match any single item without binding to a variable
                burn_total, fee_total = pool.get_total_burn_value(
                    high_value_token_position
                )
                y += burn_total - fee_total
            # If the number of burn events is greater than 1, the type of rug-pull is "sell"
            case [_, *_]:
                max_swap, swap_fee = pool.get_max_swap_value(high_value_token_position)
                y += max_swap - swap_fee
            case _:
                raise ValueError("pool.burns should have at least one burn")

        x = 0
        mint_total, fee_total = pool.get_total_mint_value(high_value_token_position)
        scam_token = pool.get_scam_token(scam_token_position)
        normal_txs, _ = DataLoader.load_transaction_by_address(scam_token.creator)
        creation_fee = next(
            (
                normal_tx.get_transaction_fee()
                for normal_tx in normal_txs
                if normal_tx.hash == scam_token.creation_tx
            ),
            0,  # Default value if no matching transaction is found
        )
        x = mint_total + fee_total + creation_fee

        # Label WT nodes
        """
        1. Go through all nodes in the cluster
        2. Look into pools that v swaps tokens with
        """
        return 0.0
