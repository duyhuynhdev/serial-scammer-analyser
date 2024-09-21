"""
TODO: 1. get pools in cluster 2. get WT in cluster 3. iterate each pool 4. get a list of scammers of the pool
"""

from idlelib.pyparse import trans

from entity.blockchain.Address import Pool
from utils import DataLoader


def calculate_cluster_true_profit(scammer_address) -> float:
    dataloader = DataLoader.DataLoader()
    pools = DataLoader.load_pool(scammer_address, dataloader)
    return sum(calculate_pool_true_profit(pool) for pool in pools)


def calculate_pool_true_profit(pool) -> float:
    high_value_token_position = pool.get_high_value_position()
    scam_token_position = 1 - high_value_token_position

    y = 0
    match pool.burns:
        # If the number of burn events is 1, the type of rug-pull is simple
        case [_]:  # Wildcard to match any single item without binding to a variable
            burn_total, fee_total = pool.get_total_burn_value(high_value_token_position)
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

    return 0.0


if __name__ == "__main__":
    cluster_id = "cluster_0x19b98792e98c54f58c705cddf74316aec0999aa6"
    cluster = DataLoader.load_cluster(cluster_id)
    calculate_cluster_true_profit(cluster[0].address)
