
# Cluster
# get pools in cluster
# get WT in cluster
# iterate each pool
## get list of scammers of the pool
import pandas as pd
from utils import DataLoader

dataloader = DataLoader.DataLoader()


def calculate_pool_true_profit(scammer_address):

    pool = DataLoader.load_pool(scammer_address, dataloader)

    y = 0
    high_value_position = pool.get_high_value_position()
    match pool.burns:
        # If the number of burn events is 1, the type of rug-pull is simple
        case [_]:  # Wildcard to match any single item without binding to a variable
            burn_total, fee_total = pool.get_total_burn_value(high_value_position)
            y += (burn_total - fee_total)
        # If the number of burn events is greater than 1, the type of rug-pull is "sell"
        case [_, *_]:
            max_swap, swap_fee = pool.get_max_swap_value(high_value_position)
            y += (max_swap - swap_fee)
        case _:
            raise ValueError("pool.burns should have at least one burn")




if __name__ == '__main__':
    cluster_id = "cluster_0x19b98792e98c54f58c705cddf74316aec0999aa6"
    cluster = DataLoader.load_cluster(cluster_id)
    calculate_pool_true_profit(cluster[0].address)