
# Cluster
# get pools in cluster
# get WT in cluster
# iterate each pool
## get list of scammers of the pool
import pandas as pd
from utils import DataLoader

dataloader = DataLoader.DataLoader()

def execute(cluster_id):
    cluster = DataLoader.load_cluster(cluster_id)
    pool = DataLoader.load_pool(cluster[0].address,dataloader)
    print(cluster)

if __name__ == '__main__':
    cluster_id = "cluster_0x19b98792e98c54f58c705cddf74316aec0999aa6"
    execute(cluster_id)