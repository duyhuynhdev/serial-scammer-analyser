
# Cluster
# get pools in cluster
# get WT in cluster
# iterate each pool
## get list of scammers of the pool
import pandas as pd
from utils.DataLoader import DataLoader

dataloader = DataLoader()

def execute(cluster_id):
    cluster = dataloader.load_cluster(cluster_id)

    pass