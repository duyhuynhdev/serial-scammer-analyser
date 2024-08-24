from entity.Cluster import Cluster

SCAMMER = set()
POOLS = set()
TOKENS = set()
END_NODES = set()


def is_scammer(address):
    return True if address in SCAMMER else False


def is_scam_pool(address):
    return True if address in POOLS else False


def is_scam_token(address):
    return True if address in TOKENS else False


def is_end_node(address):
    return True if address in END_NODES else False


def BFS(scammer) -> Cluster:
    cluster = Cluster()
    return cluster


def account_clustering(scammers):
    clusters = []
    for s in scammers:
        clusters.append(BFS(s))
    return clusters


def node_labelling(cluster: Cluster):
    return cluster


def pattern_recognition(cluster: Cluster):
    scammers, scammer_network = cluster.get_scammers()
