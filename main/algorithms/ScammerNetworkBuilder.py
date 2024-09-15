from tqdm import tqdm
from data_collection.AccountCollector import CreatorCollector
from entity import Node
from entity.Cluster import Cluster
from entity.Node import NodeLabel
from entity.OrderedQueue import OrderedSetQueue
from utils.DataLoader import DataLoader
from utils.Settings import Setting
from utils.Path import Path
from utils import Utils as ut
from utils import Constant

path = Path()
setting = Setting()
dataloader = DataLoader()


def scammer_clustering(dex='univ2'):
    finish_set = {"0xbb0e420c761d7d5e3ff881bbd6ad8059b2ddf33d", "0x4d904ce82193d62dd6415d244466b8e980e0effa", "0x0300282a3bbff0a6000fc240501c9c2a25d4dd27", "0x308092d19c2680b590e3fac72184dbb0da4de28c", "0x287e3428d2846e30a4a0bab0b3682ce8b6ce6f0d"}
    for rp in tqdm(dataloader.scam_pools):
        s = dataloader.creators[rp]
        if s in finish_set:
            continue
        print("*"*200)
        print(f"START CLUSTERING (ADDRESS {s})")
        cluster, scanned_nodes = explore_scammer_network(s, dex)
        finish_set.update(set(scanned_nodes))
        print(f"END CLUSTERING (ADDRESS {s})")
        print("*" * 200)


def check_if_end_node(address):
    if address.lower() in dataloader.bridge_addresses:
        return True, NodeLabel.BRIDGE
    if address.lower() in dataloader.defi_addresses:
        return True, NodeLabel.DEFI
    if address.lower() in dataloader.MEV_addresses:
        return True, NodeLabel.MEV
    if address.lower() in dataloader.mixer_addresses:
        return True, NodeLabel.MIXER
    if address.lower() in dataloader.wallet_addresses:
        return True, NodeLabel.WALLET
    if address.lower() in dataloader.other_addresses:
        return True, NodeLabel.OTHER
    return False, None


def explore_scammer_network(scammer_address, dex='univ2'):
    cluster_path = eval('path.{}_cluster_path'.format(dex))
    if ut.is_contract_address(scammer_address):
        return None, list()
    # end_nodes, scam_tokens, scammers, scam_pools = get_base_datasets(dex)
    creator_collector = CreatorCollector()
    # create node for an address with downloading all transactions and discovering/classifying neighbours
    node = Node.create_node(scammer_address, None, dataloader, NodeLabel.S, dex)
    queue = OrderedSetQueue()
    traversed_nodes = set()
    dead_nodes = set()
    queue.put(node)
    cluster = Cluster(scammer_address)
    cluster.add_node(node)
    # start exploring
    while not queue.empty():
        root: Node.Node = queue.get()
        print("ROOT ADDRESS", root.address)
        print("PATH", "->".join(root.path()))
        if root.address in traversed_nodes or root.address in dead_nodes:
            continue
        traversed_nodes.add(root.address)
        print("EOA NODES", len(root.eoa_neighbours))
        print("CONTRACT NODES", len(root.contract_neighbours))

        # if NodeLabel.BIG in root.labels:
        #     print("TEMPORARY SKIP BIG NODE")
        #     print("QUEUE LEN", queue.qsize())
        #     print("SCANNED NODES", len(traversed_nodes))
        #     print("DEAD NODES", len(dead_nodes))
        #     print("=" * 50)
        #     continue
        # EOA neighbours
        for eoa_neighbour_address in root.eoa_neighbours:
            eoa_neighbour_address = eoa_neighbour_address.lower()
            if eoa_neighbour_address not in traversed_nodes:
                check_endnode, label = check_if_end_node(eoa_neighbour_address)
                if check_endnode:
                    # add all end nodes into traversed nodes
                    dead_nodes.add(eoa_neighbour_address)
                    if not cluster.is_address_exist(eoa_neighbour_address):
                        eoa_end_node = Node.create_end_node(eoa_neighbour_address, root, label)
                        cluster.add_node(eoa_end_node)
                else:
                    if not cluster.is_address_exist(eoa_neighbour_address):
                        label = NodeLabel.EOA
                        if eoa_neighbour_address.lower() in dataloader.scammers:
                            label = NodeLabel.S
                        eoa_node = Node.create_node(eoa_neighbour_address, root, dataloader, label, dex)
                        cluster.add_node(eoa_node)
                        # put unvisited neighbours into queue
                        queue.put(eoa_node)
        # Contract neighbours
        for contract_neighbour_address in root.contract_neighbours:
            contract_neighbour_address = contract_neighbour_address.lower()
            if contract_neighbour_address not in traversed_nodes:
                if not cluster.is_address_exist(contract_neighbour_address):
                    dead_nodes.add(contract_neighbour_address)
                    check_endnode, label = check_if_end_node(contract_neighbour_address)
                    if check_endnode:
                        contract_end_node = Node.create_end_node(contract_neighbour_address, root, label)
                        cluster.add_node(contract_end_node)
                    else:
                        contract_end_node = Node.create_end_node(contract_neighbour_address, root, NodeLabel.UC)
                    cluster.add_node(contract_end_node)
        # add connections
        print("QUEUE LEN", queue.qsize())
        print("SCANNED NODES", len(traversed_nodes))
        print("DEAD NODES", len(dead_nodes))
        print("=" * 50)
    cluster.export(cluster_path)
    return cluster, set(traversed_nodes | dead_nodes)


if __name__ == '__main__':
    explore_scammer_network("0x287e3428d2846e30a4a0bab0b3682ce8b6ce6f0d")
    # explore_scammer_network("0x43e129c47dfd4abcf48a24f6b2d8ba6f49261f39")
    # explore_scammer_network_by_ignoring_creator("0x48f0fc8dfc672dd45e53b6c53cd5b09c71d9fbd6")
    # explore_scammer_network("0x81cfe8efdb6c7b7218ddd5f6bda3aa4cd1554fd2")
    # explore_scammer_network("0x30a9173686eb332ff2bdcea95da3b2860597a19d")
    # explore_scammer_network("0xb16a24e954739a2bbc68c5d7fbbe2e27f17dfff9")
    # print(is_contract_address("0x81cfe8efdb6c7b7218ddd5f6bda3aa4cd1554fd2"))
    # print(len(collect_end_nodes()))
    # scammer_clustering()
