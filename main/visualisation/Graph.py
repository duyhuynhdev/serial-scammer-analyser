import math

import networkx as nx
from pyvis.network import Network
from data_collection.DataExtraction import endnodes
from entity import LightNode, LightCluster
from entity.blockchain.Transaction import NormalTransaction
from data_collection.AccountCollector import TransactionCollector
from utils import DataLoader
import matplotlib.pyplot as plt
from playwright.sync_api import sync_playwright


class GNode:
    def __init__(self, id, label, color):
        self.id = id
        self.label = label
        self.color = color


def get_main_labels(node: LightNode.LightNode):
    if LightNode.LightNodeLabel.COORDINATOR in node.labels:
        return LightNode.LightNodeLabel.COORDINATOR, 'purple'
    if LightNode.LightNodeLabel.SCAMMER in node.labels:
        return LightNode.LightNodeLabel.SCAMMER, 'red'
    if LightNode.LightNodeLabel.WASHTRADER in node.labels:
        return LightNode.LightNodeLabel.WASHTRADER, 'orange'
    if LightNode.LightNodeLabel.DEPOSITOR in node.labels:
        return LightNode.LightNodeLabel.DEPOSITOR, 'blue'
    if LightNode.LightNodeLabel.WITHDRAWER in node.labels:
        return LightNode.LightNodeLabel.WITHDRAWER, 'blue'
    if LightNode.LightNodeLabel.TRANSFER in node.labels:
        return LightNode.LightNodeLabel.TRANSFER, 'yellow'
    if LightNode.LightNodeLabel.BOUNDARY in node.labels:
        return LightNode.LightNodeLabel.BOUNDARY, 'grey'
    return "unknown", "grey"


def convert_to_gn(node: LightNode.LightNode):
    label, color = get_main_labels(node)
    return GNode(node.address, label, color)


def build_graph(cluster: LightCluster.LightCluster, dex='univ2'):
    G = nx.DiGraph()
    collector = TransactionCollector()
    endnodes = DataLoader.load_full_end_nodes(dex)
    gns = []
    network_address = set()
    transactions = []
    for node in cluster.nodes.values():
        gn = convert_to_gn(node)
        gns.append(gn)
        network_address.add(gn.id)
        G.add_node(gn.id, label=gn.label, color=gn.color)
        normal_txs, _ = collector.get_transactions(gn.id, dex, 5)
        transactions.extend(normal_txs)
    for tx in transactions:
        f = tx.sender
        t = tx.to
        v = tx.get_transaction_amount()
        if v > 0 and isinstance(f, str) and isinstance(t, str):
            if f in network_address and t in network_address:
                G.add_edge(f, t, value=v)
            elif (f in endnodes and t in network_address) or (t in endnodes and f in network_address):
                if not G.has_node(f):
                    print("f",f)
                    G.add_node(f, label="exchanges", color='green')
                if not G.has_node(t):
                    print("t", t)
                    G.add_node(t, label="exchanges", color='green')
                G.add_edge(f, t, value=v)
    # legend(G)
    visualizeGraph(f"{dex}_{cluster.id}_graph",G)

def visualizeGraph(name, G):
    net = Network('1500px', '1500px', notebook=True, cdn_resources='remote')
    # net.force_atlas_2based(central_gravity=0, spring_length=300, damping=1)
    net.repulsion(node_distance=200, central_gravity=0, spring_length=200, damping=1)
    for node, node_data in G.nodes(data=True):
        color = node_data.get('color')
        label = node_data.get('label')
        # Creating the iframe HTML for the node
        # iframe_html = f"<a href='https://etherscan.io/address/{node}' target='_blank'>{node}</a>"
        try:
            net.add_node(node, color=color, label=label)
        except:
            print(node)

    for u, v, data in G.edges(data=True):
        net.add_edge(u, v, arrows='to', color="black")
    net.show_buttons(filter_=['physics'])
    net.show(f"{name}.html")


def generate_png(url_file, name):
    with sync_playwright() as p:
        for browser_type in [p.chromium]:
            browser = browser_type.launch()
            page = browser.new_page()
            file = open(url_file, "r").read()
            page.set_content(file, wait_until="load")
            page.wait_for_timeout(30000)  # this timeout for correctly render big data page
            page.screenshot(path=f'{name}.png', full_page=True)
            browser.close()


if __name__ == '__main__':
    dex = 'panv2'
    # uni_accepted_cluster = [1528,
    #                         3025,
    #                         1519,
    #                         1647,
    #                         3628,
    #                         8739,
    #                         7004,
    #                         3605,
    #                         6615,
    #                         7585,
    #                         6717,
    #                         8653,
    #                         117,
    #                         7280,
    #                         90,
    #                         4508,
    #                         5518,
    #                         8256,
    #                         5033,
    #                         2027,
    #                         268,
    #                         641,
    #                         4009,
    #                         1565,
    #                         6696,
    #                         6556,
    #                         1147,
    #                         7031,
    #                         262,
    #                         8589,
    #                         171,
    #                         667,
    #                         2042,
    #                         6,
    #                         4152,
    #                         1507,
    #                         8028,
    #                         1573,
    #                         4205,
    #                         1263,
    #                         3049,
    #                         717,
    #                         5069,
    #                         3213,
    #                         3106,
    #                         1706,
    #                         3525,
    #                         7556,
    #                         2029,
    #                         8283,
    #                         8057,
    #                         3540,
    #                         1253,
    #                         5190,
    #                         7045,
    #                         8745,
    #                         735,
    #                         1739,
    #                         5747,
    #                         2116,
    #                         3015,
    #                         1686,
    #                         6016,
    #                         4064,
    #                         3636,
    #                         5218,
    #                         1540,
    #                         567,
    #                         2249,
    #                         6607,
    #                         1559,
    #                         85,
    #                         6155,
    #                         4060,
    #                         3594,
    #                         4006,
    #                         7562,
    #                         8631,
    #                         2111,
    #                         5710,
    #                         3112,
    #                         7063,
    #                         7275,
    #                         2503,
    #                         2057,
    #                         1256,
    #                         2047,
    #                         3166,
    #                         2054,
    #                         4596,
    #                         8612,
    #                         4020,
    #                         6724,
    #                         1731,
    #                         8125,
    #                         5011,
    #                         5093,
    #                         5067,
    #                         6509,
    #                         5013,
    #                         8699,
    #                         5029,
    #                         511,
    #                         1050,
    #                         1022,
    #                         4036,
    #                         5538,
    #                         6561,
    #                         7608,
    #                         5587,
    #                         6168,
    #                         5197,
    #                         249,
    #                         5736,
    #                         8279,
    #                         6114,
    #                         1570,
    #                         6530,
    #                         4004,
    #                         5543,
    #                         8587,
    #                         7581,
    #                         1650,
    #                         4141,
    #                         1664,
    #                         3116,
    #                         4590,
    #                         645,
    #                         2187,
    #                         7159,
    #                         8597,
    #                         5062,
    #                         306,
    #                         7153,
    #                         ]
    pan_accepted_cluster = [
        6504,
        6004,
        9006,
        5502,
        1007,
        9511,
        10001,
        7509,
        8516,
        12006,
        4504,
        2,
        2003,
        1001,
        3005,
        4002,
        4503,
        11508,
        4517,
        4501,
        8503,
        8505,
        5505,
        7001,
        2007,
        8504,
        9505,
        8501,
        9508,
        2502,
        6003,
        4510,
        8001,
        9509,
        2010,
        11001,
        2002,
        2004,
        4508,
    ]
    for id in pan_accepted_cluster:
        cluster = LightCluster.LightCluster(id)
        cluster.load("/mnt/Storage/Data/Blockchain/DEX/pancakeswap/processed/cluster/")
        build_graph(cluster, dex)
    # generate_png("graph.html","graph")
