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
        if v > 0:
            if f in network_address and t in network_address:
                G.add_edge(f, t, value=v)
            elif (f in endnodes and t in network_address) or (t in endnodes and f in network_address):
                if not G.has_node(f):
                    G.add_node(f, label="exchanges", color='green')
                if not G.has_node(t):
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
        net.add_node(node, color=color, label=label)

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
    dex = 'univ2'
    cluster = LightCluster.LightCluster(4004)
    cluster.load("/mnt/Storage/Data/Blockchain/DEX/pancakeswap/processed/cluster/")
    build_graph(cluster, dex)
    # generate_png("graph.html","graph")
