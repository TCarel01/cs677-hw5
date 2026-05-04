import numpy as np
import pandas as pd
import plotly.express as px
import hw5_testing
import p2p_node as p2p
import main
import warehouse_node


def dict_to_network(node_dict: dict, warehouse_id: int, warehouse_port: int, num_traders: int, synchronous:bool):
    nodes = dict()
    network_dict = {nid: node_dict[nid]["port"] for nid in node_dict.keys()}
    for nid in node_dict.keys():
        port = node_dict[nid]["port"]
        node_view_of_network = network_dict.copy()
        del node_view_of_network[nid]
        n = p2p.P2PNode(id=nid,
                        port_number=port,
                        is_buyer=node_dict[nid]["is_buyer"],
                        is_seller=node_dict[nid]["is_seller"],
                        nodes=node_view_of_network,
                        warehouse_port=warehouse_port,
                        num_traders=num_traders,
                        shopping_list=node_dict[nid]["shopping_list"],
                        selling_list=node_dict[nid]["selling_list"],
                        synchronized=synchronous,
                        )
        nodes[nid] = n
    wh_node = warehouse_node.Warehouse(id=warehouse_id,
                                       port=warehouse_port,
                                       nodes=network_dict,
                                       synchronous=synchronous)
    nodes[warehouse_id] = wh_node
    return nodes


def generate_output():
    node_dict = {
            1: dict(
                port=49153, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            2: dict(
                port=49154, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            3: dict(
                port=49155, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            4: dict(
                port=49156, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            5: dict(
                port=49157, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            6: dict(
                port=49158, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            7: dict(
                port=49159, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            8: dict(
                port=49160, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            9: dict(
                port=49161, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            10: dict(
                port=49162, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
        }
    nodes = dict_to_network(node_dict=node_dict, warehouse_id=0, warehouse_port=49152, num_traders=2, synchronous=False)
    main.run_network(nodes, run_time=100, stop_network=True)
    return

def read_data(caching:bool):
    columns = ["ts", "uid", "status"]
    if caching:
        df = pd.read_csv("caching_output.csv", names=columns)
    else:
        df = pd.read_csv("non_caching_output.csv", names=columns)
    df["ts"] = pd.to_datetime(df["ts"])
    return df

def warehouse_throughout():
    # Read in and clean up data
    columns = ["ts", "uid", "status"]
    caching_df = read_data(caching=True)
    non_caching_df = read_data(caching=False)
    # Calculate runtime
    caching_runtime = (caching_df["ts"].max() - caching_df["ts"].min()).seconds
    non_caching_runtime = (non_caching_df["ts"].max() - non_caching_df["ts"].min()).seconds
    # Calculate total items sold
    caching_df["completed purchase"] = caching_df["status"].str.contains("purchased")
    non_caching_df["completed purchase"] = non_caching_df["status"].str.contains("purchased")
    caching_df = caching_df[caching_df["completed purchase"]]
    non_caching_df = non_caching_df[non_caching_df["completed purchase"]]
    caching_df["quantity"] = caching_df["status"].str.split().str.slice(-2, -1).apply(lambda x: x[0]).astype(int)
    non_caching_df["quantity"] = non_caching_df["status"].str.split().str.slice(-2, -1).apply(lambda x: x[0]).astype(int)
    caching_total = caching_df["quantity"].sum()
    non_caching_total = non_caching_df["quantity"].sum()
    # Calculate throughput (items sold per seconds)
    caching_throughput = caching_total / caching_runtime
    non_caching_throughput = non_caching_total / non_caching_runtime
    print(f"Caching throughput: {caching_throughput}")
    print(f"Non caching throughput: {non_caching_throughput}")
    return

def overselling_rate():
    df = read_data(caching=True)

    return

def fail_impact():
    return


if __name__ == "__main__":
    #generate_output()
    
    #warehouse_throughout()
    overselling_rate()


