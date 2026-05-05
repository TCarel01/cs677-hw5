import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import hw5_testing
import p2p_node as p2p
import main
import warehouse_node


def dict_to_network(node_dict: dict, warehouse_id: int, warehouse_port: int, num_traders: int, synchronous:bool, time_to_die:int):
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
                        leader_time_to_die=(datetime.now() + timedelta(0, time_to_die) if nid == max(list(node_dict.keys())) else None)
                        )
        nodes[nid] = n
    wh_node = warehouse_node.Warehouse(id=warehouse_id,
                                       port=warehouse_port,
                                       nodes=network_dict,
                                       synchronous=synchronous)
    nodes[warehouse_id] = wh_node
    return nodes


def generate_output(caching):
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
                port=49157, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            6: dict(
                port=49158, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            7: dict(
                port=49159, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            8: dict(
                port=49160, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            9: dict(
                port=49161, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            10: dict(
                port=49162, is_buyer=True, is_seller=False,
                shopping_list=None, selling_list=None,
            ),
            11: dict(
                port=49163, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            12: dict(
                port=49164, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            14: dict(
                port=49165, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            15: dict(
                port=49166, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            16: dict(
                port=49167, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            17: dict(
                port=49168, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            18: dict(
                port=49169, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            19: dict(
                port=49170, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            20: dict(
                port=49171, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            21: dict(
                port=49172, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            22: dict(
                port=49173, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
            23: dict(
                port=49174, is_buyer=False, is_seller=True,
                shopping_list=None, selling_list=None,
            ),
        }
    node_dict = {nid: dict(port=49152+nid, is_buyer=nid<=4, is_seller=nid>4, shopping_list=None, selling_list=None) for nid in range(1, 10)}
    num_buyers = len([n for n in node_dict.keys() if node_dict[n]["is_buyer"]])
    num_sellers = len([n for n in node_dict.keys() if node_dict[n]["is_seller"]])
    print(f"{datetime.now()}, test status, there are {num_buyers} buyers and {num_sellers} sellers")
    nodes = dict_to_network(node_dict=node_dict, warehouse_id=0, warehouse_port=49152, num_traders=2, synchronous=not caching, time_to_die=150)
    main.run_network(nodes, run_time=300, stop_network=True)
    return

def read_data(caching:bool):
    columns = ["ts", "uid", "status"]
    if caching:
        df = pd.read_csv("caching_output_fault_test_50_clients.csv", names=columns)
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
    # Get df of just the oversells
    failed_buy_mask = df["status"].str.contains("depleted")
    passed_cache_mask = ~df["status"].str.contains("expected")
    oversell_mask = failed_buy_mask & passed_cache_mask
    oversell_df = df[oversell_mask]
    # Get df of all starting purchases
    start_buy_mask = df["status"].str.contains("is buying")
    start_buy_df = df[start_buy_mask]
    # Calculate oversell rates
    oversell_rate = oversell_df.shape[0] / start_buy_df.shape[0]
    print(f"Overselling rate is {oversell_rate}")
    return

def fault_analysis():
    # Read in data and sort by time
    df = read_data(caching=True)
    df = df.sort_values("ts").reset_index(drop=True)
    # get time of first and last (finished) transaction message
    is_tx_mask = (df["uid"].str.len() == 37)
    is_restock_finish = (df["status"].str.contains("restocked"))
    is_buy_finish = (df["status"].str.contains("purchased"))
    is_tx_finish = is_restock_finish | is_buy_finish
    first_tx_time = df[is_tx_mask]["ts"].min()
    last_tx_time = df[is_tx_mask & is_tx_finish]["ts"].min()
    # mark whether each message ocurred before or after the node stopped
    stopping_mask = df["status"].str.contains("stopping")
    first_stop_ts = df[stopping_mask]["ts"].iloc[0]
    df["before stop"] = np.where(df["ts"] < first_stop_ts,
                                 True,
                                 False)
    # Calculate total seconds for each uid
    df["uid start"] = df["uid"].map(df.groupby("uid")["ts"].min())
    df["uid end"] = df["uid"].map(df.groupby("uid")["ts"].max())
    df["uid turnaround"] = (df["uid end"] - df["uid start"]).dt.microseconds
    # Keep only completed purchases
    finished_purchase_mask = df["status"].str.contains("purchased")
    df = df[finished_purchase_mask]
    df["num items purchased"] = df["status"].apply(lambda x: x.split(" ")[-2])
    # Graph distribution of microseconds per uid before and after the node shut down
    fig = px.box(df, x="before stop", y="uid turnaround")
    fig.update_layout(title="Microseconds Per Purchase")
    fig.update_xaxes(title="Purchase finished before node stopped?")
    fig.update_yaxes(title="Microseconds")
    fig.show()
    # Graph scatterplot of uids with x=ts, y=microseconds.
    fig = px.scatter(df, x="ts", y="uid turnaround")
    fig.add_vline(x=first_stop_ts)
    fig.show()
    # calculate throughputs
    pre_stop_ms = (first_stop_ts - first_tx_time).microseconds
    post_stop_ms = (last_tx_time - first_stop_ts).microseconds
    pre_stop_tput = df[df["before stop"]]["num items purchased"].astype(int).sum() / pre_stop_ms
    post_stop_tput = df[~df["before stop"]]["num items purchased"].astype(int).sum() / post_stop_ms
    return


if __name__ == "__main__":
    #generate_output(caching=True)
    
    #warehouse_throughout()
    #overselling_rate()
    fault_analysis()



