import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import hw5_testing
import p2p_node as p2p
import main
import warehouse_node
from pathlib import Path


def dict_to_network(node_dict: dict, warehouse_id: int, warehouse_port: int, num_traders: int, synchronous:bool, time_to_die:int):
    """
    This function convers a dictionary of nodes to a list of nodes, such that
    we can easily define different networks for our benchmarking.
    """
    nodes = dict()
    # Iterate through each node and spawn the needed P2PNode object
    # based on the passed dictionary.
    network_dict = {nid: node_dict[nid]["port"] for nid in node_dict.keys()}
    for nid in node_dict.keys():
        port = node_dict[nid]["port"]
        # Copy the dict so we can delete the current node from it (as the current
        # node should never send messages to itself via a socket)
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
                        # Only set a trader to simulate a fault if
                        # the passed time_to_die is greater than 0, otherwise
                        # set the trader not to fault.
                        leader_time_to_die=(datetime.now() + timedelta(0, time_to_die) if ( nid == max(list(node_dict.keys())) and time_to_die > 0 ) else None)
                        )
        nodes[nid] = n
    # We also need to spawn a warehouse.
    wh_node = warehouse_node.Warehouse(id=warehouse_id,
                                       port=warehouse_port,
                                       nodes=network_dict,
                                       synchronous=synchronous)
    nodes[warehouse_id] = wh_node
    return nodes

def read_data():
    """
    This function reads in all the log files as CSVs,
    which is possible thanks to how we structured our print statements.
    We then concatenate them into a single df, such that our benchmarks
    can easily calculate throughputs and fault rates.
    """
    log_dir = Path.cwd() / "logs"
    columns = ["ts", "uid", "status"]
    df_list = []
    # Iterate through all files in log dir and read them in to a dataframe.
    for lf in log_dir.glob("*.txt"):
        df_list.append(pd.read_csv(lf, names=columns))
    # Join the dataframes, format the ts column to be a datetime, and return.
    df = pd.concat(df_list)
    df["ts"] = pd.to_datetime(df["ts"])
    return df

def calc_throughput():
    """
    Calculates throughput (num items sold per second).
    """
    df = read_data()
    # Calculate runtime
    runtime = (df["ts"].max() - df["ts"].min()).seconds
    # Calculate total items sold
    df["completed purchase"] = df["status"].str.contains("purchased")
    df = df[df["completed purchase"]]
    df["quantity"] = df["status"].str.split().str.slice(-2, -1).apply(lambda x: x[0]).astype(int)
    total = df["quantity"].sum()
    # Calculate throughput (items sold per seconds)
    throughput = total / runtime
    print(f"Throughput: {throughput}")
    return

def calc_oversell_rate():
    """
    Calculate oversell rate (num oversells divided by total num BUY requests).
    """
    df = read_data()
    # Get df of just the oversells
    failed_buy_mask = df["status"].str.contains("depleted")
    passed_cache_mask = ~df["status"].str.contains("expected")
    oversell_mask = failed_buy_mask & passed_cache_mask
    oversell_df = df[oversell_mask]
    # Get df of all starting purchases
    start_buy_mask = df["status"].str.contains("is buying")
    start_buy_df = df[start_buy_mask]
    # Calculate oversell rate
    oversell_rate = oversell_df.shape[0] / start_buy_df.shape[0]
    print(f"Overselling rate is {oversell_rate}")
    return

def calc_fault_throughout():
    """
    This function calculates throughput before and
    after a fault is simulated.
    It also generates some graphs (evaluating the wait time of each
    transaction) to go with this analysis.
    Note that if no simulated fault is detected in the output,
    this function will return
    without performing any calculations or generating any graphs.
    """
    # Read in data and sort by time
    df = read_data()
    df = df.sort_values("ts").reset_index(drop=True)
    # get time of first and last (finished) transaction message
    is_tx_mask = (df["uid"].str.len() == 37)
    is_restock_finish = (df["status"].str.contains("restocked"))
    is_buy_finish = (df["status"].str.contains("purchased"))
    is_tx_finish = is_restock_finish | is_buy_finish
    first_tx_time = df[is_tx_mask]["ts"].min()
    last_tx_time = df[is_tx_mask & is_tx_finish]["ts"].min()
    # Find where the node simulated a fault. If no fault is found, we'll skip this analysis
    stopping_mask = df["status"].str.contains("fault")
    if stopping_mask.sum() == 0:
        print("No simulated fault detected in output, so no fault metrics calculated.")
    else:
        first_stop_ts = df[stopping_mask]["ts"].iloc[0]
        df["before stop"] = np.where(df["ts"] < first_stop_ts,
                                    True,
                                    False)
        # Calculate total seconds for each transaction
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
        fig = px.scatter(df, x="ts", y="uid turnaround", title="Microseconds per BUY")
        fig.add_vline(x=first_stop_ts)
        fig.update_xaxes(title="Timestamp of BUY Ending")
        fig.update_yaxes(title="Total Microseconds From Start to End of BUY")
        fig.show()
        # calculate throughputs
        pre_stop_ms = (first_stop_ts - first_tx_time).microseconds
        post_stop_ms = (last_tx_time - first_stop_ts).microseconds
        pre_stop_s = (first_stop_ts - first_tx_time).seconds
        post_stop_s = (last_tx_time - first_stop_ts).seconds
        pre_stop_tput = df[df["before stop"]]["num items purchased"].astype(int).sum() / (pre_stop_s)
        post_stop_tput = df[~df["before stop"]]["num items purchased"].astype(int).sum() / (post_stop_s)
        print(f"Average throughput before trader stopped: {pre_stop_tput}")
        print(f"Average throughput after trader stopped: {post_stop_tput}")
    return


if __name__ == "__main__":
    # Decide whether to run the network (run_program = True) or benchmarking (run_program = False)
    run_network = False
    # Configure parameters for running the network.
    num_traders = 2
    num_buyers = 10
    num_sellers = 10
    use_caching_version = False
    time_to_die = 150
    runtime = 300
    # Run the program. Output will be saved to the logs folder.
    if run_network:
            # Define desired node network via a dict, and
            # then convert that to a list of nodes.
            node_dict = {nid: dict(
                port=49152+nid,
                is_buyer=nid<=num_buyers, is_seller=nid>num_buyers,
                shopping_list=None, selling_list=None
                ) for nid in range(1, num_buyers+num_sellers+num_traders+1)}
            nodes = dict_to_network(node_dict=node_dict, warehouse_id=0, warehouse_port=49152,
                                    num_traders=num_traders,
                                    synchronous=not use_caching_version,
                                    time_to_die=time_to_die)
            main.run_network(nodes, run_time=runtime, stop_network=True)
    else:
        # Calculate benchmarking metrics
        calc_throughput()
        calc_oversell_rate()
        calc_fault_throughout()



