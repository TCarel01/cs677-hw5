import warehouse_node
import socket
from multiprocessing import Process
import enums
import uuid
import pickle
from concurrent.futures.thread import ThreadPoolExecutor
import time
import unittest
import p2p_node as p2p
import main






def test_correct_leaders_elected(num_nodes:int, num_traders:int):
    """
    Test that the number of leaders elected is equal to num_nodes,
    and that it is the leaders with the highest IDs that are elected.
    """
    nodes = main.make_random_network(num_nodes=num_nodes, start_port=49153,
                                     num_traders=num_traders, synchronous=True)
    main.run_network(nodes, run_time=100, stop_network=True)
    return

def test_successful_purchases():
    node_dict = {
            0: dict(
                port=49152, is_buyer=True, is_seller=False, is_leader=False,
                shopping_list=["BOAR"]*100, selling_list=None,
            ),
            1: dict(
                port=49153, is_buyer=True, is_seller=False, is_leader=False,
                shopping_list=["SALT"]*100, selling_list=None,
            ),
            2: dict(
                port=49154, is_buyer=False, is_seller=True, is_leader=False,
                shopping_list=None, selling_list=["BOAR"]*100,
            ),
            3: dict(
                port=49155, is_buyer=False, is_seller=True, is_leader=False,
                shopping_list=None, selling_list=["SALT"]*100,
            ),
            4: dict(
                port=49155, is_buyer=False, is_seller=True, is_leader=True,
                shopping_list=None, selling_list=None,
            ),
        }
    return


if __name__ == "__main__":
    test_correct_leaders_elected(num_nodes=10, num_traders=5)


