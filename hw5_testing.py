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
    """
    Sets up a network where nodes want to purchase and sell salt and boar
    Monitor to verify purchases are successful.
    """
    node_list = [
            dict(
                is_buyer=True, is_seller=False, shopping_list=["BOAR"]*100, selling_list=None,
            ),
            dict(
                is_buyer=True, is_seller=False, shopping_list=["SALT"]*100, selling_list=None,
            ),
            dict(
                is_buyer=False, is_seller=True, shopping_list=None, selling_list=["BOAR"]*100,
            ),
            dict(
                is_buyer=False, is_seller=True, shopping_list=None, selling_list=["SALT"]*100,
            ),
            dict(
                is_buyer=False, is_seller=True, shopping_list=None, selling_list=None,
            ),
        dict(
            is_buyer=False, is_seller=True, shopping_list=None, selling_list=None,
        ),
        ]

    network = main.list_to_network(node_list, 2,49153)
    main.run_network(network=network, run_time=60)
    return

def test_no_sellers_async():
    """
    Test that no sellers results in every attempt to buy fails
    Not synchronous model, requests never reach the warehouse
    """
    node_list = [
        dict(
            is_buyer=True, is_seller=False, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["SALT"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["FISH"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
    ]

    network = main.list_to_network(node_list, num_traders=2, start_port=49153,)
    main.run_network(network=network, run_time=60)
    return

def test_no_sellers_sync():
    """
    Test that no sellers results in every attempt to buy fails
    Synchronous model, requests should reach the warehouse
    """
    node_list = [
        dict(
            is_buyer=True, is_seller=False, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["SALT"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["FISH"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
    ]

    network = main.list_to_network(node_list, num_traders=2, start_port=49153, synchronous=True)
    main.run_network(network=network, run_time=60)
    return

def test_no_buyers():
    """
    Test that no sellers results in the network consistently restocking items but no purchases occur
    """
    node_list = [
        dict(
            is_buyer=False, is_seller=True, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=False, is_seller=True, shopping_list=["SALT"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=False, is_seller=True, shopping_list=["FISH"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=False, is_seller=True, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=False, is_seller=True, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
        dict(
            is_buyer=True, is_seller=False, shopping_list=["BOAR"] * 100, selling_list=None,
        ),
    ]

    network = main.list_to_network(node_list, num_traders=2, start_port=49153)
    main.run_network(network=network, run_time=60)
    return

def test_15_nodes():
    """
    Test that a network with 15 nodes still continues to function as intended, have 3 nodes designated as traders
    """
    network = main.make_random_network(15, 49153, num_traders=3)
    main.run_network(network=network, run_time=100)

def test_fault_tolerance():
    """
    Set up system in a way where requests under fault tolerance are easily trackable
    Verify that fault tolerance is correctly handled
    """
    network = main.make_random_network(6, 49153, num_traders=2, fault_tolerance=True)
    main.run_network(network, run_time=100)



if __name__ == "__main__":
    test_case = 7
    match test_case:
        case 1:
            test_correct_leaders_elected(num_nodes=10, num_traders=5)
        case 2:
            test_successful_purchases()
        case 3:
            test_no_sellers_async()
        case 4:
            test_no_sellers_sync()
        case 5:
            test_no_buyers()
        case 6:
            test_15_nodes()
        case 7:
            test_fault_tolerance()


