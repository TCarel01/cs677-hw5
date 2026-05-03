import copy
import json
import math
import pickle
import random
import socket
from xmlrpc.client import DateTime

import select
import time
import threading
from typing import Dict
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta
from datetime import datetime
from enum import Enum
from pathlib import Path
import numpy as np
import pandas as pd
from pandas.core.interchange.dataframe_protocol import DataFrame
import enums
import warehouse_node
#import debugpy

# Make sure threads fail loudly
def custom_hook(args):
    print(f"Thread failed: {args.exc_type.__name__}: {args.exc_value}")
threading.excepthook = custom_hook



class P2PNode:
    def __init__(self, id: int, port_number: int,
                 is_buyer: bool, is_seller: bool,
                 nodes: Dict[int, int],  # keys are IDs, vals are ports
                 warehouse_port: int,  # Just the port
                 num_traders: int,
                 shopping_list: list[Dict] | None = None,
                 selling_list: list[Dict] | None = None,
                 synchronized:bool = False,
                 leader_time_to_die=None
                 ):
        """
        Initializes node by recording whether each node is a buyer or a seller,
        and a list of neighbors (all nodes in network). Also sets up
        some data structures we'll need.
        """
        # Fixed details about this node
        self.id = id
        self.port_number = port_number
        self.server_socket = socket.socket()
        self.num_traders = num_traders
        # Needed for running logic
        self.running = None
        self.tx_lock = None  # This lock is ONLY used when we change to self.tx. Should be locked for 1 line of code at a time.
        self.tx = list()
        # Needed for election logic
        self.is_electing = True  # Used to keep us in election loop on startups, but not when a trader goes down.
        self.election_epoch = 0  # Incremented when declaring IWON, or receiving IWON
        self.last_ELECT_epoch = -1
        self.last_ELECT_ts = datetime.now()
        self.is_up_for_election = False
        # Role details
        self.is_buyer = is_buyer
        self.is_seller = is_seller
        self.leader_time_to_die = leader_time_to_die
        # Leader specific attributes
        self.next_heartbeat_timestamp = datetime.now() + timedelta(0, 30)
        self.stop_messages_timestamp = datetime.now() + timedelta(0, 55)

        # Further peer attributes
        self.next_buy_ts = datetime.now() + timedelta(0, random.randint(1, 10))  # days, seconds
        self.next_restock_ts = datetime.now() + timedelta(0, random.randint(12, 15))  # days, seconds
        self.restock_qty = 20 # amount of items a peer stocks the warehouse with. Set to a consistent number
        self.synchronized = synchronized
        if is_buyer and is_seller:
            raise Exception  # This shouldn't happen, per assignment instructions
        self.is_leader = False
        self.replicated_totals = {enums.Item.SALT.name: 0, enums.Item.BOAR.name: 0, enums.Item.FISH.name: 0}
        self.heartbeat_confirmation = {}
        self.num_restocks = {enums.Item.SALT.name: 0, enums.Item.BOAR.name: 0, enums.Item.FISH.name: 0}
        self.resend_peer_log = pd.DataFrame(
            columns=["uid", "sender", "type", "item", "quantity"])
        # Network details
        self.nodes = nodes
        self.warehouse_port = warehouse_port
        self.traders = dict()
        self.server_socket = socket.socket()
        # Only used by traders, and only when using cache
        self.locks = dict()
        self.inv = dict(
            SALT = 0,
            BOAR = 0,
            FISH = 0,
        )
        # testing attributes
        self.shopping_list = shopping_list
        self.selling_list = selling_list

    def start(self):
        """
        Called by parent process to start node running.
        Sets self.running to True, sets up socket, and starts run loop.
        """
        print(f"{datetime.now()}, status, node {self.id} starting using port {self.port_number}")
        # Set up server socket
        self.server_socket = socket.socket()
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(100)  # Time out so we can gracefully exit once we stop seeing messages.
        self.server_socket.bind((socket.gethostname(), self.port_number))
        self.server_socket.listen(100000)
        # Set up lock (only have one for simplicity, if adding a second one be VERY careful)
        self.tx_lock = threading.Lock()
        # Locks which will only be acquired in a testing environment
        self.locks["SELLING_LIST"] = threading.Lock()
        self.locks["SHOPPING_LIST"] = threading.Lock()
        self.locks["REPLICATED_LOCK"] = threading.Lock()
        self.locks["RESEND_LOG_LOCK"] = threading.Lock()
        self.locks["AVOID_SENDING_REQUESTS"] = threading.Lock()

        # Start run loop (after waiting 5 seconds so all nodes are online)
        time.sleep(5)
        self.running = True
        self.run_loop()
        return
    
    def stop(self):
        """
        Called when we receive stop message from parent process.
        Sets self.running to False, which will end our run loop.
        Closes server socket.
        """
        print(f"{datetime.now()}, status, node {self.id} stopping")
        self.running = False
        self.server_socket.close()
        time.sleep(5)
        return
    
    def run_loop(self):
        """
        Three states the node can be in while running.
        Electing: Only at startup, when we don't have enough leaders.
        Trading: If the node is a leader.
        Client: If the node if not a leader but we have enough leaders.
        """
        while self.running:
            self.listen()
            if self.is_electing:
                self.election_logic()
            elif self.is_leader:
                self.leader_logic()
            elif not self.is_leader:
                self.client_logic()
            else:
                raise Exception
        return
    
    def listen(self):
        """
        Listen for msgs and spawn threads to deal with each.
        Only exception to this is STOP, which we handle here so we can break out of loop.
        """
        # Open thread executor, and enter listening loop
        with ThreadPoolExecutor(max_workers=100) as executor:
            # We will continue this loop until there are no incoming messages.
            while self.running and select.select([self.server_socket], [], [], 0.1)[0]:
                socket_connection, addr = self.server_socket.accept()
                data = socket_connection.recv(4096)
                socket_connection.close()
                msg = pickle.loads(data)
                if msg["type"] == enums.ControlMsgType.STOP.name:
                    self.stop()
                    break
                elif msg["type"] in [m.name for m in enums.ElecMsgType]:
                    # Handle election messages one at a time, as we gain little
                    # from handling these concurrently (since they come one at a time
                    # anyways) and it would come at the expense of much complexity (locking)
                    self.handle_msg(msg)
                else:
                    executor.submit(self.handle_msg, msg)
                if self.leader_time_to_die is not None and datetime.now() > self.leader_time_to_die:
                    self.stop()
        return
    
    def handle_msg(self, msg:dict):
        """
        Parse msg type and pass to appropriate function
        """
        match msg["type"]:
            case enums.ElecMsgType.ELECT.name:
                self.okay(msg)
            case enums.ElecMsgType.OKAY.name:
                if msg["epoch"] == self.election_epoch:
                    self.is_up_for_election = False
            case enums.ElecMsgType.IWON.name:
                self.youwon(msg)
            # Entered when a leader requests a restock, forward the message to the warehouse
            case enums.MsgType.RESTOCK.name:
                self.forward_transaction(msg)
            # Entered when a note requests to purchase an item, forward the message to the warehouse
            case enums.MsgType.BUY.name:
                forward_transaction = True
                if not self.synchronized:
                    self.locks["REPLICATED_LOCK"].acquire()
                    forward_transaction = self.replicated_totals[msg["item"]] != 0
                    self.locks["REPLICATED_LOCK"].release()
                if forward_transaction:
                    self.forward_transaction(msg)
                else:
                    self.reject_purchase_insufficient_cache(msg)
            # Entered when the warehouse sends a response indicating the buy went through.
            case enums.MsgType.BUY_REPLY.name:
                if self.is_leader:
                    self.peer_buy_reply(msg)
                else:
                    if msg["quantity"] == 0:
                        if msg["passed_cache"]:
                            print(f"{datetime.now()}, {msg['uid']}, made by node {self.id} to buy {msg['item']} failed. Inventory for {msg['item']} depleted.")
                        else:
                            print(
                                f"{datetime.now()}. {msg['uid']}, made by node {self.id} to buy {msg['item']} failed. Inventory for {msg['item']} expected to be depleted.")
                    else:
                        print(
                            f"{datetime.now()}, {msg['uid']}, succeeded. Node {self.id} purchased {msg['quantity']} {msg['item']}")
            case enums.MsgType.RESTOCK_REPLY.name:
                self.locks["REPLICATED_LOCK"].acquire()
                self.replicated_totals[msg["item"]] += msg["quantity"]
                self.num_restocks[msg["item"]] += 1
                self.locks["REPLICATED_LOCK"].release()
                if msg["is_original_leader"]:
                    print(f"{datetime.now()}, {msg['uid']}, made by node {msg['peer_id']} succeeded. Inventory restocked with {msg['quantity']} {msg['item']}")
            case enums.MsgType.SYNC_DATA.name:
                self.locks["REPLICATED_LOCK"].acquire()
                self.replicated_totals = msg["totals"]
                self.locks["REPLICATED_LOCK"].release()
            case enums.MsgType.HEARTBEAT.name:
                self.handle_heartbeat(msg)
            case enums.MsgType.HEARTBEAT_REPLY.name:
                self.handle_heartbeat_reply(msg)
            case enums.MsgType.LEADER_DOWN.name:
                self.handle_leader_removal(msg)



        return

    def buy(self):
        """
        Request made by peer when attempting to purchase an item
        """
        uid = uuid.uuid4()
        if self.shopping_list is not None and len(self.shopping_list) > 0:
            self.locks["SHOPPING_LIST"].acquire()
            item_quantity_dict = self.shopping_list.pop()
            item = list(item_quantity_dict.keys())[0]
            quantity = item_quantity_dict[item]
            self.locks["SHOPPING_LIST"].release()
        else:
            item = random.choice(list(enums.Item)).name
            quantity = random.choice(range(1, 10))

        print(f"{datetime.now()}, {uid}, node {self.id} is buying {item}")
        outgoing_msg = enums.TxMsg(uid=uid,
                                   sender=self.id,
                                   type=enums.MsgType.BUY.name,
                                   item=item,
                                   quantity=quantity).to_dict()

        chosen_trader = random.choice(list(self.traders.keys()))
        # try catch block, for fault tolerance implementation pater
        self.locks["AVOID_SENDING_REQUESTS"].acquire()
        self.locks["AVOID_SENDING_REQUESTS"].release()
        try:
            self.send_msg(outgoing_msg, chosen_trader)
        except:
            # if we've entered here, one of our leaders has failed. Remove leader from leader set and pick another
            print(f"{datetime.now()}, {uid}. Node {self.id} failed to reach leader when purchasing. Queueing request to purchase {item}")
            self.append_to_resend_log(uid, self.id, enums.MsgType.BUY.name, item, quantity)
        finally:
            self.next_buy_ts = datetime.now() + timedelta(0, 10)
        return


    def restock(self):
        """
        Request made by peer when attempting to restock the warehouse
        """
        uid = uuid.uuid4()
        # Either pick random item and quantity, or get them from the selling list
        if self.selling_list is not None and len(self.selling_list) > 0:
            self.locks["SELLING_LIST"].acquire()
            item_quantity_dict = self.selling_list.pop()
            item = list(item_quantity_dict.keys())[0]
            self.locks["SELLING_LIST"].release()
        else:
            item = random.choice(list(enums.Item)).name
        print(f"{datetime.now()}, {uid}, node {self.id} is restocking {item}")

        # Send request to the warehouse node
        outgoing_msg = enums.TxMsg(uid=uid,
                                   sender=self.id,
                                   type=enums.MsgType.RESTOCK.name,
                                   item=item,
                                   quantity=self.restock_qty).to_dict()

        chosen_trader = random.choice(list(self.traders.keys()))
        # try catch block, for fault tolerance implementation pater
        self.locks["AVOID_SENDING_REQUESTS"].acquire()
        self.locks["AVOID_SENDING_REQUESTS"].release()
        try:
            self.send_msg(outgoing_msg, chosen_trader)
        except:
            print(f"{datetime.now()}, {uid} Node {self.id} failed to reach leader when restocking. Queueing request to be resent.")
            self.append_to_resend_log(uid, self.id, enums.MsgType.RESTOCK.name, item, self.restock_qty)
        finally:
            self.next_restock_ts = datetime.now() + timedelta(0, 20)
        return

    def forward_transaction(self, msg:dict):
        """
        To be called by leaders only, forwards the transaction request message to the warehouse
        """
        # FIXME implement checking local cache for requests
        msg["passed_cache"] = True
        msg["peer_id"] = msg["sender"]
        msg["sender"] = self.id
        self.send_msg(msg, self.warehouse_port, True)

    def peer_buy_reply(self, msg:dict):
        self.locks["REPLICATED_LOCK"].acquire()
        if not msg["print_message"]:
            self.replicated_totals[msg["item"]] -= msg["quantity"]
        self.locks["REPLICATED_LOCK"].release()
        if msg["is_original_leader"] or msg["print_message"]:
            peer_id = msg["peer_id"]
            msg["sender"] = self.id
            self.send_msg(msg, peer_id, False)

    def reject_purchase_insufficient_cache(self, msg:dict):
        msg["quantity"] = 0
        peer = msg["sender"]
        msg["sender"] = self.id
        msg["type"] = enums.MsgType.BUY_REPLY.name
        self.send_msg(msg, peer, False)

    def append_to_resend_log(self, uid, sender, type, item, quantity):
        """
        Add entry to requests to be resent.
        """
        self.locks["RESEND_LOG_LOCK"].acquire()
        log_entry = dict(
            uid = uid,
            sender = sender,
            type = type,
            item = item,
            quantity = quantity
        )
        try:
            if uid not in self.resend_peer_log["uid"].to_list():
                # Only add the entry if it's not already in the log.
                self.resend_peer_log.loc[len(self.resend_peer_log)] = log_entry
        except Exception as e:
            print(e)
            raise Exception
        self.locks["RESEND_LOG_LOCK"].release()
        return

    def send_heartbeat_request(self):
        for node_id in self.traders.keys():
            heartbeat_dict = dict(
                type = enums.MsgType.HEARTBEAT.name,
                sender = self.id
            )
            try:
                self.send_msg(heartbeat_dict, node_id, False)
            except:
                continue
        return

    def handle_heartbeat(self, msg:dict):
        reply_msg = dict(
            type = enums.MsgType.HEARTBEAT_REPLY.name,
            sender = self.id
        )
        try:
            self.send_msg(reply_msg, msg["sender"], False)
        except:
            return

    def handle_heartbeat_reply(self, msg:dict):
        self.heartbeat_confirmation[msg["sender"]] = True

    def send_leader_down(self, leaders:list):
        msg = dict(
            type=enums.MsgType.LEADER_DOWN.name,
            sender = self.id,
            leaders = leaders
        )
        # FIXME: maybe doesn't need fixing in multiple leaders going down case, but assignment specs
        # only require fault tolerance in the case of two leaders so should be fine
        # loop over set difference of nodes and leaders
        for cur_node_id in self.nodes:
            try:
                self.send_msg(msg, cur_node_id, False)
            except:
                continue
        self.send_msg(msg, -1, True)

    def handle_leader_removal(self, msg:dict):
        leaders = msg["leaders"]
        for leader in leaders:
            self.traders.pop(leader, -1)
            self.nodes.pop(leader, -1)
        self.locks["AVOID_SENDING_REQUESTS"].acquire()
        self.locks["RESEND_LOG_LOCK"].acquire()
        for i, request in self.resend_peer_log.iterrows():
            msg=dict(
                uid=request["uid"],
                sender=request["sender"],
                type=request["type"],
                item=request["item"],
                quantity=request["quantity"]
            )
            try:
                chosen_sent_leader = random.choice(list(self.traders.keys()))
                print(f"{datetime.now()}, {msg['uid']}, Retrying request of type {msg['type']} for {msg['quantity']} {msg['item']}")
                self.send_msg(msg, chosen_sent_leader, False)
            except:
                continue
        self.locks["RESEND_LOG_LOCK"].release()
        self.locks["AVOID_SENDING_REQUESTS"].release()




    def election_logic(self):
        """
        When we start, elect nodes as trader.
        """
        if self.election_epoch < self.num_traders:
            # Start elections until we have enough traders
            if self.last_ELECT_epoch < self.election_epoch:
                # Only start an election when we enter a new epoch (after a leader is found)
                # and if we're not already a leader
                if not self.is_leader:
                    self.elect()
                self.last_ELECT_epoch += 1
            elif self.is_up_for_election and datetime.now() > self.last_ELECT_ts + timedelta(0, 5):
                # If we've started an election and haven't gotten an OKAY for five seconds, declare victory
                self.iwon()
        else:
            # Once we have enough traders, we're done with electing phase
            self.is_electing = False
            time.sleep(1)
        return
    
    def iwon(self):
        """
        Send out iwon message.
        """
        print(f"{datetime.now()}, election, node {self.id} won election in epoch {self.election_epoch}")
        uid = uuid.uuid4()
        msg = enums.ElectMsg(uid=uid, sender=self.id, type=enums.ElecMsgType.IWON.name, epoch=self.election_epoch)
        self.election_epoch += 1
        for nid in self.nodes.keys():
            self.send_msg(msg.to_dict(), nid)
        self.send_msg(msg.to_dict(), -1, send_to_warehouse=True)
        self.is_leader = True
        self.is_up_for_election = False
        #self.traders[self.id] = self.nodes[self.id]  # Structuring it this way let's us avoid locks on self.traders
        return

    def elect(self):
        """
        Send out elect msgs.
        """
        print(f"{datetime.now()}, election, node {self.id} starting election in epoch {self.election_epoch}")
        uid = uuid.uuid4()
        msg = enums.ElectMsg(uid=uid, sender=self.id, type=enums.ElecMsgType.ELECT.name, epoch=self.election_epoch).to_dict()
        for nid in self.nodes.keys():
            if nid > self.id:
                self.send_msg(msg=msg, dest=nid, send_to_warehouse=False)
        self.is_up_for_election = True
        self.last_ELECT_ts = datetime.now()
        return
    
    def okay(self, msg):
        """
        Send out okay msg if needed.
        """
        if msg["epoch"] < self.election_epoch:
            # If this message is from an old epoch, we alreayd have a winner for that eleciton, so we send an OKAY
            reply_msg = enums.ElectMsg(uid=msg["uid"], sender=self.id, type=enums.ElecMsgType.OKAY.name, epoch=msg["epoch"]).to_dict()
            self.send_msg(msg=reply_msg, dest=msg["sender"], send_to_warehouse=False)
        elif not self.is_leader:
            # If we're not a leader, send an OKAY
            reply_msg = enums.ElectMsg(uid=msg["uid"], sender=self.id, type=enums.ElecMsgType.OKAY.name, epoch=msg["epoch"]).to_dict()
            self.send_msg(msg=reply_msg, dest=msg["sender"], send_to_warehouse=False)
        else:
            # Otherwise, we pass
            pass
        return
    
    def youwon(self, msg):
        """
        Called when we recieve IWON msg.
        """
        sender = msg["sender"]
        self.traders[sender] = self.nodes[sender]  # Structuring it this way let's us avoid locks on self.traders
        self.heartbeat_confirmation[sender] = True
        self.election_epoch += 1
        return
    
    def leader_logic(self):
        """
        Logic that leaders go through in run_loop.
        """
        if datetime.now() > self.next_heartbeat_timestamp:
            if all(value is True for value in self.heartbeat_confirmation.values()):
                self.heartbeat_confirmation = {key: False for key in self.heartbeat_confirmation}
                self.send_heartbeat_request()
                self.next_heartbeat_timestamp = self.next_heartbeat_timestamp + timedelta(0, 10)
            else:
                false_vals = [e for e in self.heartbeat_confirmation.keys() if self.heartbeat_confirmation[e] is False]
                self.heartbeat_confirmation = {key: self.heartbeat_confirmation[key]
                                               for key in self.heartbeat_confirmation.keys()
                                               if self.heartbeat_confirmation[key] is True}
                for val in false_vals:
                    self.heartbeat_confirmation.pop(val, -1)
                    self.traders.pop(val, -1)
                    self.nodes.pop(val, -1)
                self.send_leader_down(false_vals)
        return

    def client_logic(self):
        """
        Logic that non-leaders gro through in run_loop.
        """
        if self.is_seller:
            if self.next_restock_ts < datetime.now(): # < self.stop_messages_timestamp:
                self.restock()
        if self.is_buyer:
            if self.next_buy_ts < datetime.now(): # < self.stop_messages_timestamp:
                self.buy()
        return
    
    def send_msg(self, msg:dict, dest:int, send_to_warehouse:bool=False):
        """
        Sends outgoing msgs to dest.
        We also need to handle fault tolerance in case a trader goes
        down.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
                dest_port = self.nodes[dest] if not send_to_warehouse else self.warehouse_port
                node_socket.connect((socket.gethostname(), dest_port))
                serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
                node_socket.sendall(serialized_msg)
        finally:
            node_socket.close()
        return

