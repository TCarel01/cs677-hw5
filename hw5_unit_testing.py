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


def send_msg(msg:dict, dest_port:int):
    """
    Pickle and send msg to the given dest_port.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
        dest_port = 49153
        time.sleep(5)
        node_socket.connect((socket.gethostname(), dest_port))
        serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
        node_socket.sendall(serialized_msg)
        node_socket.close()
    return

def recieve_msg(my_socket:socket.socket):
    """
    Recieve and unpickle msg via my_socket.
    Note that this will raise an error is no msg ever comes.
    """
    socket_connection, addr = my_socket.accept()
    data = socket_connection.recv(4096)
    socket_connection.close()
    msg = pickle.loads(data)
    return msg



class TestWarehouse(unittest.TestCase):
    """
    Class with a bunch of tests to make sure warehouse logic works as expected.
    Does this by controlling a trader port, and using it to send/recieve msgs
    to/from warehouse to make sure we get expected replies.
    """
    def setUp(self):
        """
        Start up warehouse and create port
        for it to talk to.
        """
        # Set up warehouse node
        self.test_id = 0
        self.test_port = 49152
        self.wh_port = 49153
        wh_node = warehouse_node.Warehouse(id=1,
                                           port=self.wh_port,
                                           nodes={self.test_id: self.test_port},
                                           synchronous=True)
        # Set up test socket for warehouse node to send msgs to
        self.socket = socket.socket()
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.settimeout(10)  # Time out so we can gracefully exit once we stop seeing messages.
        self.socket.bind((socket.gethostname(), self.test_port))
        self.socket.listen(100000)
        # set up either process or thread testing (thread testing useful for debugging,
        # otehrwise we run warehouse in its own process.)
        use_process = True            
        self.thread_executor = ThreadPoolExecutor(max_workers=100)
        if use_process:
            p = Process(target=wh_node.start,)
            p.start()
        else:
            self.thread_executor.submit(wh_node.start)
        # Wait a few seconds so warehouse has time to start
        time.sleep(20)
        # Send IWON msg to warehouse so it knows we're the leader
        iwon_msg = enums.ElectMsg(uid=uuid.uuid4(), sender=self.test_id, type=enums.ElecMsgType.IWON.name, epoch=0).to_dict()
        send_msg(iwon_msg, dest_port=self.wh_port)
        return
    
    def tearDown(self):
        """
        Stop warehouse node and close socket.
        Also shut down thread executor.
        """
        stop_msg = dict(type=enums.ControlMsgType.STOP.name)
        send_msg(stop_msg, dest_port=self.wh_port)
        self.socket.close()
        self.thread_executor.shutdown()
        return
    
    def send_rcv_msg(self, uid, type, item, quantity):
        """
        Send either a BUY or RESTOCK msg to warehouse and get reply back.
        """
        # Send request to the warehouse node
        outgoing_msg = enums.TxMsg(uid=uid,
                                   sender=self.test_id,
                                   type=type,
                                   item=item,
                                   quantity=quantity).to_dict()
        send_msg(outgoing_msg, dest_port=self.wh_port)
        # Receive reply from warehouse
        msg = recieve_msg(self.socket)
        return msg
    
    def test_buy(self):
        """
        Buy 5 items.
        Expect 0 items bought, since no stock.
        """
        # Send a buy request to the warehouse node, and check we get expected reply (i.e., 0 items bought)
        uid = uuid.uuid4()
        reply = self.send_rcv_msg(uid=uid, type=enums.MsgType.BUY.name, item=enums.Item.SALT.name, quantity=5)
        reply.pop("passed_cache")
        expected_reply = dict(uid=uid,
                              sender=1,
                              type=enums.MsgType.BUY_REPLY.name,
                              item=enums.Item.SALT.name,
                              quantity=0,
                              peer_id=None,
                              is_original_leader=True,
                              is_resend=False,
                              print_message=False)
        self.assertDictEqual(reply, expected_reply)
        return
    
    def test_restock(self):
        """
        Restock 5 items.
        Expect all restock items to go through.
        """
        # Send a buy request to the warehouse node, and check we get expected reply
        uid = uuid.uuid4()
        reply = self.send_rcv_msg(uid=uid, type=enums.MsgType.RESTOCK.name, item=enums.Item.SALT.name, quantity=5)
        reply.pop("passed_cache")
        expected_reply = dict(uid=uid,
                              sender=1,
                              type=enums.MsgType.RESTOCK_REPLY.name,
                              item=enums.Item.SALT.name,
                              quantity=5,
                              peer_id=None,
                              is_original_leader=True,
                              is_resend=False,
                              print_message=False)
        self.assertDictEqual(reply, expected_reply)
        return
    
    def test_buy_full_stock(self):
        """
        Restock 5 items, then buy 5.
        Expect 5 items bought, since there should be stock.
        """
        # Send a restock request to the warehouse node
        r_uid = uuid.uuid4()
        r_reply = self.send_rcv_msg(uid=r_uid, type=enums.MsgType.RESTOCK.name, item=enums.Item.SALT.name, quantity=5)
        time.sleep(1)  # Wait so restock has time to go through
        # Send buy request
        b_uid = uuid.uuid4()
        b_reply = self.send_rcv_msg(uid=b_uid, type=enums.MsgType.BUY.name, item=enums.Item.SALT.name, quantity=5)
        b_reply.pop("passed_cache")
        expected_reply = dict(uid=b_uid,
                              sender=1,
                              type=enums.MsgType.BUY_REPLY.name,
                              item=enums.Item.SALT.name,
                              quantity=5,
                              peer_id=None,
                              is_original_leader=True,
                              is_resend=False,
                              print_message=False)
        self.assertDictEqual(b_reply, expected_reply)
        return
    
    def test_buy_more_than_stock(self):
        """
        Restock 5 items then buy 6.
        Expect 5 items bought, since that's all that's in stock.
        """
        # Send a restock request to the warehouse node
        r_uid = uuid.uuid4()
        r_reply = self.send_rcv_msg(uid=r_uid, type=enums.MsgType.RESTOCK.name, item=enums.Item.SALT.name, quantity=5)
        time.sleep(1)  # Wait so restock has time to go through
        # Send buy request
        b_uid = uuid.uuid4()
        b_reply = self.send_rcv_msg(uid=b_uid, type=enums.MsgType.BUY.name, item=enums.Item.SALT.name, quantity=6)
        b_reply.pop("passed_cache")
        expected_reply = dict(uid=b_uid,
                              sender=1,
                              type=enums.MsgType.BUY_REPLY.name,
                              item=enums.Item.SALT.name,
                              quantity=5,
                              peer_id=None,
                              is_original_leader=True,
                              is_resend=False,
                              print_message=False)
        self.assertDictEqual(b_reply, expected_reply)
        return
    
    def test_buy_less_than_stock(self):
        """
        Restock 5 items, then buy 4, then buy 1.
        Expect 1 item bought in last purchase,
        since that should still be left in stock.
        """
        # Send a restock request to the warehouse node
        r_uid = uuid.uuid4()
        r_reply = self.send_rcv_msg(uid=r_uid, type=enums.MsgType.RESTOCK.name, item=enums.Item.SALT.name, quantity=5)
        time.sleep(1)  # Wait so restock has time to go through
        # Send two buy request, first less than full stock, than full remaining.
        b1_uid = uuid.uuid4()
        b1_reply = self.send_rcv_msg(uid=b1_uid, type=enums.MsgType.BUY.name, item=enums.Item.SALT.name, quantity=4)
        b2_uid = uuid.uuid4()
        b2_reply = self.send_rcv_msg(uid=b2_uid, type=enums.MsgType.BUY.name, item=enums.Item.SALT.name, quantity=1)
        b2_reply.pop("passed_cache")
        expected_reply = dict(uid=b2_uid,
                              sender=1,
                              type=enums.MsgType.BUY_REPLY.name,
                              item=enums.Item.SALT.name,
                              quantity=1,
                              peer_id=None,
                              is_original_leader=True,
                              is_resend=False,
                              print_message=False)
        self.assertDictEqual(b2_reply, expected_reply)
        return


class TestNonCachingTrader(unittest.TestCase):
    """
    Class used to test that non-caching traders operate as expected.
    Does this by controlling node and warehouse ports, and using
    these to send/recieve messages from the trader, and check that
    these messages are as expected.
    Note that this class is inherited later to test caching traders,
    so we include some code here to support that.
    """
    synchronized = True  # Used to indicate we're in the non-caching version.
    def setUp(self):
        """
        Start up trader node and create fake node and
        warehouse ports (which the test functions control) for it to talk to.
        """
        self.node_id = 0
        self.node_port = 49152
        self.trader_id = 1
        self.trader_port = 49153
        self.warehouse_id = 2
        self.warehouse_port = 49154
        # Set up trader node. Manually set it as a leader
        trader_node = p2p.P2PNode(id=self.trader_id, port_number=self.trader_port,
                                  is_buyer=True, is_seller=False,
                                  warehouse_port=self.warehouse_port,
                                  nodes={self.node_id: self.node_port},
                                  num_traders=1, synchronized=self.synchronized)
        trader_node.is_leader = True
        trader_node.is_electing = False
        # Set up test node socket for trader node to send msgs to
        self.node_socket = socket.socket()
        self.node_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.node_socket.settimeout(10)  # Time out so we can gracefully exit once we stop seeing messages.
        self.node_socket.bind((socket.gethostname(), self.node_port))
        self.node_socket.listen(100000)
        # Set up test warehouse socket for trader node to send msgs to
        self.wh_socket = socket.socket()
        self.wh_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.wh_socket.settimeout(10)  # Time out so we can gracefully exit once we stop seeing messages.
        self.wh_socket.bind((socket.gethostname(), self.warehouse_port))
        self.wh_socket.listen(100000)
        # set up testing either using threads or processes. We only use threads
        # for debugging; otherwise, we always use processes.
        use_process = True            
        self.thread_executor = ThreadPoolExecutor(max_workers=100)
        if use_process:
            p = Process(target=trader_node.start,)
            p.start()
        else:
            self.thread_executor.submit(trader_node.start)
        # Wait a few seconds so nodes have time to start
        time.sleep(5)
        return
    
    def tearDown(self):
        """
        Stop warehouse node and close socket.
        Also shut down thread executor.
        """
        stop_msg = dict(type=enums.ControlMsgType.STOP.name)
        send_msg(stop_msg, dest_port=self.trader_port)
        time.sleep(1)  # time for trader to finish sending any outgoing messages before stopping
        self.node_socket.close()
        self.wh_socket.close()
        self.thread_executor.shutdown()
        return
    
    def node_send_msg(self, uid, type, item, quantity):
        """
        Send request from node to trader.
        """
        # Send request to the trader node
        outgoing_msg = enums.TxMsg(uid=uid,
                                   sender=self.node_id,
                                   type=type,
                                   item=item,
                                   quantity=quantity).to_dict()
        send_msg(outgoing_msg, dest_port=self.trader_id)
        return
    
    def warehouse_send_msg(self, uid, type, item, quantity):
        """
        Send message from warehouse to trader.
        """
        # Send request reply to the trader node
        outgoing_msg = enums.TxMsg(uid=uid,
                                   sender=self.warehouse_id,
                                   type=type,
                                   item=item,
                                   quantity=quantity,
                                   peer_id=self.node_id,
                                   passed_cache=True,
                                   is_original_leader=True,
                                   ).to_dict()
        send_msg(outgoing_msg, dest_port=self.trader_id)
        return
    
    def warehouse_send_cache(self, fish:int, salt:int, boar:int):
        """
        Send specific cache values from warehouse to trade.
        """
        totals = {
            "FISH": fish,
            "SALT": salt,
            "BOAR": boar,
        }
        msg = dict(
            type = enums.MsgType.SYNC_DATA.name,
            totals = totals
        )
        send_msg(msg, dest_port=self.trader_id)
    

    def test_buy_is_fwded(self):
        """
        Test that makes sure buy is always forwarded in non-caching version.
        """
        # Test that a BUY request is forwarded in non caching version.
        # If we're in caching version, we skip this test
        if not self.synchronized:
            print("test_buy_is_fwded only applies to non-caching version, so we skip it in caching version.")
        else:
            uid = uuid.uuid4()
            self.node_send_msg(uid=uid, type=enums.MsgType.BUY.name, item=enums.Item.SALT.name, quantity=5)
            reply_msg = recieve_msg(self.wh_socket)
            expected_reply = dict(uid=uid,
                                sender=self.trader_id,
                                type=enums.MsgType.BUY.name,
                                item=enums.Item.SALT.name,
                                quantity=5,
                                peer_id=self.node_id,
                                passed_cache=True,
                                is_resend=False,
                                is_original_leader=False,
                                print_message=False)
            self.assertDictEqual(reply_msg, expected_reply)
        return
    
    def test_restock_is_fwded(self):
        """
        Test that a RESTOCK request is always forwarded.
        """
        uid = uuid.uuid4()
        self.node_send_msg(uid=uid, type=enums.MsgType.RESTOCK.name, item=enums.Item.SALT.name, quantity=5)
        reply_msg = recieve_msg(self.wh_socket)
        expected_reply = dict(uid=uid,
                              sender=self.trader_id,
                              type=enums.MsgType.RESTOCK.name,
                              item=enums.Item.SALT.name,
                              quantity=5,
                              peer_id=self.node_id,
                              passed_cache=True,
                              is_original_leader=False,
                              is_resend=False,
                              print_message=False)
        self.assertDictEqual(reply_msg, expected_reply)
        return
    
    def test_reply_restock_is_forwarded(self):
        """
        Test that replies to restocks from warehouse to trader are forwarded to node.
        """
        uid = uuid.uuid4()
        self.warehouse_send_msg(uid=uid, type=enums.MsgType.RESTOCK_REPLY.name, item=enums.Item.SALT.name, quantity=5)
        reply_msg = recieve_msg(self.node_socket)
        expected_reply = dict(uid=uid,
                              sender=self.trader_id,
                              type=enums.MsgType.RESTOCK_REPLY.name,
                              item=enums.Item.SALT.name,
                              quantity=5,
                              peer_id=self.node_id,
                              passed_cache=True,
                              is_original_leader=True,
                              is_resend=False,
                              print_message=False)
        self.assertDictEqual(reply_msg, expected_reply)

    def test_reply_buy_is_forwarded(self):
        """
        Test that replies to buys from warehouse to trader are forwarded to node.
        """
        uid = uuid.uuid4()
        self.warehouse_send_msg(uid=uid, type=enums.MsgType.BUY_REPLY.name, item=enums.Item.SALT.name, quantity=5)
        reply_msg = recieve_msg(self.node_socket)
        expected_reply = dict(uid=uid,
                              sender=self.trader_id,
                              type=enums.MsgType.BUY_REPLY.name,
                              item=enums.Item.SALT.name,
                              quantity=5,
                              peer_id=self.node_id,
                              passed_cache=True,
                              is_original_leader=True,
                              is_resend=False,
                              print_message=False)
        self.assertDictEqual(reply_msg, expected_reply)
    
class TestCachingTrader(TestNonCachingTrader):
    """
    Class for testing the caching version.
    We inherit the initialization and tests use in the non caching version,
    and add an additional test here.
    """
    synchronized = False  # Overwrite class attribute so the inherited tests know how to behave.

    def test_buy_not_fwded(self):
        """
        Test that a BUY request is not forwarded when the cache has that item at 0
        """
        uid = uuid.uuid4()
        self.warehouse_send_cache(fish=0, salt=0, boar=0)
        self.node_send_msg(uid=uid, type=enums.MsgType.BUY.name, item=enums.Item.SALT.name, quantity=5)
        reply_msg = recieve_msg(self.node_socket)
        expected_reply = dict(uid=uid,
                              sender=self.trader_id,
                              type=enums.MsgType.BUY_REPLY.name,
                              item=enums.Item.SALT.name,
                              quantity=0,
                              peer_id=None,
                              passed_cache=False,
                              is_original_leader=False,
                              is_resend=False,
                              print_message=False)
        self.assertDictEqual(reply_msg, expected_reply)
        return


if __name__ == "__main__":
    #unittest.main()
    warehouse_suite = unittest.TestLoader().loadTestsFromTestCase(TestWarehouse)
    caching_trader_suite = unittest.TestLoader().loadTestsFromTestCase(TestNonCachingTrader)
    non_caching_trader_suite = unittest.TestLoader().loadTestsFromTestCase(TestCachingTrader)
    unittest.TextTestRunner().run(warehouse_suite)
    unittest.TextTestRunner().run(non_caching_trader_suite)
    unittest.TextTestRunner().run(caching_trader_suite)

