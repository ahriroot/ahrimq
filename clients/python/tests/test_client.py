import threading
import time
import unittest

from ahrimq.client import Ahrimq
from ahrimq.config import Config
from ahrimq.message import *

from .server import ConnectionHandler, ThreadedTCPServer


class TestClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """启动测试服务器"""
        server = ThreadedTCPServer(('localhost', 0), ConnectionHandler)
        ip, port = server.server_address
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()

        cls.server_port = port
        cls.server = server
        cls.server_thread = server_thread
        print(f"Server running on {ip}:{port}")

    @classmethod
    def tearDownClass(cls):
        """关闭测试服务器"""
        cls.server.shutdown()
        cls.server_thread.join()

    def test_access_invalid_key_secret(self):
        with self.assertRaises(ValueError):
            amq = Ahrimq(Config(host="localhost", port=self.server_port))
            try:
                amq.connect()
            finally:
                amq.close()

    def test_connect(self):
        def callback(msg):
            self.assertEqual(msg, b"message")

        amq = Ahrimq(
            Config(
                host="localhost",
                port=self.server_port,
                access_key="test",
                access_secret="test"
            )
        )
        try:
            amq.connect()
            amq.subscribe("test_topic", callback)

            amq.publish("test_topic", b"hello world")

            time.sleep(1)

            amq.unsubscribe("test_topic")
        finally:
            amq.close()
            time.sleep(1)

    def test_connect_with_default_config(self):
        amq = Ahrimq()
        try:
            self.assertIsNotNone(amq.config)
            with self.assertRaises(ValueError):
                amq.connect()
        finally:
            amq.close()

    def test_receive_withou_header(self):
        """测试接收数据时连接中断"""
        amq = Ahrimq(
            Config(
                host="localhost",
                port=self.server_port,
                access_key="close",
                access_secret="close",
            )
        )
        try:
            with self.assertRaises(ConnectionError):
                amq.connect()
        finally:
            amq.close()

    def test_auth_timeout(self):
        """测试接收数据时连接中断"""
        amq = Ahrimq(
            Config(
                host="localhost",
                port=self.server_port,
                access_key="timeout",
                access_secret="timeout",
            ),
            auth_timeout=1
        )
        try:
            with self.assertRaises(TimeoutError):
                amq.connect()
        finally:
            amq.close()

    def test_auth_invalid_msg_type(self):
        """测试接收数据时连接中断"""
        amq = Ahrimq(
            Config(
                host="localhost",
                port=self.server_port,
                access_key="invalid",
                access_secret="invalid",
            )
        )
        try:
            with self.assertRaises(ValueError):
                amq.connect()
        finally:
            amq.close()
