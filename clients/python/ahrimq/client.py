import select
import socket
import struct
import threading
import time
from typing import Any, Callable, Dict

from .config import Config
from .message import (
    TYPE_REQ_PING, TYPE_RESP_AUTHORIZER, TYPE_RESP_PING, TYPE_RESP_SUBSCRIBE,
    ByteArray, MsgStatus, ReqMsgAuthorizer, ReqMsgPing, ReqMsgPublish,
    ReqMsgSubscriber, ReqMsgUnsubscriber, RespMsgSubscribe, deserialize, serialize,
    ReqReconsumeDelay, RespReconsumeDelay
)


class Ahrimq:
    def __init__(self, config: Config | None = None, auth_timeout: int = 5) -> None:
        self.config = config
        if self.config is None:
            self.config = Config.default()

        # 通知消息线程停止运行的 socket
        self._control_sock: socket.socket | None = None
        self._worker_sock: socket.socket | None = None

        # 通知 ping 线程停止运行的事件
        self._stop_ping_event: threading.Event | None = None

        # 处理消息线程
        self._recv_thread: threading.Thread | None = None

        # 发送 ping 线程
        self._ping_thread: threading.Thread | None = None

        # 验证超时时间
        self._auth_timeout = auth_timeout

        # 回调函数
        self.callbacks: Dict[str, Callable[[bytes], None]] = {}

    def connect(self) -> None:
        """
        Connect to the ahrimq server.
        """
        self._control_sock, self._worker_sock = socket.socketpair()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.config.host, self.config.port))

        msg = ReqMsgAuthorizer(
            access_key=self.config.access_key, access_secret=self.config.access_secret
        )
        self._send_message(msg)

        # 读取响应数据
        self._sock.settimeout(self._auth_timeout)
        length_bytes = self._recv_exact(4)

        length = struct.unpack('>I', length_bytes)[0]  # BigEndian uint32
        data = self._recv_exact(length)
        t, msg = deserialize(data)
        if t != TYPE_RESP_AUTHORIZER:
            raise ValueError("Invalid response message type")
        if msg.status != MsgStatus.SUCCESS:
            raise ValueError("Authorization failed")
        self._sock.settimeout(None)

        self._stop_ping_event = threading.Event()
        self._ping_thread = threading.Thread(
            target=self._send_ping, daemon=False
        )
        self._ping_thread.start()

        self._recv_thread = threading.Thread(
            target=self._receive_loop, daemon=True
        )
        self._recv_thread.start()

    def _receive_loop(self) -> None:
        """
        Receive messages from the server and handle them.
        """
        while self._sock and self._worker_sock:
            r, _, _ = select.select(
                [self._sock, self._worker_sock], [], []
            )
            if self._worker_sock in r:
                break
            if self._sock in r:
                length_bytes = self._recv_exact(4)
                length_unpack = struct.unpack(
                    '>I', length_bytes
                )  # BigEndian uint32
                length = length_unpack[0]

                # 2. 读取实际消息体
                data = self._recv_exact(length)
                if data:
                    self._handle_message(data)

    def _recv_exact(self, length: int) -> bytes:
        """
        Read exactly `length` bytes from the socket.
        :param length: Number of bytes to read.
        :return: Bytes read.
        """
        data = bytearray()
        while len(data) < length and self._sock:
            chunk = self._sock.recv(length - len(data))
            if not chunk:  # 连接关闭
                raise ConnectionError("Connection closed")
            data.extend(chunk)
        return bytes(data)

    def _send_ping(self) -> None:
        """
        Send a ping message to the server.
        """
        while not self._stop_ping_event.is_set():
            msg = ReqMsgPing()
            self._send_message(msg)
            self._stop_ping_event.wait(timeout=self.config.ping_interval)

    def _handle_message(self, data: bytes) -> None:
        """
        Handle a received message.
        :param data: Message data.
        """
        t, msg = deserialize(data)
        if t == TYPE_RESP_PING:
            pass
        elif t == TYPE_RESP_SUBSCRIBE:
            resp: RespMsgSubscribe = msg
            if resp.topic in self.callbacks:
                threading.Thread(
                    target=self.callbacks[resp.topic], args=(resp.message,)
                ).start()

    def _send_message(self, msg: Any) -> None:
        """
        Send a message to the server.
        :param msg: Message data.
        """
        message_bytes = serialize(msg)
        self._sock.sendall(struct.pack('>I', len(message_bytes)))
        self._sock.sendall(message_bytes)

    def close(self) -> None:
        """
        Close the connection to the server.
        """
        if self._stop_ping_event is not None:
            self._stop_ping_event.set()
            self._ping_thread.join()
        if self._control_sock is not None:
            self._control_sock.send(b"STOP")
            time.sleep(0.1)  # 等待线程退出
            self._control_sock.close()
            self._control_sock = None
        if self._worker_sock is not None:
            self._worker_sock.close()
            self._worker_sock = None
        if self._recv_thread is not None:
            self._recv_thread.join()
            self._recv_thread = None
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def subscribe(self, topic: str, callback: Callable[[bytes], None]) -> None:
        """
        Subscribe to a topic.
        :param topic: Topic name.
        :param callback: [(message: bytes) -> None] Callback function to call when a message is received.
        """
        message = ReqMsgSubscriber(topic=topic)
        self._send_message(message)
        self.callbacks[topic] = callback

    def unsubscribe(self, topic: str) -> None:
        """
        Unsubscribe from a topic.
        :param topic: Topic name.
        """
        message = ReqMsgUnsubscriber(topic=topic)
        self._send_message(message)

    def publish(self, topic: str, message: bytes) -> None:
        """
        Publish a message to a topic.
        :param topic: Topic name.
        :param message: Message to publish.
        """
        message = ReqMsgPublish(topic=topic, message=ByteArray(message))
        self._send_message(message)

    def reconsume_delay(self, message_id: int, delay: int) -> None:
        """
        Reconsume a message with delay.
        :param message_id: Message ID.
        :param delay: Delay in seconds.
        """
        message = ReqReconsumeDelay(id=message_id, delay=delay)
        self._send_message(message)
