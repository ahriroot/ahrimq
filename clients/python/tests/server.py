import socket
import socketserver
import struct
import threading
import time

from ahrimq.message import *

from concurrent.futures import ThreadPoolExecutor


class ThreadedTCPServer(socketserver.ThreadingTCPServer):
    """支持优雅关闭的多线程服务器"""
    allow_reuse_address = True
    daemon_threads = True  # 主线程退出时自动结束工作线程

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._shutdown_event = threading.Event()
        self.thread_pool = ThreadPoolExecutor(max_workers=10)

    def shutdown(self):
        """优雅关闭服务器"""
        self._shutdown_event.set()
        self.thread_pool.shutdown(wait=True)
        super().shutdown()


class ConnectionHandler(socketserver.BaseRequestHandler):
    """支持协议处理的多连接处理器"""

    def handle(self):
        try:
            while not self.server._shutdown_event.is_set():
                try:
                    # 1. 读取消息头
                    header = self._recv_exact(4)
                    if not header:
                        break

                    # 2. 解析消息长度
                    msg_len = struct.unpack('>I', header)[0]

                    # 3. 读取消息体
                    data = self._recv_exact(msg_len)

                    # 4. 提交到线程池处理
                    self.server.thread_pool.submit(
                        self._process_message,
                        data
                    )
                except ConnectionError:
                    break
        finally:
            self.request.close()

    def _recv_exact(self, length: int) -> bytes:
        """精确读取指定长度的数据"""
        data = bytearray()
        while len(data) < length:
            try:
                chunk = self.request.recv(length - len(data))
                if not chunk:
                    return b''
                data.extend(chunk)
            except (ConnectionError, OSError):
                return b''
        return bytes(data)

    def _process_message(self, data: bytes):
        """实际业务处理（线程池中执行）"""
        msg_type, msg = deserialize(data)

        if msg_type == TYPE_REQ_PING:
            response = serialize(RespMsgPing("pong"))

        elif msg_type == TYPE_REQ_AUTHORIZER:
            req: ReqMsgAuthorizer = msg
            if req.access_key == "test" and req.access_secret == "test":
                response = serialize(RespMsgAuthorizer(
                    1, MsgStatus.SUCCESS, "success"))
            elif req.access_key == "timeout" and req.access_secret == "timeout":
                time.sleep(2)
                response = serialize(RespMsgAuthorizer(
                    1, MsgStatus.FAILURE, "failure"))
            elif req.access_key == "invalid" and req.access_secret == "invalid":
                response = serialize(RespMsgPing("pong"))
            elif req.access_key == "close" and req.access_secret == "close":
                self.request.shutdown(socket.SHUT_RDWR)
                self.request.close()
                return
            else:
                response = serialize(RespMsgAuthorizer(
                    1, MsgStatus.FAILURE, "failure"))

        elif msg_type == TYPE_REQ_PUBLISH:
            req: ReqMsgPublish = msg
            r = RespMsgPublish(
                1, MsgStatus.SUCCESS, req.topic, "success"
            )
            response = serialize(r)

            r = RespMsgSubscribe(
                1, MsgStatus.SUCCESS, req.topic, ByteArray(
                    "message", encoding="utf-8")
            )
            response = serialize(r)

        else:
            response = serialize(f"Unsupported type: {msg_type}")

        # 发送响应
        self.request.sendall(struct.pack('>I', len(response)) + response)
