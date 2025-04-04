class Config:
    """
    AMQ 配置类，包含连接信息和其他可选参数。

    :param host: 服务地址, 默认 127.0.0.1
    :param port: 端口号, 默认 60001
    :param access_key: 访问密钥 access_key
    :param access_secret: 访问密钥 access_secret
    :param retry_times: 消息重新发送次数, 默认 3 次
    :param retry_interval: 消息重新发送间隔 (秒), 默认 60 秒
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 60001,
        access_key: str = '',
        access_secret: str = '',
        retry_times: int = 3,
        retry_interval: int = 60,
    ):
        self.host = host
        self.port = port
        self.access_key = access_key
        self.access_secret = access_secret
        self.retry_times = retry_times
        self.retry_interval = retry_interval

    def __repr__(self) -> str:
        return f"Config(host={self.host}, port={self.port}, access_key={self.access_key}, access_secret={self.access_secret}, retry_times={self.retry_times}, retry_interval={self.retry_interval})"
