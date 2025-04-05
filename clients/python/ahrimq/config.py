class Config:
    """
    AMQ configuration class, containing connection information and optional parameters.

    :param host: Server address, default is 127.0.0.1
    :param port: Port number, default is 60001
    :param access_key: Access key
    :param access_secret: Access secret
    :param retry_times: Message retry times, default is 3
    :param retry_interval: Message retry interval (seconds), default is 60
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 60001,
        access_key: str = '',
        access_secret: str = '',
        retry_times: int = 3,
        retry_interval: int = 60,
        ping_interval: int = 30,
    ):
        self.host = host
        self.port = port
        self.access_key = access_key
        self.access_secret = access_secret
        self.retry_times = retry_times
        self.retry_interval = retry_interval
        self.ping_interval = ping_interval
        if self.ping_interval < 10:
            self.ping_interval = 10

    @classmethod
    def default(cls) -> "Config":
        """
        Use default config.
        host: 127.0.0.1|
        port: 60001|
        access_key: ''|
        access_secret: ''|
        retry_times: 3|
        retry_interval: 60
        """
        return cls()

    def __repr__(self) -> str:
        return f"Config(host='{self.host}', port={self.port}, access_key='{self.access_key}', access_secret='{self.access_secret}', retry_times={self.retry_times}, retry_interval={self.retry_interval}, ping_interval={self.ping_interval})"
