
class AhrimqAsync:
    def __init__(self, host: str, port: int, access_key: str, access_secret: str, retry_times: int = 3, retry_interval: int = 60):
        self.host = host
        self.port = port
        self.access_key = access_key
        self.access_secret = access_secret
        self.retry_times = retry_times
        self.retry_interval = retry_interval
        self.conn = FileNotFoundError
