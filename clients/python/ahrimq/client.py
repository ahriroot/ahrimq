

class Ahrimq:
    def __init__(self, host: str, port: int, access_key: str, access_secret: str, retry_times: int = 3, retry_interval: int = 60):
        self.host = host
        self.port = port