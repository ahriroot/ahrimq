from .config import Config

class AhrimqAsync:
    def __init__(self, config: Config) -> None:
        self.config = config
