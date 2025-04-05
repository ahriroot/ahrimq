import unittest

from ahrimq.config import Config


class TestConfig(unittest.TestCase):
    def test_default(self):
        config = Config.default()
        self.assertEqual(config.host, '127.0.0.1')
        self.assertEqual(config.port, 60001)
        self.assertEqual(config.access_key, '')
        self.assertEqual(config.access_secret, '')
        self.assertEqual(config.retry_times, 3)
        self.assertEqual(config.retry_interval, 60)
        self.assertEqual(config.ping_interval, 30)

    def test_repr(self):
        config = Config('127.0.0.2', 60002, 'key', 'secret', 5, 10, 30)
        self.assertEqual(
            repr(config),
            "Config(host='127.0.0.2', port=60002, access_key='key', access_secret='secret', retry_times=5, retry_interval=10, ping_interval=30)"
        )

    def test_ping_interval(self):
        config = Config(ping_interval=1)
        self.assertEqual(config.ping_interval, 10)
