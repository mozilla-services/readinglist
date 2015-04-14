import sys

import mock

import readinglist
from .support import unittest


class GEventActivationTest(unittest.TestCase):
    def setUp(self):
        self.gevent_mocked = mock.MagicMock()
        self.psycogreen_mocked = mock.MagicMock()
        sys.modules['gevent'] = self.gevent_mocked
        sys.modules['gevent.monkey'] = self.gevent_mocked.monkey
        sys.modules['psycogreen'] = self.psycogreen_mocked
        sys.modules['psycogreen.gevent'] = self.psycogreen_mocked.gevent

    def test_gevent_is_not_setup_by_default(self):
        readinglist.patch_gevent({})
        self.assertFalse(self.gevent_mocked.monkey.patch_socket.called)
        self.assertFalse(self.psycogreen_mocked.gevent.patch_psycopg.called)

    def test_gevent_is_activated_from_settings(self):
        readinglist.patch_gevent({'readinglist.gevent_enabled': 'true'})
        self.assertTrue(self.gevent_mocked.monkey.patch_socket.called)
        self.assertTrue(self.psycogreen_mocked.gevent.patch_psycopg.called)
