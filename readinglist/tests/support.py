try:
    import unittest2 as unittest
except ImportError:
    import unittest  # NOQA

import webtest

from cliquet.tests.support import (BaseWebTest as CliquetBaseTest,
                                   get_request_class)
from readinglist import API_VERSION


class BaseWebTest(CliquetBaseTest):
    """Base Web Test to test your cornice service.

    It setups the database before each test and delete it after.
    """

    def _get_test_app(self, settings=None):
        app = webtest.TestApp("config:config/readinglist.ini",
                              relative_to='.')
        app.RequestClass = get_request_class(API_VERSION)
        return app
