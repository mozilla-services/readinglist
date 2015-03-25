import pkg_resources
import logging

from pyramid.config import Configurator
from pyramid.settings import asbool

from cliquet import initialize_cliquet

# Module version, as defined in PEP-0396.
__version__ = pkg_resources.get_distribution(__package__).version

# The API version is derivated from the module version.
API_VERSION = 'v%s' % __version__.split('.')[0]

# Main readinglist logger
logger = logging.getLogger(__name__)


def patch_gevent(settings):
    if asbool(settings.get('readinglist.gevent_enabled', False)):
        import gevent
        import gevent.monkey
        gevent.monkey.patch_socket()

        import psycogreen.gevent
        psycogreen.gevent.patch_psycopg()


def main(global_config, **settings):
    config = Configurator(settings=settings)

    patch_gevent(settings)

    initialize_cliquet(config, version=__version__)
    config.scan("readinglist.views")
    return config.make_wsgi_app()
