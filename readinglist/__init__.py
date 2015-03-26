import pkg_resources
import logging

from pyramid.config import Configurator

from cliquet import initialize_cliquet

# Module version, as defined in PEP-0396.
__version__ = pkg_resources.get_distribution(__package__).version

# The API version is derivated from the module version.
API_VERSION = 'v%s' % __version__.split('.')[0]

# Main readinglist logger
logger = logging.getLogger(__name__)


def main(global_config, **settings):
    config = Configurator(settings=settings)

    initialize_cliquet(config, version=__version__)

    # Force default pagination (if empty, None or 0)
    paginate_by = config.registry.settings.get('cliquet.paginate_by')
    if not paginate_by:
        config.registry.settings['cliquet.paginate_by'] = 100

    config.scan("readinglist.views")
    return config.make_wsgi_app()
