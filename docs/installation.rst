Installation
############


Run locally
===========

*Reading List* is based on top of the `cliquet <https://cliquet.rtfd.org>`_ project, and
as such, please refer to cliquet's documentation for more details.


For development
---------------

By default, *Reading List* persists the records and internal cache in a PostgreSQL
database.

The default configuration will connect to the ``postgres`` database on
``localhost:5432``, with user/password ``postgres``/``postgres``. See more details
below about installation and setup of PostgreSQL.

::

    make serve


Using Docker
------------

*Reading List* uses `Docker Compose <http://docs.docker.com/compose/>`_, which takes
care of running and connecting PostgreSQL:

::

    docker-compose up


Authentication
--------------

By default, *Reading List* relies on Firefox Account OAuth2 Bearer tokens to authenticate
users.

See `cliquet documentation <http://cliquet.readthedocs.org/en/latest/configuration.html#authentication>`_
to configure authentication options.


Install and setup PostgreSQL
============================

 (*requires PostgreSQL 9.3 or higher*).


Using Docker
------------

::

    docker run -e POSTGRES_PASSWORD=postgres -p 5434:5432 postgres


Linux
-----

On debian / ubuntu based systems:

::

    apt-get install postgresql postgresql-contrib


By default, the ``postgres`` user has no password and can hence only connect
if ran by the ``postgres`` system user. The following command will assign it:

::

    sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';"


OS X
----

Assuming `brew <http://brew.sh/>`_ is installed:

::

    brew update
    brew install postgresql

Create the initial database:

::

    initdb /usr/local/var/postgres


Cryptography libraries
======================

Linux
-----

On Debian / Ubuntu based systems::

    apt-get install libffi-dev libssl-dev

On RHEL-derivatives::

    apt-get install libffi-devel openssl-devel

OS X
----

Assuming `brew <http://brew.sh/>`_ is installed:

::

    brew install libffi openssl pkg-config


Running in production
=====================

Recommended settings
--------------------

Most default setting values in the application code base are suitable for production.

But the set of settings mentionned below might deserve some review or adjustments:


.. code-block :: ini

    cliquet.http_scheme = https
    cliquet.paginate_by = 100
    cliquet.batch_max_requests = 25
    cliquet.delete_collection_enabled = false
    cliquet.basic_auth_enabled = false
    cliquet.storage_pool_maxconn = 50
    cliquet.cache_pool_maxconn = 50
    fxa-oauth.cache_ttl_seconds = 3600

:note:

    For an exhaustive list of available settings and their default values,
    refer to `cliquet source code <https://github.com/mozilla-services/cliquet/blob/93b94a4ce7f6d8788e2c00b609ec270c377851eb/cliquet/__init__.py#L34-L59>`_.


Monitoring
----------

.. code-block :: ini

    # Heka
    cliquet.logging_renderer = cliquet.logs.MozillaHekaRenderer

    # StatsD
    cliquet.statsd_url = udp://carbon.server:8125

Application output should go to ``stdout``, and message format should have no
prefix string:


.. code-block :: ini

    [handler_console]
    class = StreamHandler
    args = (sys.stdout,)
    level = INFO

    [formatter_heka]
    format = %(message)s


If you want to plug sentry, you should also add:

.. code-block:: ini

    [loggers]
    keys = root, sentry
    
    [handlers]
    keys = console, sentry
    
    [formatters]
    keys = generic
    
    [logger_root]
    level = INFO
    handlers = console, sentry
    
    [logger_sentry]
    level = WARN
    handlers = console
    qualname = sentry.errors
    propagate = 0
    
    [handler_console]
    class = StreamHandler
    args = (sys.stderr,)
    level = NOTSET
    formatter = generic
    
    [handler_sentry]
    class = raven.handlers.logging.SentryHandler
    args = ('http://public:secret@example.com/1',)
    level = WARNING
    formatter = generic
    
    [formatter_generic]
    format = %(asctime)s,%(msecs)03d %(levelname)-5.5s [%(name)s] %(message)s
    datefmt = %H:%M:%S


PostgreSQL setup
----------------

In production, it is wise to run the application with a dedicated database and
user.

::

    postgres=# CREATE USER produser;
    postgres=# CREATE DATABASE proddb OWNER produser;
    CREATE DATABASE


On the first app run, the tables and objects are created.

:note:

    Alternatively the SQL initialization files can be found in the
    *cliquet* source code (``cliquet/cache/postgresql/schemal.sql`` and
    ``cliquet/storage/postgresql/schemal.sql``).


Running with uWsgi
------------------

To run the application using uWsgi, an **app.wsgi** file is provided.
This command can be used to run it::

    uwsgi --ini config/readinglist.ini

uWsgi configuration can be tweaked in the ini file in the dedicated
**[uwsgi]** section.

To use a different ini file, the ``READINGLIST_INI`` environment variable
should be present with a path to it.


Running with gevent
-------------------

It is possible to use `gevent <https://gevent.org>`_, by adding this in the
configuration:

.. code-block :: ini

      readinglist.gevent_enabled = true

Gevent and psycogreen should be installed in the virtualenv for it to work
properly::

    .venv/bin/pip install gevent psycogreen

:note:

    Gevent support is known to have issues with Python 3, and as such, it
    is discouraged to use it in this environment.
