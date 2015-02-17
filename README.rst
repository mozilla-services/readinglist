Readinglist
===========

Reading list is a service that aims to synchronize a list of articles URLs
between a set of devices owned by a same account.

|travis| |readthedocs|

.. |travis| image:: https://travis-ci.org/mozilla-services/readinglist.svg?branch=master
    :target: https://travis-ci.org/mozilla-services/readinglist

.. |readthedocs| image:: https://readthedocs.org/projects/readinglist/badge/?version=latest
    :target: http://readinglist.readthedocs.org/en/latest/
    :alt: Documentation Status



API
===

* `API Design proposal
  <https://github.com/mozilla-services/readinglist/wiki/API-Design-proposal>`_
* `Online documentation <http://readinglist.readthedocs.org/en/latest/>`_



Run locally
===========

Reading list persists its sessions and its records inside a `Redis <http://redis.io/>`_
database, so it has to be installed first (see the "Install Redis" section below for
more on this).

Once Redis is installed:

::

    make serve


Storage backend
===============

Configuration can be changed to persist the records in different storage engines.


In-Memory
---------

Useful for development or testing purposes, but records are lost after each server restart.

In `conf/readinglist.ini`::

    readinglist.storage_backend = readinglist.storage.memory


Redis
-----

Useful for very low server load, but won't scale since records sorting and filtering
are performed in memory.

In `conf/readinglist.ini`::

    readinglist.storage_backend = readinglist.storage.simpleredis

*(Optional)* Instance location URI can be customized::

    readinglist.storage_url = localhost:6379/0


PostgreSQL
----------

Recommended in production (*requires PostgreSQL 9.3 or higher*).

Install PostgreSQL client headers::

    sudo apt-get install libpq-dev

Install Reading list related dependencies::

    pip install readinglist[postgresql]

In `conf/readinglist.ini`::

    readinglist.storage_backend = readinglist.storage.postgresql

*(Optional)* Instance location URI can be customized::

    readinglist.storage_url = user:pass@db.server.lan:5432/dbname


Install Redis
=============

Linux
-----

On debian / ubuntu based systems::

    apt-get install redis-server


or::

    yum install redis

OS X
----

Assuming `brew <http://brew.sh/>`_ is installed, Redis installation becomes:

::

    brew install redis

To restart it (Bug after configuration update)::

    brew services restart redis


Run tests
=========

::

    make tests
