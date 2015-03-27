Contributing
############

Thank you for considering to contribute to *Reading List*!

:note:

    No contribution is too small; please submit as many fixes for typos and
    grammar bloopers as you can!

:note:

    Open a pull-request even if your contribution is not ready yet! It can
    be discussed and improved collaboratively!


Run tests
=========

::

    make tests


Run load tests
==============

From the :file:`loadtests` folder:

::

    make test SERVER_URL=http://localhost:8000


Run a particular type of action instead of random:

::

    LOAD_ACTION=batch_create make test SERVER_URL=http://localhost:8000

(*See loadtests source code for an exhaustive list of available actions and
their respective randomness.*)


Performance profiling
=====================

Gather data from the application execution, using cProfile:

::

    .venv/bin/python -m cProfile -o myprofile .venv/bin/pserve config/readinglist.ini

Run a load test (*for example*):

::

    SERVER_URL=http://localhost:8000 make bench -e


Render execution graphs using GraphViz:

::

    sudo apt-get install graphviz

::

    pip install gprof2dot
    gprof2dot -f pstats stats.prof | dot -Tpng -o output.png


IRC channel
===========

Join ``#storage`` on ``irc.mozilla.org``!
