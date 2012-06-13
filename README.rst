Description
===========

This is Django based website that allows people to inspect the results
of a LOFAR transients pipeline run. It will show light curves and
images for found transients (in fact, all found sources), as well as
some diagnostics.

It's main purposes currently is to provide an easy way for people to
browse through the data obtained from a transients pipeline run.


Installation
============

The actual installation is, at the moment, simply a matter of cloning the git repository.

Check that you have the necessary dependencies (see below), then take
the following steps:

- Put jquery.js in tkpweb/staticfiles/javascript/ (from jquery.com).
  Note that you can also change the link in tkpweb/templates/base.html


- copy the templates to their respective names (i.e., remove
  "_template" from their copy). There are four files:

  - tkpweb/settings.py_template

  - tkpweb/apps/database/views.py_tempmlate

  - tkp.cfg_template

  - runserver.bash_template


- edit tkpweb/settings.py:

  - verify that you're happy with the default database settings

  - edit the `SECRET_KEY`.

  - if required, enable the MongoDB image store and enter the details of your
    MongoDB database.

- edit tkpweb/apps/database/views.py:

  - fill out the necessary servers, ports and passwords to access your
    database(s), so that these can be listed and set on the webpages

- edit the tkp.cfg file to point to your default database

- set up a Django database by running::

    python manage.py syncdb

  at the base level

- alter the runserver.bash script to set the `PYTHONPATH` and
  `LD_LIBRARY_PATH` variables correctly.

  You can also add a server name and port at the end of the "python
  manage.py runserver" line; see the django documentation for more
  information.

- Convert the \*scss files in the static directories to css files,
  using sass ::

    sass base.scss:base.css

- Collect the static files ::

    python manage.py collectstatic

- Now you can start the development server::

      ./runserver.bash


Dependencies
------------

- Python (2.6 or 2.7).

- The TKP library (with all its dependencies).

- Django  (> 1.3): https://www.djangoproject.com/

  For the necessary (server side) web framework.

- Matplotlib (> 1): http://matplotlib.sourceforge.net/

- AplPy (> 0.9.6): http://aplpy.github.com/

  For the image plots.

  AplPy requires numpy and matplotlib, as well as:

  - pyfits: http://www.stsci.edu/institute/software_hardware/pyfits/

  - pywcs: https://trac.assembla.com/astrolib

- Sass, to convert the scss files to CSS files.
  See http://sass-lang.com/

- (Optionally) PyMongo (including GridFS) to fetch images from a MongoDB
  databse: https://github.com/mongodb/mongo-python-driver


License
=======

TKP-web is licensed under the very liberal ISC license
(http://www.isc.org/software/license).
