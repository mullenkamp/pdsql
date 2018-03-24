pdsql - A Python package for Pandas/SQL
========================================

The pdsql package contains convenience functions for adding, manipulating, and changing data in SQL servers with a emphasis on Pandas DataFrames for the handling of data in Python.

At the moment, the only supported SQL system is MSSQL, but other SQL systems can/will be added in the future. Priority will be given to PostgreSQL and SQLite/Spatialite.

Documentation
--------------
The primary documentation for the package can be found `here <http://pdsql.readthedocs.io>`_.

Installation
------------
pdsql can be installed via pip or conda::

  pip install pdsql

or::

  conda install -c mullenkamp pdsql

The core dependencies are Pandas, Sqlalchemy, and pyodbc.

Geometry data types in SQL are supported, but GeoPandas and pycrs must also be installed.

To do
-----
- More documentation
- Unit tests
- Support for other SQL database programs (e.g. PostgreSQL and SQLite)
