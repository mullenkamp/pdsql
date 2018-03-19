pdsql - A Python package for Pandas/SQL
========================================

The pdsql package contains convenience functions for adding, manipulating, and changing data in SQL servers with a emphasis on Pandas DataFrames for the handling of data in Python.

At the moment, the only supported SQL system is MSSQL, but other SQL systems can/will be added in the future. Priority will be given to PostgreSQL and SQLite/Spatialite.

create_engine
--------------
The create_engine function is used to create an appropriate database engine through Sqlalchemy to interact with SQL databases.

.. toctree::
   :maxdepth: 2
   :caption: MSSQL

   mssql
