.. _reference:

MSSQL
======

The mssql module contains a variety of functions to interact with MSSQL databases through Python and Pandas.

Reading tables
--------------

.. autoattribute:: pdsql.mssql.rd_sql

.. automethod:: pdsql.mssql.rd_sql_ts

.. automethod:: pdsql.mssql.rd_sql_geo

Creating tables
---------------

.. automethod:: pdsql.mssql.create_mssql_table

Writing to tables
-----------------

.. automethod:: pdsql.mssql.to_mssql

Delete rows in tables
---------------------

.. automethod:: pdsql.mssql.del_mssql_table_rows


API Pages
---------

.. currentmodule:: pdsql
.. autosummary::
  :template: autosummary.rst
  :toctree: mssql/
