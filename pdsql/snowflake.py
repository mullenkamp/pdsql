# -*- coding: utf-8 -*-
"""
Created on Mon Jan 27 15:40:52 2020

@author: MichaelEK
"""
import pandas as pd
from util import create_snowflake_engine

####################################################


def read_table(username, password, account, database, schema, stmt, con=None):
    """
    Function to import data from an MSSQL database.

    Parameters
    ----------
    username : str
        The username
    password : str
        The password
    account : str
        account is the name assigned to your account by Snowflake. In the hostname you received from Snowflake (after your account was provisioned), your account name is the full/entire string to the left of snowflakecomputing.com
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    schema : str
        The schema associated with the table to be read.
    stmt : str
        Custom SQL statement to be directly passed to the database and schema.
    con : SQLAlchemy connectable (engine/connection) or database string URI
        The sqlalchemy connection to be passed to pandas.read_sql

    Returns
    -------
    DataFrame
    """

    ## Create connection to database and execute sql statement
    if con is None:
        con = create_snowflake_engine(username, password, account, database, schema)
    lst1 = []
    for chunk in pd.read_sql_query(stmt, con, chunksize=50000):
        lst1.append(chunk)
    df1 = pd.concat(lst1)

    return df1

