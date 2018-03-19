# -*- coding: utf-8 -*-
"""
Utility functions for +other SQL modules.
"""
import os
import sqlalchemy


def create_engine(db_type, server, database, username=None, password=None):
    """
    Function to create an sqlalchemy engine.

    Parameters
    ----------
    db_type : str
        The type of database to connect to. Options include mssql, postgresql, oracle, mysql, and sqlite.
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    username : str or None
        Either the username or None when not needed.
    password : str or None
        Either the password or None when not needed.

    Returns
    -------
    sqlalchemy engine

    Notes
    -----
    If pymssql is installed, create_eng will use the package instead of pyodbc.
    """
    if isinstance(username, str):
        up = username
        if isinstance(password, str):
            up = up + ':' + password
        up = up + '@'
    else:
        up = ''
    if db_type == 'mssql':
        try:
            import pymssql
            eng_str = 'mssql+pymssql://' + up + server + '/' + database
            engine = sqlalchemy.create_engine(eng_str)
        except ImportError:
            driver1 = '?driver=ODBC+Driver+13+for+SQL+Server'
            eng_str = 'mssql+pyodbc://' + up + server + '/' + database + driver1
            engine = sqlalchemy.create_engine(eng_str)
            try:
                engine.connect()
            except:
                driver2 = '?driver=ODBC+Driver+11+for+SQL+Server'
                eng_str = 'mssql+pyodbc://' + up + server + '/' + database + driver2
                engine = sqlalchemy.create_engine(eng_str)
                engine.connect()
    elif db_type == 'postgresql':
        eng_str = 'postgresql://' + up + server + '/' + database
        engine = sqlalchemy.create_engine(eng_str)
    elif db_type == 'oracle':
        eng_str = 'oracle://' + up + server + '/' + database
        engine = sqlalchemy.create_engine(eng_str)
    elif db_type == 'mysql':
        eng_str = 'mysql+mysqldb://' + up + server + '/' + database
        engine = sqlalchemy.create_engine(eng_str)
    elif db_type == 'sqlite':
        engine = sqlalchemy.create_engine('sqlite:///:memory:')

    return engine


def save_df(df, path_str, index=True, header=True):
    """
    Function to save a dataframe based on the path_str extension. The path_str must  either end in csv or h5.

    df -- Pandas DataFrame.\n
    path_str -- File path (str).\n
    index -- Should the row index be saved? Only necessary for csv.
    """

    path1 = os.path.splitext(os.path_str)

    if path1[1] in '.h5':
        df.to_hdf(path_str, 'df', mode='w')
    if path1[1] in '.csv':
        df.to_csv(path_str, index=index, header=header)


