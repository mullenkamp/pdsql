# -*- coding: utf-8 -*-
"""
Utility functions for +other SQL modules.
"""
import numpy as np
import pandas as pd
import os
import sqlalchemy
import urllib


get_pk_stmt = "SELECT ORDINAL_POSITION AS [index], COLUMN_NAME AS name FROM {db}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '{table}' AND CONSTRAINT_NAME LIKE '%PK%' order by [index]"
get_un_stmt = "SELECT ORDINAL_POSITION AS [index], COLUMN_NAME AS name FROM {db}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '{table}' AND CONSTRAINT_NAME LIKE '%_UN' order by [index]"


def compare_dfs(old_df, new_df, on):
    """
    Function to compare two DataFrames with nans and return a dict with rows that have changed (diff), rows that exist in new_df but not in old_df (new), and rows  that exist in old_df but not in new_df (remove).
    Both DataFrame must have the same columns.

    Parameters
    ----------
    old_df : DataFrame
        The old DataFrame.
    new_df : DataFrame
        The new DataFrame.
    on : str or list of str
        The primary key(s) to index/merge the two DataFrames.

    Returns
    -------
    dict of DataFrames
        As described above, keys of 'diff', 'new', and 'remove'.
    """
    if ~np.in1d(old_df.columns, new_df.columns).any():
        raise ValueError('Both DataFrames must have the same columns')

    val_cols = [c for c in old_df.columns if not c in on]
    all_cols = old_df.columns.tolist()

    comp1 = pd.merge(old_df, new_df, on=on, how='outer', indicator=True, suffixes=('_x', ''))

    rem1 = comp1.loc[comp1._merge == 'left_only', on].copy()
    add1 = comp1.loc[comp1._merge == 'right_only', all_cols].copy()
    comp2 = comp1[comp1._merge == 'both'].drop('_merge', axis=1).copy()
#    comp2[comp2.isnull()] = np.nan

    old_cols = on.copy()
    old_cols_map = {c: c[:-2] for c in comp2 if '_x' in c}
    old_cols.extend(old_cols_map.keys())
    old_set = comp2[old_cols].copy()
    old_set.rename(columns=old_cols_map, inplace=True)
    new_set = comp2[all_cols].copy()

    comp_list = []
    for c in val_cols:
        isnull1 = new_set[c].isnull()
        if isnull1.any():
            new_set.loc[new_set[c].isnull(), c] = np.nan
        if old_set[c].dtype.name == 'float64':
            c1 = ~np.isclose(old_set[c], new_set[c])
        elif old_set[c].dtype.name == 'object':
            new_set[c] = new_set[c].astype(str)
            c1 = old_set[c].astype(str) != new_set[c]
        elif old_set[c].dtype.name == 'geometry':
            old1 = old_set[c].apply(lambda x: hash(x.wkt))
            new1 = new_set[c].apply(lambda x: hash(x.wkt))
            c1 = old1 != new1
        else:
            c1 = old_set[c] != new_set[c]
        notnan1 = old_set[c].notnull() | new_set[c].notnull()
        c2 = c1 & notnan1
        comp_list.append(c2)
    comp_index = pd.concat(comp_list, axis=1).any(1)
    diff_set = new_set[comp_index].copy()

    dict1 = {'diff': diff_set, 'new': add1, 'remove': rem1}

    return dict1


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
    base_url = "{driver};Trusted_Connection=yes;"
    sd = "SERVER={server};DATABASE={db};".format(server=server, db=database)
    base_url = base_url + sd

    if isinstance(username, str):
    	if not isinstance(password, str):
    		raise ValueError('If username is defined, then the password must also be defined')
    	up = "UID={user};PWD={password};".format(user=username, password=password)
    	base_url = base_url + up

    if db_type == 'mssql':
        try:
            import pymssql
            eng_str = 'mssql+pymssql://' + up + server + '/' + database
            engine = sqlalchemy.create_engine(eng_str)
        except:
            eng_str = urllib.parse.quote_plus(base_url.format(driver='DRIVER={ODBC Driver 17 for SQL Server}'))
            engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(eng_str))
            try:
                engine.connect()
            except:
                eng_str = urllib.parse.quote_plus(base_url.format(driver='DRIVER={ODBC Driver 13.1 for SQL Server}'))
                engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(eng_str))
                try:
                    engine.connect()
                except:
                    eng_str = urllib.parse.quote_plus(base_url.format(driver='DRIVER={ODBC Driver 13 for SQL Server}'))
                    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(eng_str))
                    try:
                        engine.connect()
                    except:
                        eng_str = urllib.parse.quote_plus(base_url.format(driver='DRIVER={ODBC Driver 11 for SQL Server}'))
                        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(eng_str))
                        try:
                            engine.connect()
                        except:
                            eng_str = urllib.parse.quote_plus(base_url.format(driver='DRIVER={SQL Server}'))
                            engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(eng_str))
                            try:
                                engine.connect()
                            except:
                                print('Install a proper ODBC mssql driver')
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


def create_snowflake_engine(username, password, account, database, schema):
    """

    """

    engine = sqlalchemy.create_engine(
    'snowflake://{user}:{password}@{account}/{database}/{schema}'.format(
        user=username,
        password=password,
        account=account,
        database=database,
        schema=schema
    ))
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


