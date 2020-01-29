# -*- coding: utf-8 -*-
"""
Created on Mon Jan 27 15:40:52 2020

@author: MichaelEK
"""
import numpy as np
import pandas as pd
from pdsql.util import create_snowflake_engine, get_pk_stmt, compare_dfs, get_un_stmt

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


def to_table(df, table, username, password, account, database, schema):
    """
    Function to append a DataFrame onto an existing mssql table.

    Parameters
    ----------
    df : DataFrame
        DataFrame to be saved. The DataFrame column/index names must match those on the mssql table exactly.
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
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

    Returns
    -------
    None
    """
    ### Prepare the engine
    engine = create_snowflake_engine(username, password, account, database, schema)

    ### Save to mssql table
    df.to_sql(name=table, con=engine, if_exists='append', chunksize=5000, index=False)


#def del_table_rows(username, password, account, database, schema, table=None, pk_df=None, stmt=None):
#    """
#    Function to selectively delete rows from an mssql table.
#
#    Parameters
#    ----------
#    server : str
#        The server name. e.g.: 'SQL2012PROD03'
#    database : str
#        The specific database within the server. e.g.: 'LowFlows'
#    table : str or None if stmt is a str
#        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
#    pk_df : DataFrame
#        A DataFrame of the primary keys of the table for the rows that should be removed.
#    stmt : str
#        SQL delete statement. Will override everything except server and database.
#
#    Returns
#    -------
#    None
#
#    Notes
#    -----
#    Using the pk_df is the only way to ensure that specific rows will be deleted from composite keys. The column data types and names of pk_df must match the equivelant columns in the SQL table. The procedure creates a temporary table from the pk_df then deletes the rows in the target table based on the temp table. Then finally deletes the temp table.
#    """
#
#    ### Make the delete statement
##    del_where_list = sql_where_stmts(**kwargs)
#    if isinstance(stmt, str):
#        del_rows_stmt = stmt
#    elif isinstance(pk_df, pd.DataFrame):
#        temp_tab = '#temp_del_tab1'
#
#        ### Check the primary keys and unique keys
#        if '.' in table:
#            table1 = table.split('.')[1]
#        else:
#            table1 = table
#        pk_stmt = get_pk_stmt.format(db=database, table=table1)
#        pk = read_table(server, database, stmt=pk_stmt, username=username, password=password).name
#
#        un_stmt = get_un_stmt.format(db=database, table=table1)
#        un = read_table(server, database, stmt=un_stmt, username=username, password=password).name
#
#        if pk.empty:
#            raise ValueError('SQL table has no primary key. Please set one up.')
#        if (not np.isin(pk, pk_df.columns.tolist()).all()) & (not np.isin(un, pk_df.columns.tolist()).all()):
#            raise ValueError('The primary or unique keys in the SQL table does not match up with the pk_df')
#
#        sel_t1 = "select * from " + temp_tab
#        cols = pk_df.columns.tolist()
#        tab_where = [table + '.' + i for i in cols]
#        t1_where = [temp_tab + '.' + i for i in cols]
#        where_list = [t1_where[i] + ' = ' + tab_where[i] for i in np.arange(len(cols))]
#        where_stmt = " where " + " and ".join(where_list)
#        exists_stmt = "(" + sel_t1 + where_stmt + ")"
#        del_rows_stmt = "DELETE FROM " + table + " where exists " + exists_stmt
##    elif isinstance(del_where_list, list):
##        del_rows_stmt = "DELETE FROM " + table + " WHERE " + " AND ".join(del_where_list)
#    else:
#        raise ValueError('Please specify pk_df or stmt')
#
#    ### Delete rows
#    engine = create_snowflake_engine(username, password, account, database, schema)
#    with engine.begin() as conn:
#        if isinstance(pk_df, pd.DataFrame):
#            pk_df.to_sql(name=temp_tab, con=conn, if_exists='replace', chunksize=1000)
#        conn.execute(del_rows_stmt)


#def update_table_rows(df, username, password, account, database, schema, table, on=None, append=True):
#    """
#    Function to update rows from an mssql table. SQL table must have a primary key and the primary key must be in the input DataFrame.
#
#    Parameters
#    ----------
#    df : DataFrame
#        DataFrame with data to be overwritten in SQL table.
#    server : str
#        The server name. e.g.: 'SQL2012PROD03'
#    database : str
#        The specific database within the server. e.g.: 'LowFlows'
#    table : str
#        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
#    on : None, str, or list of str
#        The index by which the update should be applied on. If None, then it uses the existing primary key(s).
#    append : bool
#        Should new sites be appended to the table?
#
#    Returns
#    -------
#    None
#    """
#    ### Check the primary keys
#    if on is None:
#        pk_stmt = get_pk_stmt.format(db=database, table=table)
#        pk = read_table(username, password, account, database, schema, pk_stmt).name.tolist()
#
#        if not pk:
#            raise ValueError('SQL table has no primary key. Please set one up or assign "on" explicitly.')
#        on = pk
#    elif isinstance(on, str):
#        on = [on]
#
#    ## Check that "on" are in the tables
#    df_bool = ~np.isin(on, df.columns).all()
#    if df_bool:
#        raise ValueError('"on" contains column names that are not in the df')
#
#
#    ### Make the update statement
#    temp_tab = '#temp_up1'
#    on_tab = [table + '.' + i for i in on]
#    on_temp = [temp_tab + '.' + i for i in on]
#    cols = df.columns.tolist()
#    val_cols = [i for i in cols if not i in on]
#    tab_list = [table + '.' + i for i in val_cols]
#    temp_list = [temp_tab + '.' + i for i in val_cols]
#    temp_list2 = [temp_tab + '.' + i for i in cols]
#    up_list = [tab_list[i] + ' = ' + temp_list[i] for i in np.arange(len(temp_list))]
#    on_list = [on_tab[i] + ' = ' + on_temp[i] for i in np.arange(len(on))]
##    up_stmt = "update " + table + " set " + ", ".join(up_list) + " from " + table + " inner join " + temp_tab + " on " + " and ".join(on_list)
#    if append:
#        up_stmt = "merge " + table + " using " + temp_tab + " on (" + " and ".join(on_list) + ") when matched then update set " + ", ".join(up_list) +  " WHEN NOT MATCHED BY TARGET THEN INSERT (" + ", ".join(cols) + ") values (" + ", ".join(temp_list2) + ");"
#    else:
#        up_stmt = "merge " + table + " using " + temp_tab + " on (" + " and ".join(on_list) + ") when matched then update set " + ", ".join(up_list) +  ";"
#
#    ### Run SQL code to update rows
#    engine = create_snowflake_engine(username, password, account, database, schema)
#    with engine.begin() as conn:
#        print('Saving data to temp table...')
#        df.to_sql(name=temp_tab, con=engine, if_exists='append', chunksize=5000, index=False)
#        print('Updating primary table...')
#        conn.execute(up_stmt)
