# -*- coding: utf-8 -*-
"""
Functions for importing mssql data.
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime
from pdsql.util import create_engine, get_pk_stmt, compare_dfs, get_un_stmt

try:
    from geopandas import GeoDataFrame
    from shapely.wkb import loads
    from pycrs import parse
except ImportError:
    print('Install geopandas for reading geometery columns')

###########################################
### Parameters

get_tables_stmt = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='{dbName}'"

geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table}' AND DATA_TYPE='geometry'"

geo_srid_stmt = "select distinct {geo_col}.STSrid from {table}"


###########################################
### Functions


def rd_sql(server, database, table=None, col_names=None, where_in=None, where_op='AND', geo_col=False, from_date=None, to_date=None, date_col=None, rename_cols=None, stmt=None, con=None, username=None, password=None):
    """
    Function to import data from an MSSQL database.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    col_names : list of str
        The column names that should be retrieved. e.g.: ['SiteID', 'BandNo', 'RecordNo']
    where_in : dict
        A dictionary of strings to lists of strings.'. e.g.: {'SnapshotType': ['value1', 'value2']}
    where_op : str
        If where_in is a dictionary and there are more than one key, then the operator that connects the where statements must be either 'AND' or 'OR'.
    geo_col : bool
        Is there a geometry column in the table?.
    from_date : str
        The start date in the form '2010-01-01'.
    to_date : str
        The end date in the form '2010-01-01'.
    date_col : str
        The SQL table column that contains the dates.
    rename_cols: list of str
        List of strings to rename the resulting DataFrame column names.
    stmt : str
        Custom SQL statement to be directly passed to the database. This will ignore all prior arguments except server and database.
    con : SQLAlchemy connectable (engine/connection) or database string URI
        The sqlalchemy connection to be passed to pandas.read_sql

    Returns
    -------
    DataFrame
    """

    ## Create where statements
    if stmt is None:

        if table is None:
            raise ValueError('Must at least provide input for server, database, and table.')

        if col_names is not None:
            if isinstance(col_names, str):
                col_names = [col_names]
            col_names1 = ['[' + i.encode('ascii', 'ignore').decode() + ']' for i in col_names]
            col_stmt = ', '.join(col_names1)
        else:
            col_stmt = '*'

        where_lst, where_temp = sql_where_stmts(where_in=where_in, where_op=where_op, from_date=from_date, to_date=to_date, date_col=date_col)

        if isinstance(where_lst, list):
            stmt1 = "SELECT " + col_stmt + " FROM " + table + " where " + " and ".join(where_lst)
        else:
            stmt1 = "SELECT " + col_stmt + " FROM " + table

    elif isinstance(stmt, str):
        where_temp = {}
        stmt1 = stmt

    else:
        raise ValueError('stmt must either be an SQL string or None.')

    ## Create connection to database and execute sql statement
    if geo_col & (stmt is None):
        df = rd_sql_geo(server=server, database=database, table=table, col_stmt=col_stmt, where_lst=where_lst, username=username, password=password)
        if rename_cols is not None:
            rename_cols1 = rename_cols.copy()
            rename_cols1.extend(['geometry'])
            df.columns = rename_cols1
    else:
        if con is None:
            engine = create_engine('mssql', server, database, username=username, password=password)
            with engine.begin() as conn:
                if where_temp:
                    for key, value in where_temp.items():
                        df = pd.DataFrame(data=value, columns=[key.lower()])
                        temp_tab = '#temp_'+key.lower()
                        df.to_sql(temp_tab, con=conn, if_exists='replace', index=False, chunksize=1000)
                df = pd.read_sql(stmt1, con=conn)
        else:
            if where_temp:
                for key, value in where_temp.items():
                    df = pd.DataFrame(data=value, columns=[key])
                    temp_tab = '#temp_'+key
                    df.to_sql(temp_tab, con=con, if_exists='replace', index=False, chunksize=1000)
            df = pd.read_sql(stmt1, con=con)
        if rename_cols is not None:
            df.columns = rename_cols

    return df


def rd_sql_ts(server, database, table, groupby_cols, date_col, values_cols, resample_code=None, period=1, fun='mean', val_round=3, where_in=None, where_op='AND', from_date=None, to_date=None, min_count=None, con=None, username=None, password=None):
    """
    Function to specifically read and possibly aggregate time series data stored in MSSQL tables.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    groupby_cols : str or list of str
        The columns in the SQL table to grouped and returned with the time series data.
    date_col : str
        The date column in the table.
    values_cols : str or list of str
        The column(s) of the value(s) that should be resampled.
    resample_code : str or None
        The Pandas time series resampling code. e.g. 'D' for day, 'W' for week, 'M' for month, etc.
    period : int
        The number of resampling periods. e.g. period = 2 and resample = 'D' would be to resample the values over a 2 day period.
    fun : str
        The resampling function. i.e. mean, sum, count, min, or max. No median yet...
    val_round : int
        The number of decimals to round the values.
    where_in : dict
        A dictionary of strings to lists of strings.'. e.g.: {'SnapshotType': ['value1', 'value2']}
    where_op : str
        If where_in is a dictionary and there are more than one key, then the operator that connects the where statements must be either 'AND' or 'OR'.
    from_date : str
        The start date in the form '2010-01-01'.
    to_date : str
        The end date in the form '2010-01-01'.
    min_count : int
        The minimum number of values required to return groupby_cols. Only works when groupby_cols and vlue_cols are str.
    con : SQLAlchemy connectable (engine/connection) or database string URI
        The sqlalchemy connection to be passed to pandas.read_sql

    Returns
    -------
    DataFrame
        Pandas DataFrame with MultiIndex of groupby_cols and date_col
    """

    ## Create where statement
    where_lst, where_temp = sql_where_stmts(where_in=where_in, where_op=where_op, from_date=from_date, to_date=to_date, date_col=date_col)

    ## Create ts statement and append earlier where statement
    if isinstance(groupby_cols, str):
        groupby_cols = [groupby_cols]
    col_names1 = ['[' + i.encode('ascii', 'ignore').decode() + ']' for i in groupby_cols]
    col_stmt = ', '.join(col_names1)

    ## Create sql stmt
    sql_stmt1 = sql_ts_agg_stmt(table, groupby_cols=groupby_cols, date_col=date_col, values_cols=values_cols, resample_code=resample_code, period=period, fun=fun, val_round=val_round, where_lst=where_lst)

    ## Create connection to database
    if con is None:
        con = create_engine('mssql', server, database, username=username, password=password)

    ## Make minimum count selection
    if (min_count is not None) & isinstance(min_count, int) & (len(groupby_cols) == 1):
        cols_count_str = ', '.join([groupby_cols[0], 'count(' + values_cols + ') as count'])
        if isinstance(where_lst, list):
            stmt1 = "SELECT " + cols_count_str + " FROM " + "(" + sql_stmt1 + ") as agg" + " GROUP BY " + col_stmt + " HAVING count(" + values_cols + ") >= " + str(min_count)
        else:
            stmt1 = "SELECT " + cols_count_str + " FROM " + table + " GROUP BY " + col_stmt + " HAVING count(" + values_cols + ") >= " + str(min_count)

        up_sites = pd.read_sql(stmt1, con)[groupby_cols[0]].tolist()
        up_sites = [str(i) for i in up_sites]

        if not up_sites:
            raise ValueError('min_count filtered out all sites.')

        where_in.update({groupby_cols[0]: up_sites})
        where_lst, where_temp = sql_where_stmts(where_in, where_op=where_op, from_date=from_date, to_date=to_date, date_col=date_col)

        ## Create sql stmt
        sql_stmt1 = sql_ts_agg_stmt(table, groupby_cols=groupby_cols, date_col=date_col, values_cols=values_cols, resample_code=resample_code, period=period, fun=fun, val_round=val_round, where_lst=where_lst)

    ## Create connection to database and execute sql statement
    df = pd.read_sql(sql_stmt1, con)

    ## Check to see if any data was found
    if df.empty:
        raise ValueError('No data was found in the database for the parameters given.')

    ## set the index
    df[date_col] = pd.to_datetime(df[date_col])
    groupby_cols.append(date_col)
    df1 = df.set_index(groupby_cols).sort_index()

    return df1


def to_mssql(df, server, database, table, index=False, dtype=None, schema=None, username=None, password=None):
    """
    Function to append a DataFrame onto an existing mssql table.

    Parameters
    ----------
    df : DataFrame
        DataFrame to be saved. The DataFrame column/index names must match those on the mssql table exactly.
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    index : bool
        Should the index be added as a column?
    dtype : dict of column name to SQL type, default None
        Optional specifying the datatype for columns. The SQL type should be an SQLAlchemy type.

    Returns
    -------
    None
    """
    ### Prepare the engine
    engine = create_engine('mssql', server, database, username=username, password=password)

    ### Save to mssql table
    df.to_sql(name=table, con=engine, if_exists='append', chunksize=1000, index=index, dtype=dtype, schema=schema)


def create_table(server, database, table, dtype_dict, primary_keys=None, foreign_keys=None, foreign_table=None, drop_table=False, con=None, username=None, password=None):
    """
    Function to create a table in an mssql database.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    dtype_dict : dict of str
        Dictionary of df columns to the associated sql data type. Examples below.
    primary_keys : str or list of str
        Index columns to define uniqueness in the data structure.
    foreign_keys : str or list of str
        Columns to link to another table in the same database.
    foreign_table: str
        The table in the same database with the identical foreign key(s).
    drop_table : bool
        If the table already exists, should it be dropped?

    Returns
    -------
    None
    """
    ### Make connection
    if con is None:
        engine = create_engine('mssql', server, database, username=username, password=password)
        con = engine.connect()

    ### Primary keys
    if isinstance(primary_keys, str):
        primary_keys = [primary_keys]
    if isinstance(primary_keys, list):
        pkey_stmt = ", Primary key (" + ", ".join(primary_keys) + ")"
    else:
        pkey_stmt = ""

    ### Foreign keys
    if isinstance(foreign_keys, str):
        foreign_keys = [foreign_keys]
    if isinstance(foreign_keys, list):
        fkey_stmt = ", Foreign key (" + ", ".join(foreign_keys) + ") " + "References " + foreign_table + "(" + ", ".join(foreign_keys) + ")"
    else:
        fkey_stmt = ""

    ### Initial create table statement
    d1 = [str(i) + ' ' + dtype_dict[i] for i in dtype_dict]
    d2 = ', '.join(d1)
    tab_create_stmt = "IF OBJECT_ID(" + str([str(table)])[1:-1] + ", 'U') IS NULL create table " + table + " (" + d2 + pkey_stmt + fkey_stmt + ")"

    trans = con.begin()
    try:

        ### Drop table option or check
        if drop_table:
            drop_stmt = "IF OBJECT_ID(" + str([str(table)])[1:-1] + ", 'U') IS NOT NULL DROP TABLE " + table
            con.execute(drop_stmt)
        else:
            check_tab_stmt = "IF OBJECT_ID(" + str([str(table)])[1:-1] + ", 'U') IS NOT NULL SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" +  str([str(table)])[1:-1]
#            tab1 = read_sql(check_tab_stmt, conn)
            con.execute(check_tab_stmt)
#            list1 = [i for i in conn]
#            if list1:
#                print('Table already exists. Returning the table info.')
#                df = pd.DataFrame(list1, columns=['columns', 'dtype'])
#                conn.close()
#                return df

        ### Create table in database
        con.execute(tab_create_stmt)
        trans.commit()
        con.close()

    except Exception as err:
        trans.rollback()
        con.close()
        raise err


def del_table_rows(server, database, table=None, pk_df=None, stmt=None, username=None, password=None):
    """
    Function to selectively delete rows from an mssql table.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str or None if stmt is a str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    pk_df : DataFrame
        A DataFrame of the primary keys of the table for the rows that should be removed.
    stmt : str
        SQL delete statement. Will override everything except server and database.

    Returns
    -------
    None

    Notes
    -----
    Using the pk_df is the only way to ensure that specific rows will be deleted from composite keys. The column data types and names of pk_df must match the equivelant columns in the SQL table. The procedure creates a temporary table from the pk_df then deletes the rows in the target table based on the temp table. Then finally deletes the temp table.
    """

    ### Make the delete statement
#    del_where_list = sql_where_stmts(**kwargs)
    if isinstance(stmt, str):
        del_rows_stmt = stmt
    elif isinstance(pk_df, pd.DataFrame):
        temp_tab = '#temp_del_tab1'

        ### Check the primary keys and unique keys
        if '.' in table:
            table1 = table.split('.')[1]
        else:
            table1 = table
        pk_stmt = get_pk_stmt.format(db=database, table=table1)
        pk = rd_sql(server, database, stmt=pk_stmt, username=username, password=password).name

        un_stmt = get_un_stmt.format(db=database, table=table1)
        un = rd_sql(server, database, stmt=un_stmt, username=username, password=password).name

        if pk.empty:
            raise ValueError('SQL table has no primary key. Please set one up.')
        if (not np.isin(pk, pk_df.columns.tolist()).all()) & (not np.isin(un, pk_df.columns.tolist()).all()):
            raise ValueError('The primary or unique keys in the SQL table does not match up with the pk_df')

        sel_t1 = "select * from " + temp_tab
        cols = pk_df.columns.tolist()
        tab_where = [table + '.' + i for i in cols]
        t1_where = [temp_tab + '.' + i for i in cols]
        where_list = [t1_where[i] + ' = ' + tab_where[i] for i in np.arange(len(cols))]
        where_stmt = " where " + " and ".join(where_list)
        exists_stmt = "(" + sel_t1 + where_stmt + ")"
        del_rows_stmt = "DELETE FROM " + table + " where exists " + exists_stmt
#    elif isinstance(del_where_list, list):
#        del_rows_stmt = "DELETE FROM " + table + " WHERE " + " AND ".join(del_where_list)
    else:
        raise ValueError('Please specify pk_df or stmt')

    ### Delete rows
    engine = create_engine('mssql', server, database, username=username, password=password)
    with engine.begin() as conn:
        if isinstance(pk_df, pd.DataFrame):
            pk_df.to_sql(name=temp_tab, con=conn, if_exists='replace', chunksize=1000)
        conn.execute(del_rows_stmt)


def update_table_rows(df, server, database, table, on=None, index=False, append=True, username=None, password=None):
    """
    Function to update rows from an mssql table. SQL table must have a primary key and the primary key must be in the input DataFrame.

    Parameters
    ----------
    df : DataFrame
        DataFrame with data to be overwritten in SQL table.
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    on : None, str, or list of str
        The index by which the update should be applied on. If None, then it uses the existing primary key(s).
    index : bool
        Does the df have an index that corresponds to the SQL table primary keys?
    append : bool
        Should new sites be appended to the table?

    Returns
    -------
    None
    """
    ### Check the primary keys
    if on is None:
        pk_stmt = get_pk_stmt.format(db=database, table=table)
        pk = rd_sql(server, database, stmt=pk_stmt, username=username, password=password).name.tolist()

        if not pk:
            raise ValueError('SQL table has no primary key. Please set one up or assign "on" explicitly.')
        on = pk
    elif isinstance(on, str):
        on = [on]

    ## Check that "on" are in the tables
    df_bool = ~np.isin(on, df.columns).all()
    if df_bool:
        raise ValueError('"on" contains column names that are not in the df')


    ### Make the update statement
    temp_tab = '#temp_up1'
    on_tab = [table + '.' + i for i in on]
    on_temp = [temp_tab + '.' + i for i in on]
    cols = df.columns.tolist()
    val_cols = [i for i in cols if not i in on]
    tab_list = [table + '.' + i for i in val_cols]
    temp_list = [temp_tab + '.' + i for i in val_cols]
    temp_list2 = [temp_tab + '.' + i for i in cols]
    up_list = [tab_list[i] + ' = ' + temp_list[i] for i in np.arange(len(temp_list))]
    on_list = [on_tab[i] + ' = ' + on_temp[i] for i in np.arange(len(on))]
#    up_stmt = "update " + table + " set " + ", ".join(up_list) + " from " + table + " inner join " + temp_tab + " on " + " and ".join(on_list)
    if append:
        up_stmt = "merge " + table + " using " + temp_tab + " on (" + " and ".join(on_list) + ") when matched then update set " + ", ".join(up_list) +  " WHEN NOT MATCHED BY TARGET THEN INSERT (" + ", ".join(cols) + ") values (" + ", ".join(temp_list2) + ");"
    else:
        up_stmt = "merge " + table + " using " + temp_tab + " on (" + " and ".join(on_list) + ") when matched then update set " + ", ".join(up_list) +  ";"

    ### Run SQL code to update rows
    engine = create_engine('mssql', server, database, username=username, password=password)
    with engine.begin() as conn:
        print('Saving data to temp table...')
        df.to_sql(temp_tab, con=conn, if_exists='replace', index=index, chunksize=1000)
        print('Updating primary table...')
        conn.execute(up_stmt)


def sql_where_stmts(where_in=None, where_op='AND', from_date=None, to_date=None, date_col=None):
    """
    Function to take various input parameters and convert them to a list of where statements for SQL.

    Parameters
    ----------
    where_in : str or dict
        Either a str with an associated where_val list or a dictionary of string keys to list values. If a str, it should represent the table column associated with the 'where' condition.
    where_in : dict
        A dictionary of strings to lists of strings.'. e.g.: {'SnapshotType': ['value1', 'value2']}
    where_op : str of either 'AND' or 'OR'
        The binding operator for the where conditions.
    from_date : str or None
        The start date in the form '2010-01-01'.
    to_date : str or None
        The end date in the form '2010-01-01'.
    date_col : str or None
        The SQL table column that contains the dates.

    Returns
    -------
    list of str or None
        Returns a list of str where conditions to be passed to an SQL execution function. The function needs to bind it with " where " + " and ".join(where_lst)
    """
    ### Where stmts
    where_stmt = []
    temp_where = {}

    if isinstance(where_in, dict):
        where_in_bool = {k: len(where_in[k]) > 20000 for k in where_in}
        for key, value in where_in.items():
            if not isinstance(value, list):
                raise ValueError('Values in the dict where_in must be lists.')
            if where_in_bool[key]:
                temp_where.update({key: value})
                where_stmt.append("{key} IN (select {key} from {temp_tab})".format(key=key, temp_tab='#temp_'+key.lower()))
            else:
                where_stmt.append("{key} IN ({values})".format(key=key, values=str(value)[1:-1]))

    if isinstance(from_date, str):
        from_date1 = pd.to_datetime(from_date, errors='coerce')
        if isinstance(from_date1, pd.Timestamp):
            from_date2 = str(from_date1)
            where_from_date = date_col + " >= " + from_date2.join(['\'', '\''])
        else:
            where_from_date = ''
    else:
        where_from_date = ''

    if isinstance(to_date, str):
        to_date1 = pd.to_datetime(to_date, errors='coerce')
        if isinstance(to_date1, pd.Timestamp):
            to_date2 = str(to_date1)
            where_to_date = date_col + " <= " + to_date2.join(['\'', '\''])
        else:
            where_to_date = ''
    else:
        where_to_date = ''

    where_stmt.extend([where_from_date, where_to_date])
    where_lst = [i for i in where_stmt if len(i) > 0]
    if len(where_lst) == 0:
        where_lst = None
    return where_lst, temp_where


def sql_ts_agg_stmt(table, groupby_cols, date_col, values_cols, resample_code, period=1, fun='mean', val_round=3, where_lst=None):
    """
    Function to create an SQL statement to pass to an SQL driver to resample a time series table.

    Parameters
    ----------
    table : str
        The SQL table name.
    groupby_cols : str or list of str
        The columns in the SQL table to grouped and returned with the time series data.
    date_col : str
        The date column in the table.
    values_cols : str or list of str
        The column(s) of the value(s) that should be resampled.
    resample_code : str
        The Pandas time series resampling code. e.g. 'D' for day, 'W' for week, 'M' for month, etc.
    period : int
        The number of resampling periods. e.g. period = 2 and resample = 'D' would be to resample the values over a 2 day period.
    fun : str
        The resampling function. i.e. mean, sum, count, min, or max. No median yet...
    val_round : int
        The number of decimals to round the values.
    where_lst : list or None
        A list of where statements to be passed and added to the final SQL statement.

    Returns
    -------
    str
        A full SQL statement that can be passed directly to an SQL connection driver through pandas read_sql function.
    """

    if isinstance(groupby_cols, (str, list)):
        groupby_cols_lst = list(groupby_cols)
    else:
        raise TypeError('groupby must be either a str or list of str.')
    if isinstance(values_cols, str):
        values_cols = [values_cols]
    elif not isinstance(values_cols, list):
        raise TypeError('values must be either a str or list of str.')

    pandas_dict = {'D': 'day', 'W': 'week', 'H': 'hour', 'M': 'month', 'Q': 'quarter', 'T': 'minute', 'A': 'year'}
    fun_dict = {'mean': 'avg', 'sum': 'sum', 'count': 'count', 'min': 'min', 'max': 'max'}

    groupby_str = ", ".join(groupby_cols_lst)
    values_lst = ["round(" + fun_dict[fun] + "(" + i + "), " + str(val_round) + ") AS " + i for i in values_cols]
    values_str = ", ".join(values_lst)

    if isinstance(where_lst, list):
        where_stmt = " where " + " and ".join(where_lst)
    else:
        where_stmt = ""

    if isinstance(resample_code, str):
        stmt1 = "SELECT " + groupby_str + ", DATEADD(" + pandas_dict[resample_code] + ", DATEDIFF(" + pandas_dict[resample_code] + ", 0, " + date_col + ")/ " + str(period) + " * " + str(period) + ", 0) AS " + date_col + ", " + values_str + " FROM " + table + where_stmt + " GROUP BY " + groupby_str + ", DATEADD(" + pandas_dict[resample_code] + ", DATEDIFF(" + pandas_dict[resample_code] + ", 0, " + date_col + ")/ " + str(period) + " * " + str(period) + ", 0)"
    else:
        groupby_cols_lst.extend([date_col])
        groupby_cols_lst.extend(values_cols)
        stmt1 = "SELECT " + ", ".join(groupby_cols_lst) + " FROM " + table + where_stmt

    return stmt1


def site_stat_stmt(table, site_col, values_col, fun):
    """
    Function to produce an SQL statement to make a basic summary grouped by a sites column.

    Parameters
    ----------

    table : str
        The database table.
    site_col : str
        The column containing the sites.
    values_col : str
        The column containing the values to be summarised.
    fun : str
        The function to apply.

    Returns
    -------
    str
        SQL statement.
    """
    fun_dict = {'mean': 'avg', 'sum': 'sum', 'count': 'count', 'min': 'min', 'max': 'max'}

    cols_str = ', '.join([site_col, fun_dict[fun] + '(' + values_col + ') as ' + values_col])
    stmt1 = "SELECT " + cols_str + " FROM " + table + " GROUP BY " + site_col
    return stmt1


#def sql_del_rows_stmt(table, **kwargs):
#    """
#    Function to create an sql statement to delete rows based on where statements.
#    """
#
#    where_list = sql_where_stmts(**kwargs)
#    stmt1 = "DELETE FROM " + table + " WHERE " + " and ".join(where_list)
#    return stmt1


def rd_sql_geo(server, database, table, col_stmt, where_lst=None, username=None, password=None):
    """
    Function to extract the geometry and coordinate system from an SQL geometry field. Returns a shapely geometry object and a proj4 str.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    where_lst : list
        A list of where statements to be passed and added to the final SQL statement.

    Returns
    -------
    list of shapely geometry objects
        The main output is a list of shapely geometry objects for all queried rows of the SQL table.
    str
        The second output is a proj4 str of the projection system.
    """

    ## Create connection to database
    engine = create_engine('mssql', server, database, username=username, password=password)

    geo_col_stmt1 = geo_col_stmt.format(table=table)
    geo_col = str(pd.read_sql(geo_col_stmt1, engine).iloc[0, 0])
    geo_srid_stmt1 = geo_srid_stmt.format(geo_col=geo_col, table=table)
    geo_srid = int(pd.read_sql(geo_srid_stmt1, engine).iloc[0, 0])
    if where_lst is not None:
        if len(where_lst) > 0:
            stmt2 = "SELECT " + col_stmt + ", " + geo_col + ".STAsBinary() as geometry" + " FROM " + table + " where " + " and ".join(where_lst)
        else:
            stmt2 = "SELECT " + col_stmt + ", " + geo_col + ".STAsBinary() as geometry" + " FROM " + table
    else:
        stmt2 = "SELECT " + col_stmt + ", " + geo_col + ".STAsBinary() as geometry" + " FROM " + table
    df2 = pd.read_sql(stmt2, engine)
    df2['geometry'] = df2.geometry.apply(lambda x: loads(x))
#    proj4 = from_epsg_code(geo_srid).to_proj4()
#    crs = {'init' :'epsg:' + str(geo_srid)}
    crs = parse.from_epsg_code(geo_srid).to_proj4()
    geo_df = GeoDataFrame(df2, geometry='geometry', crs=crs)

    return geo_df


def update_from_difference(df, server, database, table, on=None, index=False, append=True, mod_date_col=False, remove_rows=False, where_cols=None, username=None, password=None):
    """
    Function to update rows from an mssql table from the difference between a DataFrame and the existing SQL table.

    Parameters
    ----------
    df : DataFrame
        DataFrame with data to be overwritten in SQL table.
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    on : None, str, or list of str
        The index by which the update should be applied on. If None, then it uses the existing primary key(s).
    index : bool
        Does the df have an index that corresponds to the SQL table primary keys?
    append : bool
        Should new sites be appended to the table?
    mod_date_col : str or None
        Name of the modification date column to be updated. None if it doesn't exist.

    Returns
    -------
    DataFrame
        Of the results that were updated in SQL.
    """
    ### Check the primary keys
    if on is None:
        pk_stmt = get_pk_stmt.format(db=database, table=table)
        pk = rd_sql(server, database, stmt=pk_stmt, username=username, password=password).name.tolist()

        if not pk:
            raise ValueError('SQL table has no primary key. Please set one up or assign "on" explicitly.')
        on = pk
    elif isinstance(on, str):
        on = [on]

    ## Check that "on" are in the tables
    df_bool = ~np.isin(on, df.columns).all()
    if df_bool:
        raise ValueError('"on" contains column names that are not in the df')

    ### Preprocess dataframe
    if isinstance(df.index, pd.MultiIndex) | index:
        df1 = df.reset_index().copy()
    else:
        df1 = df.reset_index(drop=True).copy()

    if isinstance(where_cols, list):
        where_dict1 = {c: df1[c].unique().tolist() for c in where_cols if len(df1[c].unique().tolist()) < 1000}
    else:
        where_dict1 = {c: df1[c].unique().tolist() for c in on if len(df1[c].unique().tolist()) < 1000}

    if not where_dict1:
        where_dict1 = None

    ### Get SQL table data
    print('Get existing data...')
    old1 = rd_sql(server, database, table, df1.columns.tolist(), where_in=where_dict1, username=username, password=password)

    ## Make sure that only the relevant indexes are compared
    old2 = pd.merge(old1, df1[on], on=on, how='outer')

    ### Compare old to new
    print('Compare existing to new data...')
    comp_dict = compare_dfs(old2, df1, on)
    new1 = comp_dict['new']
    diff1 = comp_dict['diff']
    rem1 = comp_dict['remove'][on]

    both1 = pd.concat([new1, diff1])

    if not both1.empty:
        if isinstance(mod_date_col, str):
            run_time_start = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            both1[mod_date_col] = run_time_start
        print('New data found, updating tables...')
        update_table_rows(both1, server, database, table, on=on, append=append, username=username, password=password)

    if remove_rows:
        if not rem1.empty:
            del_table_rows(server, database, table, pk_df=rem1, username=username, password=password)

        return both1, rem1

    return both1, rem1


def backup_db(server, database, tables=None, output_path='', username=None, password=None):
    """
    Function to copy the tables in a database as individual parquet files.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    tables : list of str or None
        The tables to be copied. If None, all tables will be copied.
    output_path : str
        The base path where the database tables will be saved. This function will create an additional folder with the database name.

    Returns
    -------
    None

    """
    today1 = pd.Timestamp.today().strftime('%Y%m%dT%H%M')
    file_format = '{table}_{date}.parquet'

    ## Create connection to database
    engine = create_engine('mssql', server, database, username=username, password=password)

    ### Get table names
    if not isinstance(tables, list):
        get_tables_stmt1 = get_tables_stmt.format(dbName=database)
        tables = rd_sql(server, database, stmt=get_tables_stmt1, username=username, password=password).TABLE_NAME.tolist()

    ### Determine if any tables have geometry columns
    str_columns = str(tables)[1:-1]
    geo_col_stmt2 = "SELECT table_name, column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name in ({tables}) and DATA_TYPE='geometry'".format(tables=str_columns)

    geo_tables = pd.read_sql(geo_col_stmt2, engine)

    geo_table_list = geo_tables.table_name.unique().tolist()

    str_geo = str(geo_table_list)[1:-1]

    if not geo_tables.empty:
        other_col_stmt = "SELECT table_name, column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name in ({geo_tables}) and DATA_TYPE != 'geometry'".format(geo_tables=str_geo)
        other_cols = pd.read_sql(other_col_stmt, engine)

    ### Read and save all tables
    save_path = os.path.join(output_path, database)

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    for t in tables:
        print(t)
        if t in geo_table_list:
            cols = other_cols.loc[other_cols.table_name == t, 'column_name'].tolist()
            data1 = rd_sql(server, database, t, cols, username=username, password=password)
        else:
            data1 = rd_sql(server, database, t, username=username, password=password)

        data1.to_parquet(os.path.join(save_path,  file_format.format(table=t, date=today1)), index=False)

    print('success')




































