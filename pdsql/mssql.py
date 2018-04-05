# -*- coding: utf-8 -*-
"""
Functions for importing mssql data.
"""
import pandas as pd
import numpy as np
from pdsql.util import create_engine, save_df


def rd_sql(server, database, table=None, col_names=None, where_col=None, where_val=None, where_op='AND', geo_col=False, from_date=None, to_date=None, date_col=None, rename_cols=None, stmt=None, export_path=None):
    """
    Function to import data from an MSSQL database. Requires the pymssql package.

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
    where_col : str or dict
        Must be either a string with an associated where_val list or a dictionary of strings to lists.'. e.g.: 'SnapshotType' or {'SnapshotType': ['value1', 'value2']}
    where_val : list
        The WHERE query values for the where_col. e.g. ['value1', 'value2']
    where_op : str
        If where_col is a dictionary and there are more than one key, then the operator that connects the where statements must be either 'AND' or 'OR'.
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
    export_path : str
        The export path for a csv file if desired. If None, then nothing is exported.

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

        where_lst = sql_where_stmts(where_col=where_col, where_val=where_val, where_op=where_op, from_date=from_date,
                                    to_date=to_date, date_col=date_col)

        if isinstance(where_lst, list):
            stmt1 = "SELECT " + col_stmt + " FROM " + table + " where " + " and ".join(where_lst)
        else:
            stmt1 = "SELECT " + col_stmt + " FROM " + table

    elif isinstance(stmt, str):
        stmt1 = stmt

    else:
        raise ValueError('stmt must either be an SQL string or None.')

    ## Create connection to database and execute sql statement
    if geo_col & (stmt is None):
        df = rd_sql_geo(server=server, database=database, table=table, col_stmt=col_stmt, where_lst=where_lst)
        if rename_cols is not None:
            rename_cols.extend(['geometry'])
            df.columns = rename_cols
    else:
        engine = create_engine('mssql', server, database)
        df = pd.read_sql(stmt1, engine)
        if rename_cols is not None:
            df.columns = rename_cols

    ## save and return
    if export_path is not None:
        save_df(df, export_path, index=False)

    return df


def rd_sql_ts(server, database, table, groupby_cols, date_col, values_cols, resample_code=None, period=1, fun='mean',
              val_round=3, where_col=None, where_val=None, where_op='AND', from_date=None, to_date=None, min_count=None,
              export_path=None):
    """
    Function to specifically read and possibly aggregate time series data stored in MSSQL tables. Returns a MultiIndex DataFrame.

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
    where_col : str or dict
        Must be either a string with an associated where_val list or a dictionary of strings to lists.'. e.g.: 'SnapshotType' or {'SnapshotType': ['value1', 'value2']}
    where_val : list
        The WHERE query values for the where_col. e.g. ['value1', 'value2']
    where_op : str
        If where_col is a dictionary and there are more than one key, then the operator that connects the where statements must be either 'AND' or 'OR'.
    from_date : str
        The start date in the form '2010-01-01'.
    to_date : str
        The end date in the form '2010-01-01'.
    min_count : int
        The minimum number of values required to return groupby_cols. Only works when groupby_cols and vlue_cols are str.
    export_path : str
        The export path for a csv file if desired. If None, then nothing is exported.

    Returns
    -------
    DataFrame
        Pandas DataFrame with MultiIndex of groupby_cols and date_col
    """

    ## Create where statement
    where_lst = sql_where_stmts(where_col=where_col, where_val=where_val, where_op=where_op, from_date=from_date,
                                to_date=to_date, date_col=date_col)

    ## Create ts statement and append earlier where statement
    if isinstance(groupby_cols, str):
        groupby_cols = [groupby_cols]
    col_names1 = ['[' + i.encode('ascii', 'ignore').decode() + ']' for i in groupby_cols]
    col_stmt = ', '.join(col_names1)

    ## Create sql stmt
    sql_stmt1 = sql_ts_agg_stmt(table, groupby_cols=groupby_cols, date_col=date_col, values_cols=values_cols,
                                resample_code=resample_code, period=period, fun=fun, val_round=val_round,
                                where_lst=where_lst)

    ## Create connection to database
    engine = create_engine('mssql', server, database)

    ## Make minimum count selection
    if (min_count is not None) & isinstance(min_count, int) & (len(groupby_cols) == 1):
        cols_count_str = ', '.join([groupby_cols[0], 'count(' + values_cols + ') as count'])
        if isinstance(where_lst, list):
            stmt1 = "SELECT " + cols_count_str + " FROM " + "(" + sql_stmt1 + ") as agg" + " GROUP BY " + col_stmt + " HAVING count(" + values_cols + ") >= " + str(
                min_count)
        else:
            stmt1 = "SELECT " + cols_count_str + " FROM " + table + " GROUP BY " + col_stmt + " HAVING count(" + values_cols + ") >= " + str(
                min_count)

        up_sites = pd.read_sql(stmt1, engine)[groupby_cols[0]].tolist()
        up_sites = [str(i) for i in up_sites]

        if not up_sites:
            raise ValueError('min_count filtered out all sites.')

        if isinstance(where_col, str):
            where_col = {where_col: where_val}

        where_col.update({groupby_cols[0]: up_sites})
        where_lst = sql_where_stmts(where_col, where_op=where_op, from_date=from_date, to_date=to_date,
                                    date_col=date_col)

        ## Create sql stmt
        sql_stmt1 = sql_ts_agg_stmt(table, groupby_cols=groupby_cols, date_col=date_col, values_cols=values_cols,
                                    resample_code=resample_code, period=period, fun=fun, val_round=val_round,
                                    where_lst=where_lst)

    ## Create connection to database and execute sql statement
    df = pd.read_sql(sql_stmt1, engine)

    ## Check to see if any data was found
    if df.empty:
        raise ValueError('No data was found in the database for the parameters given.')

    ## set the index
    df[date_col] = pd.to_datetime(df[date_col])
    groupby_cols.append(date_col)
    df1 = df.set_index(groupby_cols).sort_index()

    ## Save and return
    if export_path is not None:
        save_df(df1, export_path)

    return df1


def to_mssql(df, server, database, table, index=False, dtype=None):
    """
    Function to append a DataFrame onto an existing mssql table.

    Parameters
    ----------
    df : DataFrame
        DataFrame to be saved.
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    index : bool
        Should the index be added as a column?
    dtype : dict of column name to SQL type, default None
        Optional specifying the datatype for columns. The SQL type should be a SQLAlchemy type.

    Returns
    -------
    None
    """
    ### Prepare the engine
    engine = create_engine('mssql', server, database)

    ### Save to mssql table
    df.to_sql(name=table, con=engine, if_exists='append', chunksize=1000, index=index, dtype=dtype)


def create_mssql_table(server, database, table, dtype_dict, primary_keys=None, foreign_keys=None, foreign_table=None, drop_table=False):
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
    engine = create_engine('mssql', server, database)
    conn = engine.connect()

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

    trans = conn.begin()
    try:

        ### Drop table option or check
        if drop_table:
            drop_stmt = "IF OBJECT_ID(" + str([str(table)])[1:-1] + ", 'U') IS NOT NULL DROP TABLE " + table
            conn.execute(drop_stmt)
        else:
            check_tab_stmt = "IF OBJECT_ID(" + str([str(table)])[1:-1] + ", 'U') IS NOT NULL SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" +  str([str(table)])[1:-1]
#            tab1 = read_sql(check_tab_stmt, conn)
            conn.execute(check_tab_stmt)
#            list1 = [i for i in conn]
#            if list1:
#                print('Table already exists. Returning the table info.')
#                df = pd.DataFrame(list1, columns=['columns', 'dtype'])
#                conn.close()
#                return df

        ### Create table in database
        conn.execute(tab_create_stmt)
        trans.commit()
        conn.close()

    except Exception as err:
        trans.rollback()
        conn.close()
        raise err


def del_mssql_table_rows(server, database, table=None, pk_df=None, stmt=None, **kwargs):
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
        A DataFrame of the primary keys of the table for the rows that should be removed. Will override anything in the kwargs.
    stmt : str
        SQL delete statement. Will override everything except server and database.
    **kwargs
        Any kwargs that can be passed to sql_where_stmts.

    Returns
    -------
    None

    Notes
    -----
    Using the pk_df is the only way to ensure that specific rows will be deleted from composite keys. The column data types and names of pk_df must match the equivelant columns in the SQL table. The procedure creates a temporary table from the pk_df then deletes the rows in the target table based on the temp table. Then finally deletes the temp table.
    """
    ### Make connection
    engine = create_engine('mssql', server, database)
    conn = engine.connect()

    ### Make the delete statement
    del_where_list = sql_where_stmts(**kwargs)
    if isinstance(stmt, str):
        del_rows_stmt = stmt
    elif isinstance(pk_df, pd.DataFrame):
        to_mssql(pk_df, server, database, 'temp_pk_table')
        sel_t1 = "select * from temp_pk_table"
        cols = pk_df.columns.tolist()
        tab_where = [table + '.' + i for i in cols]
        t1_where = ['temp_pk_table.' + i for i in cols]
        where_list = [t1_where[i] + ' = ' + tab_where[i] for i in np.arange(len(cols))]
        where_stmt = " where " + " and ".join(where_list)
        exists_stmt = "(" + sel_t1 + where_stmt + ")"
        del_rows_stmt = "DELETE FROM " + table + " where exists " + exists_stmt
    elif isinstance(del_where_list, list):
        del_rows_stmt = "DELETE FROM " + table + " WHERE " + " AND ".join(del_where_list)
    elif del_where_list is None:
        del_rows_stmt = "DELETE FROM " + table

    ### Delete rows
    trans = conn.begin()
    try:
        conn.execute(del_rows_stmt)
        conn.execute("IF OBJECT_ID('temp_pk_table', 'U') IS NOT NULL drop table temp_pk_table")
        trans.commit()
        conn.close()
    except Exception as err:
        conn.execute("IF OBJECT_ID('temp_pk_table', 'U') IS NOT NULL drop table temp_pk_table")
        trans.rollback()
        conn.close()
        raise err


def sql_where_stmts(where_col=None, where_val=None, where_op='AND', from_date=None, to_date=None, date_col=None):
    """
    Function to take various input parameters and convert them to a list of where statements for SQL.

    Parameters
    ----------
    where_col : str or dict
        Either a str with an associated where_val list or a dictionary of string keys to list values. If a str, it should represent the table column associated with the 'where' condition.
    where_val : list or None
        If where_col is a str, then where_val must be a list of associated condition values.
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

    if where_col is not None:
        if isinstance(where_col, str) & isinstance(where_val, list):
            #            if len(where_val) > 10000:
            #                raise ValueError('The number of values in where_val cannot be over 10000 (or so). MSSQL limitation. Break them into smaller chunks.')
            where_val = [str(i) for i in where_val]
            where_stmt = [str(where_col) + ' IN (' + str(where_val)[1:-1] + ')']
        elif isinstance(where_col, dict):
            where_stmt = [i + " IN (" + str([str(j) for j in where_col[i]])[1:-1] + ")" for i in where_col]
        else:
            raise ValueError(
                'where_col must be either a string with an associated where_val list or a dictionary of string keys to list values.')
    else:
        where_stmt = []

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
    return where_lst


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
        A full SQL statement that can be passed directly to an SQL connection driver like pymssql through pandas read_sql function.
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


def sql_del_rows_stmt(table, **kwargs):
    """
    Function to create an sql statement to row rows based on where statements.
    """

    where_list = sql_where_stmts(**kwargs)
    stmt1 = "DELETE FROM " + table + " WHERE " + " and ".join(where_list)
    return stmt1


def rd_sql_geo(server, database, table, col_stmt, where_lst=None):
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
    from geopandas import GeoDataFrame
    from shapely.wkt import loads
    from pycrs.parser import from_epsg_code

    ## Create connection to database
    engine = create_engine('mssql', server, database)

    geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" + "\'" + table + "\'" + " AND DATA_TYPE='geometry'"
    geo_col = str(pd.read_sql(geo_col_stmt, engine).iloc[0, 0])
    geo_srid_stmt = "select distinct " + geo_col + ".STSrid from " + table
    geo_srid = int(pd.read_sql(geo_srid_stmt, engine).iloc[0, 0])
    if where_lst is not None:
        if len(where_lst) > 0:
            stmt2 = "SELECT " + col_stmt + ", (" + geo_col + ".STGeometryN(1).ToString()) as geometry" + " FROM " + table + " where " + " and ".join(where_lst)
        else:
            stmt2 = "SELECT " + col_stmt + ", (" + geo_col + ".STGeometryN(1).ToString()) as geometry" + " FROM " + table
    else:
        stmt2 = "SELECT " + col_stmt + ", (" + geo_col + ".STGeometryN(1).ToString()) as geometry" + " FROM " + table
    df2 = pd.read_sql(stmt2, engine)
    geo = [loads(x) for x in df2.geometry]
    proj4 = from_epsg_code(geo_srid).to_proj4()
    geo_df = GeoDataFrame(df2.drop('geometry', axis=1), geometry=geo, crs=proj4)

    return geo_df
