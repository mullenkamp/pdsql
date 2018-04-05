# -*- coding: utf-8 -*-
"""
Created on Thu Apr  5 14:19:37 2018

@author: michaelek
"""
import pandas as pd
import numpy as np
import pytest
from pdsql.mssql import to_mssql, del_mssql_table_rows, rd_sql, create_engine

##########################################
### Parameters

server = 'sql2012dev01'
database = 'Hydro'
table = 'test_table1'

df1 = pd.DataFrame([np.array([1, 1, 1, 2, 3, 4, 4, 4, 5, 5]), np.array([0, 1, 2, 0, 1, 0, 1, 2, 0, 1]), np.arange(10, 20)]).T
df1.columns = ['pk1', 'pk2', 'val']

df2 = pd.DataFrame([np.array([1, 2, 4, 4, 5]), np.array([0, 0, 0, 2, 1])]).T
df2.columns = ['pk1', 'pk2']

#########################################
### Tests


def test_del_rows():
    ## Write table
    to_mssql(df1, server, database, table)

    ## Read table
    beta1 = rd_sql(server, database, table)

    ## Delete parts
    del_mssql_table_rows(server, database, table, pk_df=df2)

    ## Test
    beta2 = rd_sql(server, database, table)
    beta3 = beta1.set_index(['pk1', 'pk2'])
    beta3.index.isin(df2.set_index(['pk1', 'pk2']).index)
    beta4 = beta3.loc[~beta3.index.isin(df2.set_index(['pk1', 'pk2']).index)].reset_index()

    ## Remove table
    engine = create_engine('mssql', server, database)
    conn = engine.connect()
    trans = conn.begin()
    conn.execute("IF OBJECT_ID(" + str([str(table)])[1:-1] + ", 'U') IS NOT NULL drop table " + table)
    trans.commit()
    conn.close()

    assert all(beta2 == beta4)











