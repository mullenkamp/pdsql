# -*- coding: utf-8 -*-
"""
Created on Thu Apr  5 14:19:37 2018

@author: michaelek
"""
import pandas as pd
import numpy as np
#import pytest
from pdsql import mssql
#from shapely import wkb
#import geopandas as gpd

pd.options.display.max_columns = 10

##########################################
### Parameters

server = 'sql2012dev01'
database = 'Hydro'
table = 'test_table1'

server = 'SQL2012PROD05'
database = 'GISPUBLIC'
table = 'PLAN_NZTM_SURFACE_WATER_ALLOCATION_ZONES'
col_names =  ['ZONE_GROUP_NAME', 'ZONE_NAME']
geo_col = True

server = 'edwdev01'
database = 'PlanLimits'


df1 = pd.DataFrame([np.array([1, 1, 1, 2, 3, 4, 4, 4, 5, 5]), np.array([0, 1, 2, 0, 1, 0, 1, 2, 0, 1]), np.arange(10, 20)]).T
df1.columns = ['pk1', 'pk2', 'val']

df2 = pd.DataFrame([np.array([1, 2, 4, 4, 5]), np.array([0, 0, 0, 2, 1])]).T
df2.columns = ['pk1', 'pk2']

#########################################
### Tests


def test_del_rows():
    ## Write table
    mssql.to_mssql(df1, server, database, table)

    ## Read table
    beta1 = mssql.rd_sql(server, database, table)

    ## Delete parts
    mssql.del_mssql_table_rows(server, database, table, pk_df=df2)

    ## Test
    beta2 = mssql.rd_sql(server, database, table)
    beta3 = beta1.set_index(['pk1', 'pk2'])
    beta3.index.isin(df2.set_index(['pk1', 'pk2']).index)
    beta4 = beta3.loc[~beta3.index.isin(df2.set_index(['pk1', 'pk2']).index)].reset_index()

    ## Remove table
    engine = mssql.create_engine('mssql', server, database)
    conn = engine.connect()
    trans = conn.begin()
    conn.execute("IF OBJECT_ID(" + str([str(table)])[1:-1] + ", 'U') IS NOT NULL drop table " + table)
    trans.commit()
    conn.close()

    assert all(beta2 == beta4)




gpd1 = mssql.rd_sql_geo(server, database, table, ', '.join(col_names))

engine = mssql.create_engine('mssql', gis_server, gis_database)
stmt1 = 'select ZONE_NAME, shape.STAsBinary() as geometry from ' + table2
stmt2 = 'select ZONE_NAME, shape.STGeometryN(1).ToString() as geometry from ' + table2

df2 = pd.read_sql(stmt1, engine)
df2['geometry'] = df2.geometry.apply(lambda x: wkb.loads(x))

gpd1 = gpd.GeoDataFrame(df2, geometry='geometry')
beta1 = mssql.rd_sql(server, database, table, col_names, geo_col = True)




