import pandas as pd
from dask import dataframe as dd

ext = dd.read_csv("testebateria.csv",dtype={'Value': 'object'})
    
ext = ext.categorize(columns = ['ERP'])
ext = ext.categorize(columns = ['DateTime'])
ext = ext.categorize(columns = ['Tag'])
ext = ext.compute()
ext['DateTime'] = pd.to_datetime(ext['DateTime'], format='%Y-%m-%dT%H:%M:%S.%f%z')
ext['DateTime'] = ext['DateTime'].dt.date

gb = ext.groupby(['ERP','Tag','DateTime']).agg({'Value': ['count']})
gb_r = gb.reset_index()
gb_r = gb_r.droplevel(1,axis=1)

gb_r['Value'] = (gb_r['Value']/288)*100
gb_r.loc[gb_r['Value'] > 100,'Value'] = 100

# Todos os dados tabulados, em valor completo e em porcentagem
alldata_p = gb_r.pivot_table(values = 'Value', index = ['ERP','DateTime'], columns = 'Tag', aggfunc = lambda x: ', '.join(x.astype(str)))
alldata_p = alldata_p.reset_index()
    

writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
alldata_p.to_excel(writer,sheet_name='output')
writer.save()