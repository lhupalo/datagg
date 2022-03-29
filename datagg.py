"""

Criado em 28/03/2022 

por Luiz Hupalo

Soluções e Serviços

v1.0.2 => Correção para novo formato de extração do mongocsv
v1.1.0 => Nova funcionalidade de análise de baterias das estações
"""

import os
import sys
import time
import getopt
import pathlib
import xlsxwriter
import pandas as pd
from dask import dataframe as dd
from typing import ContextManager, Optional
from alive_progress import alive_bar

pd.options.mode.chained_assignment = None  # default='warn'

path = pathlib.Path().resolve()
os.chdir(f'{path}/data_input')

print("**************************************************************************************")
print("")
print("                            Data Aggregator - v1.1.0")
print("")
print("                                                                        by Luiz Hupalo")
print("                                                                               03/2022")
print("**************************************************************************************")
print("")

        
def csv_toERPanalysis(file_path):
    
    with spinner("Agregando dados de ERPs..."):
    
        
        # Extração dos dados obtidos pelo mongocsv
        ext = dd.read_csv(f'/{file_path}',dtype={'Value': 'object'})
        
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
        
        ##################################################################################
        
        
        ext_bateria = ext[ext['Tag'] == "EQPBATT"]
        ext_bateria['Value'] = ext_bateria['Value'].str[:2].astype(float)


        gb_batt = ext_bateria.groupby(['ERP','Tag','DateTime']).agg({'Value': ['mean']})
        gb_rbatt = gb_batt.reset_index()
        gb_rbatt = gb_rbatt.droplevel(1,axis=1)

        gb_bateria = gb_rbatt[gb_rbatt['Tag'] == 'EQPBATT']

        # Todos os dados tabulados, em valor completo e em porcentagem
        alldata_batt = gb_bateria.pivot_table(values = 'Value', index = ['ERP','DateTime'], columns = 'Tag', aggfunc = lambda x: ', '.join(x.astype(str)))
        alldata_batt = alldata_batt.reset_index()
        
    os.chdir(f'{path}/data_output')
    writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
    alldata_p.to_excel(writer,sheet_name='Dados')
    alldata_batt.to_excel(writer,sheet_name='Bateria')
    writer.save()
    print("")
    print("SUCESSO!")
    
    
def csv_toNERPanalysis(file_path):
    
    with spinner("Agregando dados de NERPs..."):
    
        
        # Extração dos dados obtidos pelo mongocsv
        ext = dd.read_csv(f'/{file_path}',dtype={'Value': 'object'})
        
        ext = ext.categorize(columns = ['ERP'])
        ext = ext.categorize(columns = ['DateTime'])
        ext = ext.categorize(columns = ['Tag'])
        ext = ext.compute()
        ext['DateTime'] = pd.to_datetime(ext['DateTime'], format='%Y-%m-%dT%H:%M:%S.%f%z')
        ext['DateTime'] = ext['DateTime'].dt.date
        
        gb = ext.groupby(['ERP','Tag','DateTime']).agg({'Value': ['count']})
        gb_r = gb.reset_index()
        gb_r = gb_r.droplevel(1,axis=1)
        
        gb_r['Value'] = (gb_r['Value']/24)*100
        gb_r.loc[gb_r['Value'] > 100,'Value'] = 100
        
        # Todos os dados tabulados, em valor completo e em porcentagem
        alldata_p = gb_r.pivot_table(values = 'Value', index = ['ERP','DateTime'], columns = 'Tag', aggfunc = lambda x: ', '.join(x.astype(str)))
        alldata_p = alldata_p.reset_index()
        
        ##################################################################################
        
        
        ext_bateria = ext[ext['Tag'] == "EQPBATT"]
        ext_bateria['Value'] = ext_bateria['Value'].str[:2].astype(float)


        gb_batt = ext_bateria.groupby(['ERP','Tag','DateTime']).agg({'Value': ['mean']})
        gb_rbatt = gb_batt.reset_index()
        gb_rbatt = gb_rbatt.droplevel(1,axis=1)

        gb_bateria = gb_rbatt[gb_rbatt['Tag'] == 'EQPBATT']

        # Todos os dados tabulados, em valor completo e em porcentagem
        alldata_batt = gb_bateria.pivot_table(values = 'Value', index = ['ERP','DateTime'], columns = 'Tag', aggfunc = lambda x: ', '.join(x.astype(str)))
        alldata_batt = alldata_batt.reset_index()
        
    os.chdir(f'{path}/data_output')
    writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
    alldata_p.to_excel(writer,sheet_name='Dados')
    alldata_batt.to_excel(writer,sheet_name='Bateria')
    writer.save()
    print("")
    print("SUCESSO!")

def spinner(title: Optional[str] = None) -> ContextManager:
    """
    Context manager to display a spinner while a long-running process is running.

    Usage:
        with spinner("Fetching data..."):
            fetch_data()

    Args:
        title: The title of the spinner. If None, no title will be displayed.
    """
    return alive_bar(monitor=None, stats=None, title=title)          

def main(argv):
   inputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      print("Uso:")
      print("")
      print('datagg.py -i <tipo de estacao>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print("Uso:")
         print("")
         print('datagg.py -i <tipo de estacao>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg

   return inputfile


if __name__ == "__main__":
   inputfile = main(sys.argv[1:])

if os.listdir() == [] or len(os.listdir()) > 1:
    print("")
    print("ERRO: arquivo .csv na pasta /data_input inválido")
    print("")
    print("SOLUÇÃO: a pasta /data_input deverá conter apenas um arquivo .csv obtido através do mongocsv")
else:
    for file in os.listdir():
        if file.endswith(".csv"):
            file_path = f'{path}/data_input/{file}'
            
            if inputfile == 'erp':
                csv_toERPanalysis(file_path)
            elif inputfile == 'nerp':
                csv_toNERPanalysis(file_path)
            else:
                print("ERRO")   