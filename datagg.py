"""

Criado em 28/03/2022 

por Luiz Hupalo

Soluções e Serviços
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

path = pathlib.Path().resolve()
os.chdir(f'{path}/data_input')

print("*******************************************")
print("       Data Aggregation - v1.0.1")
print("")
print("                             by Luiz Hupalo")
print("                                    03/2022")
print("*******************************************")
print("")

        
def csv_toERPanalysis(file_path):
    
    with spinner("Agregando dados de ERPs..."):
    
        
        # Extração dos dados obtidos pelo mongocsv
        ext = dd.read_csv(f'/{file_path}',dtype={'Valor': 'object'})
        
        ext = ext.categorize(columns = ['ERP'])
        ext = ext.categorize(columns = ['Data'])
        ext = ext.categorize(columns = ['tag'])
        ext = ext.compute()
        ext['Data'] = pd.to_datetime(ext['Data'], format='%Y-%m-%dT%H:%M:%S.%f%z')
        ext['Data'] = ext['Data'].dt.date
        
        gb = ext.groupby(['ERP','tag','Data']).agg({'Valor': ['count']})
        gb_r = gb.reset_index()
        gb_r = gb_r.droplevel(1,axis=1)
        
        gb_r['Valor'] = (gb_r['Valor']/288)*100
        gb_r.loc[gb_r['Valor'] > 100,'Valor'] = 100
        
        # Todos os dados tabulados, em valor completo e em porcentagem
        alldata_p = gb_r.pivot_table(values = 'Valor', index = ['ERP','Data'], columns = 'tag', aggfunc = lambda x: ', '.join(x.astype(str)))
        alldata_p = alldata_p.reset_index()
        
    os.chdir(f'{path}/data_output')
    writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
    alldata_p.to_excel(writer,sheet_name='output')
    writer.save()
    
    
def csv_toNERPanalysis(file_path):
    
    with spinner("Agregando dados de NERPs..."):
    
        
        # Extração dos dados obtidos pelo mongocsv
        ext = dd.read_csv(f'/{file_path}',dtype={'Valor': 'object'})
        
        ext = ext.categorize(columns = ['ERP'])
        ext = ext.categorize(columns = ['Data'])
        ext = ext.categorize(columns = ['tag'])
        ext = ext.compute()
        ext['Data'] = pd.to_datetime(ext['Data'], format='%Y-%m-%dT%H:%M:%S.%f%z')
        ext['Data'] = ext['Data'].dt.date
        
        gb = ext.groupby(['ERP','tag','Data']).agg({'Valor': ['count']})
        gb_r = gb.reset_index()
        gb_r = gb_r.droplevel(1,axis=1)
        
        gb_r['Valor'] = (gb_r['Valor']/24)*100
        gb_r.loc[gb_r['Valor'] > 100,'Valor'] = 100
        
        # Todos os dados tabulados, em valor completo e em porcentagem
        alldata_p = gb_r.pivot_table(values = 'Valor', index = ['ERP','Data'], columns = 'tag', aggfunc = lambda x: ', '.join(x.astype(str)))
        alldata_p = alldata_p.reset_index()
        
    os.chdir(f'{path}/data_output')
    writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
    alldata_p.to_excel(writer,sheet_name='output')
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