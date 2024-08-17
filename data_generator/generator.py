import datetime
from datetime import date
from dotenv import load_dotenv
import json
import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import pyodbc
import uuid
from auxiliar import * 

table_params = {"rows_limit":100}

# Carrega as variáveis do arquivo .env
load_dotenv(r"C:\Users\SALA443\Desktop\Projetos\use_cases\data_series_3\.env")



# data = {"nome":["josé","maria","larissa"],"idade":[52,62,63]}

def pyodbc_insert_params(data_dict:dict) -> list:  
     """
     Args:
          None
          
     Raises:
          None
          
     Returns:
          str: lista com argumentos para 
                      
     Example:
          pyodbc_insert_params(data_dict)   
     """        
     param1 = tuple(data_dict.keys())
     
     param2 = "? " * len(param1)
     param2 = param2.replace(" ",",").rstrip(",")
     
     param1 = str(tuple(param1)).replace("'","")
     param2 = str(tuple(param2)).replace("', '","").replace("'","")
               
     return [param1, param2]

def set_pyodbc_cursor() -> object:
     
     # Configurações do banco de dados
     server = os.getenv("server")
     database = os.getenv("database")
     username = os.getenv("username_")
     password = os.getenv("password")
     driver = os.getenv("driver")
     
     cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
     
     cursor = cnxn.cursor()
     
     return cursor, cnxn

def writes_vendedores_into_sqlserver(dataframe:pd.DataFrame,tabela,cursor:object,cnxn:object,columns_names:str,question_marks:str):
     for index, row in dataframe.iterrows():
          cursor.execute(f"INSERT INTO dbo.{tabela} {columns_names} values{question_marks}", row.id_vendedor, row.nome_vendedor, row.nivel_cargo)
     cnxn.commit()
     cursor.close()

def writes_clientes_into_sqlserver(dataframe:pd.DataFrame,tabela,cursor:object,cnxn:object,columns_names:str,question_marks:str):
     for index, row in dataframe.iterrows():
          cursor.execute(f"INSERT INTO dbo.{tabela} {columns_names} values{question_marks}", row.id_cliente, row.nome_cliente, row.idade, row.uf, row.cidade)
     cnxn.commit()
     cursor.close()
	

def writes_produtos_into_sqlserver(dataframe:pd.DataFrame,tabela,cursor:object,cnxn:object,columns_names:str,question_marks:str):
     for index, row in dataframe.iterrows():
          cursor.execute(f"INSERT INTO dbo.{tabela} {columns_names} values{question_marks}", row.id_produto, row.categoria, row.nome_produto, row.fornecedor, row.custo, row.margem_lucro, row.data_cadastro, row.expira_em)
     cnxn.commit()
     cursor.close()

def writes_vendas_into_sqlserver(dataframe:pd.DataFrame,tabela,cursor:object,cnxn:object,columns_names:str,question_marks:str):
     for index, row in dataframe.iterrows():
          cursor.execute(f"INSERT INTO dbo.{tabela} {columns_names} values{question_marks}", row.id_venda, row.id_produto, row.id_cliente, row.quantidade, row.preco, row.valor, row.data_venda, row.id_vendedor)
     cnxn.commit()
     cursor.close()

if __name__ == "__main__":
          
     vendedores = {
     "id_vendedor":gera_de_ids(),
     "nome_vendedor":gera_lista_de_nomes(adiciona_nomes_ao_gerador_de_nomes(),fator=10),
     "nivel_cargo":gera_lista_de_niveis(["jr","pl","sr"])}
     
     param1, param2 = pyodbc_insert_params(vendedores)      

     cursor,cnxn = set_pyodbc_cursor()
     
     vendedores_df = pd.DataFrame(vendedores)    
     
     writes_vendedores_into_sqlserver(
          dataframe=vendedores_df,
          tabela="vendedores",
          cursor=cursor,
          cnxn=cnxn,
          columns_names=param1,
          question_marks=param2
          )
     
     
     clientes = {
     "id_cliente":gera_de_ids(),
     "nome_cliente":gera_lista_de_nomes(adiciona_nomes_ao_gerador_de_nomes()),
     "idade":gera_lista_de_niveis([20,52,32,26,51,41,52,62,61,21,32,18,19,44]),
     "uf":gera_lista_de_niveis(["sp"]),
     "cidade":gera_lista_de_niveis(["Jundiaí","São Paulo","Vinhedo","São José","São Bernardo","Itaquaquecetuba"])}
     
     param1, param2 = pyodbc_insert_params(clientes)      

     cursor,cnxn = set_pyodbc_cursor()
     
     clientes_df = pd.DataFrame(clientes)   
     writes_clientes_into_sqlserver(
     dataframe=clientes_df,
     tabela="clientes",
     cursor=cursor,
     cnxn=cnxn,
     columns_names=param1,
     question_marks=param2
     )

     produtos = cria_produtos_dict()

     param1, param2 = pyodbc_insert_params(produtos)      

     cursor,cnxn = set_pyodbc_cursor()
     produtos_df = pd.DataFrame(produtos)

     writes_produtos_into_sqlserver(
     dataframe=produtos_df,
     tabela="produtos",
     cursor=cursor,
     cnxn=cnxn,
     columns_names=param1,
     question_marks=param2
     )


     vendas = cria_base_vendas(clientes,vendedores,produtos)
     param1, param2 = pyodbc_insert_params(vendas)      

     cursor,cnxn = set_pyodbc_cursor()
     
     vendas_df = pd.DataFrame(vendas)
     
     vendas_df.drop(columns=["preco","valor"],inplace=True)
     vendas_df = vendas_df.merge(produtos_df[["id_produto","custo","margem_lucro"]],on="id_produto")
     vendas_df["preco"] = vendas_df["custo"] * (1 + vendas_df["margem_lucro"])
     vendas_df["valor"] = vendas_df["preco"] * vendas_df["quantidade"]
     vendas_df.drop(columns=["custo"],inplace=True)
     
     writes_vendas_into_sqlserver(
     dataframe=vendas_df,
     tabela="vendas",
     cursor=cursor,
     cnxn=cnxn,
     columns_names=param1,
     question_marks=param2
     )
     
     








