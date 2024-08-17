import requests
import json
from io import BytesIO,StringIO
import pandas as pd
from functions_packages.path_builder import path_definition,path_builder
import datetime as dt
from data_auxiliars.schemas import *
import pyarrow as pa
import pyarrow.parquet as pq
import deltalake as dtl
from deltalake import DeltaTable
from deltalake.writer import write_deltalake


def dq_define_schema_pandas(dataset:pd.DataFrame):
    """_summary_

    Args:
        df (pd.DataFrame): _description_
    """
    dataset = dataset.apply(pd.to_numeric, errors='ignore')

    return dataset   
    

def dq_define_schema_pyarrow(table_name):
    """_summary_

    Args:
        table_name (_type_): _description_

    Returns:
        _type_: _description_
    
    example:
        dq_define_schema("clientes")
    """
    
    PA_SCHEMA_CLIENTE = pa.schema([
    ("cidade", SCHEMAS["clientes"]["cidade"]),
    ("id_cliente", SCHEMAS["clientes"]["id_cliente"]),
    ("idade", SCHEMAS["clientes"]["idade"]),
    ("nome_cliente", SCHEMAS["clientes"]["nome_cliente"]),
    ("uf", SCHEMAS["clientes"]["uf"]),     
    ("load_date", SCHEMAS["clientes"]["load_date"])
    ]) 
    
    PA_SCHEMA_PRODUTOS = pa.schema([
    ("id_produto", SCHEMAS["produtos"]["id_produto"]),
    ("categoria", SCHEMAS["produtos"]["categoria"]),
    ("nome_produto", SCHEMAS["produtos"]["nome_produto"]),
    ("fornecedor", SCHEMAS["produtos"]["fornecedor"]),
    ("custo", SCHEMAS["produtos"]["custo"]),
    ("margem_lucro", SCHEMAS["produtos"]["margem_lucro"]),
    ("data_cadastro", SCHEMAS["produtos"]["data_cadastro"]),
    ("expira_em", SCHEMAS["produtos"]["expira_em"]),
    ("load_date", SCHEMAS["produtos"]["load_date"])
    ])

    PA_SCHEMA_VENDEDORES = pa.schema([
    ("id_vendedor", SCHEMAS["vendedores"]["id_vendedor"]),
    ("nome_vendedor", SCHEMAS["vendedores"]["nome_vendedor"]),
    ("nivel_cargo", SCHEMAS["vendedores"]["nivel_cargo"]), 
    ("load_date", SCHEMAS["vendedores"]["load_date"])
    ])
    
    PA_SCHEMA_VENDAS = pa.schema([
    ("id_venda", SCHEMAS["vendas"]["id_venda"]),
    ("id_produto", SCHEMAS["vendas"]["id_produto"]),
    ("id_cliente", SCHEMAS["vendas"]["id_cliente"]),
    ("quantidade", SCHEMAS["vendas"]["quantidade"]),
    ("preco", SCHEMAS["vendas"]["preco"]),     
    ("valor", SCHEMAS["vendas"]["valor"]),
    ("data_venda", SCHEMAS["vendas"]["data_venda"]),  
    ("id_vendedor", SCHEMAS["vendas"]["id_vendedor"]),
    ("load_date", SCHEMAS["vendas"]["load_date"])
  
    ])
    
    schemas = {"clientes":PA_SCHEMA_CLIENTE,
     "produtos":PA_SCHEMA_PRODUTOS,
     "vendedores":PA_SCHEMA_VENDEDORES,
     "vendas":PA_SCHEMA_VENDAS,
     }

    return schemas[table_name]

def is_api_available(table_name:str) -> list:
    """
    Args:
        table_name (str): Nome da tabela a qual se consulta via API

    Raises:
        Exception: Indisponibilidade do serviço

    Returns:
        str: lista com mensagem 'Disponíve', response e url
    """
    url = f"http://127.0.0.1:5000/api/tabela/{table_name}"
    response = requests.get(url)

    if str(response) == "<Response [200]>":
        return ["disponível",str(response),url]
    else:
        raise Exception(f"Url: {url} indisponível: {response}")

def get_data_from_api(table_name) -> tuple: 
    """_summary_

    Args:
        table_name (_type_): Nome da tabela a qual se consulta via API

    Returns:
        tuple: response.content,table_name
    """
    resp_list = is_api_available(table_name)
    if resp_list[0] == "disponível":
        response = requests.get(resp_list[2])
        # data = json.load(response.content)
    
    return response.content,table_name

def writeson_landing(table_name:str,layer=1,extension="json") -> object:
    """_summary_

    Args:
        table_name (str): Nome da tabela a qual se consulta via API
        layer (int, optional): Camada na qual será escrito o arquivo. Defaults to 1.
        extension (str, optional): Extensão do arquivo. Defaults to "json".
    Calls: 
        get_data_from_api(): Requisita API através do nome da tabela
        path_definition(): Define o diretório conforme table_name,layer e extension
        path_builder(): Cria o diretório, caso não exista

    Writes:
        object: Escreve o objeto no diretorio definido
    """
    
    data, table_name = get_data_from_api(table_name)

    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        src_layer=layer,
        table_name=table_name,
        src_extension=extension)

    path_builder(src_path)

    with BytesIO(data) as file_like_object:

        # Abre um arquivo local em modo binário para escrita
        with open(src_path, 'wb') as local_file:

            # Escreve o conteúdo do BytesIO no arquivo local
            local_file.write(file_like_object.getbuffer())



def read_from_landing(table_name:str,layer=1,extension="json") -> object:
    """_summary_

    Args:
        table_name (str): Nome da tabela a qual se consulta via API
        layer (int, optional): Camada na qual será escrito o arquivo. Defaults to 1.
        extension (str, optional): Extensão do arquivo. Defaults to "json".

    Returns:
        object: Pandas DataFrame
    """

    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        src_layer=layer,
        table_name=table_name,
        src_extension=extension)    

    with open(src_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        df = pd.DataFrame(data)

        return df    
    
def add_columns(df:pd.DataFrame,table_name:str) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.DataFrame): Pandas Dataframe a ser tratado
        table_name (str): Nome da tabela fonte do Dataframe

    Returns:
        pd.DataFrame: Pandas Dataframe com a nova coluna 'load_date'
    """
    df["load_date"] = dt.datetime.now()
    return df 
    

def writeson_bronze(table_name:str,layer=2,extension="parquet") -> object:
    """
    Args:
        table_name (str): Nome da tabela a ser coletada e escrita
        layer (int, optional): Camada na qual será escrito o arquivo. Defaults to 2.
        extension (str, optional): Extensão do arquivo. Defaults to "csv".

    Writes:
        object: csv file
    
    example: 
        writeson_bronze(table_name="vendedores")
    """
    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        dstn_layer=layer,
        table_name=table_name,
        dstn_extension=extension)
    
    path_builder(dstn_path)
    
    df = read_from_landing(table_name=table_name) 
    df = add_columns(df,table_name)
    df.drop(columns="table_id",inplace=True)
    df = dq_define_schema_pandas(df)
    
    table = pa.Table.from_pandas(df,schema=dq_define_schema_pyarrow(table_name))

    pq.write_table(table,dstn_path)
    
def writeson_silver(table_name:list,layer=3,extension="parquet") -> object:
    """
    Args:
        table_name (list): Lista de nomes das tabelas a ser coletadas, integradas e escritas
        layer (int, optional): Camada na qual será escrito o arquivo. Defaults to 3.
        extension (str, optional): Extensão do arquivo. Defaults to "csv".

    Writes:
        object: arquivo csv com a integração das tabelas produtos. clientes,vendedores e vendas
        
    writeson_gold(table_name="vendedores")
    """
    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        src_layer=2,
        dstn_layer=layer,
        table_name=table_name,
        src_extension="parquet",
        dstn_extension=extension)
    
    src = path_builder(src_path)
    dtsn = path_builder(dstn_path)
    
    df_dict = {}
    idx = 0
    for table in table_name:
        df = pd.read_parquet(src + table_name[idx] + "." +src_extension)
        df = df.drop(columns=["load_date"])
        df_dict[table] = df 
        
        idx += 1
        write_deltalake(dtsn + table,df,mode="overwrite")
        
def writeson_gold(table_name:list,layer=4,extension="parquet") -> object:
    """
    Args:
        table_name (list): Lista de nomes das tabelas a ser coletadas, integradas e escritas
        layer (int, optional): Camada na qual será escrito o arquivo. Defaults to 3.
        extension (str, optional): Extensão do arquivo. Defaults to "csv".

    Writes:
        object: arquivo csv com a integração das tabelas produtos. clientes,vendedores e vendas
        
    writeson_gold(table_name="vendedores")
    """
    src_path, src_extension,dstn_path,dstn_extension = path_definition(
        src_layer=3,
        dstn_layer=layer,
        table_name=table_name,
        src_extension="parquet",
        dstn_extension=extension)
    
    src = path_builder(src_path)
    dtsn = path_builder(dstn_path)
    
    df_dict = {}
    idx = 0
    for table in table_name: 
        
        delta_table_path = src + table

        dt = DeltaTable(delta_table_path)        

        df = dt.to_pandas() 

        df_dict[table] = df 
        idx += 1
    
    df = df_dict["vendas"] \
    .merge(df_dict["clientes"],on="id_cliente")  \
    .merge(df_dict["produtos"],on="id_produto") \
    .merge(df_dict["vendedores"],on="id_vendedor")  
    
    write_deltalake(dtsn + "obt",df,mode="overwrite")
        
    return 

if __name__ == "__main__":
    

    lst = ["vendas","produtos","clientes","vendedores"]

    for i in lst:

        writeson_landing(table_name=i)
        writeson_bronze(table_name=i)
        
writeson_silver(table_name=lst)
writeson_gold(table_name=lst)

      

