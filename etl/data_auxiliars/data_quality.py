from .schemas import SCHEMAS
import pyarrow as pa


def dq_define_schema(table_name):
    
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
    
    schemas = {table_name:PA_SCHEMA_CLIENTE,
     table_name:PA_SCHEMA_PRODUTOS,
     table_name:PA_SCHEMA_VENDEDORES,
     table_name:PA_SCHEMA_VENDAS,
     }

    return schemas
    
