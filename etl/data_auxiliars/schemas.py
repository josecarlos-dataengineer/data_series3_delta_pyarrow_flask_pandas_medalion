import pyarrow as pa

SCHEMAS = {
    "clientes":
        {
        "id_cliente":pa.string(),
        "nome_cliente":pa.string(),
        "idade":pa.int64(),
        "uf":pa.string(),
        "cidade":pa.string(),
        "load_date":pa.timestamp('us')
        },
        
    "produtos":
        {
        "id_produto":pa.string(),
        "categoria":pa.string(),
        "nome_produto":pa.string(),
        "fornecedor":pa.string(),
        "custo":pa.float64(),
        "margem_lucro":pa.float64(),
        "data_cadastro": pa.string(),
        "expira_em":pa.string(),
        "load_date":pa.timestamp('us')
        },
    "vendedores":
        {
        "id_vendedor":pa.string(),
        "nome_vendedor":pa.string(),
        "nivel_cargo":pa.string(),
        "load_date":pa.timestamp('us')

        },
    "vendas":
        {
        "id_venda":pa.string(),
        "id_produto":pa.string(),
        "id_cliente":pa.string(),
        "quantidade":pa.int64(),
        "preco":pa.float64(),
        "valor":pa.float64(),
        "data_venda":pa.string(),
        "id_vendedor":pa.string(),
        "load_date":pa.timestamp('us')
        },
            }


