USE estudos
--Criação das tabelas
CREATE TABLE vendedores (
	table_id		INT IDENTITY(1,1),
	id_vendedor		VARCHAR(100) PRIMARY KEY,
	nome_vendedor	VARCHAR(100) NOT NULL,
	nivel_cargo		VARCHAR(100)
)


CREATE TABLE clientes (
	table_id		INT IDENTITY(1,1),
	id_cliente		VARCHAR(100) PRIMARY KEY,
	nome_cliente	VARCHAR(100) NOT NULL,
	idade			TINYINT NOT NULL,
	uf				CHAR(2),
	cidade			VARCHAR(100)
)



CREATE TABLE produtos (

	table_id		INT IDENTITY(1,1),
	id_produto		VARCHAR(100) PRIMARY KEY,
	categoria		VARCHAR(100) NOT NULL,
	nome_produto	VARCHAR(100) NOT NULL,
	fornecedor		VARCHAR(100) NOT NULL,
	custo			DECIMAL(6,2) NOT NULL,
	margem_lucro	DECIMAL(6,2) NOT NULL,
	data_cadastro	DATETIME NOT NULL,
	expira_em		DATETIME NULL
	
)

CREATE TABLE vendas (

	table_id		INT IDENTITY(1,1),
	id_venda		VARCHAR(100) PRIMARY KEY,
	id_produto		VARCHAR(100) FOREIGN KEY REFERENCES produtos(id_produto)  NOT NULL,
	id_cliente		VARCHAR(100) FOREIGN KEY REFERENCES clientes(id_cliente)  NOT NULL,
	quantidade		INT NOT NULL,
	preco			DECIMAL(6,2) NOT NULL DEFAULT 0.00,
	valor			DECIMAL(6,2) NOT NULL DEFAULT 0.00,
	data_venda		DATETIME NOT NULL,
	id_vendedor		VARCHAR(100) FOREIGN KEY REFERENCES vendedores(id_vendedor)  NOT NULL,
)
