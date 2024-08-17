import random
import uuid
import pandas as pd
import datetime as dt

dicionario = dict()

TAMANHO_DA_LISTA = 200    

def cria_produtos_dict():
    modelo = ["verao 25","zz","copa 14","darwin","brasilidades","olimpics"]

    genero = ["fem", "masc", "unissex"]

    categorias = ["acessorios","vestuario","calçados"]

    produtos = {"acessorios":["tiara","boné","chapéu","brinco","laço","pulseira","colar"],
        "vestuario":["casaco","moletom","jaqueta","calça jeans","calça sarja","calça social","camiseta","camisa","saia","vestido","meia"],
        "calçados":["sapato","tenis","chinelo","sandalia"]
    }

    data_dict = {
                    "id_produto":list(),
                    "categoria":list(),
                    "nome_produto":list(),
                    "fornecedor":list(),
                    "custo":list(),
                    "margem_lucro":list(),
                    "data_cadastro":list(),
                    "expira_em":list()
                }

    for n in range(0,TAMANHO_DA_LISTA):
        categoria_n = random.randint(0,2)
        categoria = categorias[categoria_n]
        produto = produtos[categorias[categoria_n]][random.randint(0,len(produtos[categorias[categoria_n]])-1)] + " - " + modelo[random.randint(0,len(modelo)-1)] + " - " + genero[random.randint(0,2)]

        data_dict["id_produto"].append(uuid.uuid4().hex[:16])
        data_dict["categoria"].append(categoria) 	
        data_dict["nome_produto"].append(produto)
        data_dict["fornecedor"].append(random.choice(["ABC atacado","Vem de vest","Fabril do barrio"]))
        data_dict["custo"].append(random.randint(20,100))
        data_dict["margem_lucro"].append(random.randint(10,150)/100)
        data_dict["data_cadastro"].append(dt.datetime(2023,random.randint(1,12),random.randint(1,28)))
        data_dict["expira_em"].append(dt.datetime(2023,random.randint(1,12),random.randint(1,28)))
    
    return data_dict

    
def adiciona_nomes_ao_gerador_de_nomes(nome="Joana",segundo_nome="Mara",sobrenome="Silva"):
    
    gerador_de_nomes = {"nome":["José","Ícaro","Maria","Miriam","Lídia","Laisla","Larissa","Olga","Jozi"],
                        "segundo_nome":["Maria","José","Praxedes","Costa","Tavares","Cândido","Januária","Pedro","Damares"],
                        "sobrenome":["Silva","da Silva","Oliveira","Mendes","da Costa","e Silva","Januário","Justino","Mariane"]
                        } 
    
    gerador_de_nomes['nome'].append(nome)
    gerador_de_nomes['segundo_nome'].append(segundo_nome)
    gerador_de_nomes['sobrenome'].append(sobrenome)
    
    iteracao = int((len(gerador_de_nomes['nome']) + len(gerador_de_nomes['segundo_nome']) + len(gerador_de_nomes['sobrenome'])) / 3)
    return gerador_de_nomes, iteracao

def gera_lista_de_nomes(adiciona_nomes_ao_gerador_de_nomes:callable,fator=1):
    """_summary_

    Args:
        adiciona_nomes_ao_gerador_de_nomes (callable): _description_

    Returns:
        _type_: _description_
        
    Example:
        gera_lista_de_nomes(adiciona_nomes_ao_gerador_de_nomes())

    """
    gerador_de_nomes, iteracao = adiciona_nomes_ao_gerador_de_nomes

    iteracao = iteracao - 1
    lista_de_nomes = list()

    for elemento in range(TAMANHO_DA_LISTA):
        nome = gerador_de_nomes["nome"][random.randint(0,iteracao)] + " " + gerador_de_nomes["segundo_nome"][random.randint(0,iteracao)] + " " + gerador_de_nomes["sobrenome"][random.randint(0,iteracao)]
        lista_de_nomes.append(nome)
        
    return lista_de_nomes

def gera_lista_de_niveis(nomes:list):

    niveis = list()
    for nivel in range(TAMANHO_DA_LISTA):
        n_niveis = nomes
        niveis.append(random.choice(n_niveis))
    return niveis

def gera_de_ids():

    ids = list()
    for n in range(TAMANHO_DA_LISTA):
        ids.append(uuid.uuid4().hex[:16])
        
    return ids


def cria_base_vendas(clientes:dict,vendedores:dict,produtos:dict):
    
    data_dict = {"id_venda":[],
                     "id_produto":[],
                     "id_cliente":[],
                     "quantidade":[],
                     "preco":[],
                     "valor":[],
                     "data_venda":[],
                     "id_vendedor":[]
                     }
    
    for n in range(0,TAMANHO_DA_LISTA*10):
        
        data_dict["id_venda"].append(uuid.uuid4().hex[:16])
        data_dict["id_produto"].append(produtos["id_produto"][random.randint(0,len(produtos["id_produto"])-1)]) 	
        data_dict["id_cliente"].append(clientes["id_cliente"][random.randint(0,len(clientes["id_cliente"])-1)]) 	
        data_dict["quantidade"].append(random.randint(1,5)) 		
        data_dict["preco"].append(0.0) 	
        data_dict["valor"].append(0.0) 	
        data_dict["data_venda"].append(dt.datetime(2023,random.randint(1,12),random.randint(1,28)))
        data_dict["id_vendedor"].append(vendedores["id_vendedor"][random.randint(0,len(vendedores["id_vendedor"])-1)]) 	

    return data_dict
		
