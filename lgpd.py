import pandas as pd # Biblioteca para trabalhar com dados em tabelas
import time # Biblioteca para calcular tempo
import functools # Biblioteca para criar decoradores, que são funções que recebem outras funções como argumento
from sqlalchemy import create_engine, text # Biblioteca para conectar programas Python com banco de dados, o create_engine é para criar o motor da conexão e o text é para poder utilizar a linguagem SQL
from datetime import datetime # Biblioteca para se trabalhar com data e hora

#  ATIVIDADE 4
def decorator_tempo(func): # Decorador que calcula o tempo para executar a atividade 2 e a atividade 3 
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        inicio = time.time()
        resultado = func(*args, **kwargs)
        fim = time.time()
        tempo_total = fim - inicio
        with open("log_execucao.txt", "a") as log:
            log.write(f"[{datetime.now()}] Função '{func.__name__}' executada em {tempo_total:.4f} segundos.\n")
        return resultado
    return wrapper


engine = create_engine("postgresql+psycopg2://alunos:AlunoFatec@200.19.224.150:5432/atividade2") # Cria uma engine para conectar o script ao banco de dados

#  ATIVIDADE 1
def LGPD(row):
    
    row_list = list(row)
    
    
    nome_partes = row_list[1].split() # Esse bloco de código é para anonimizar o nome, ele mostra a primeira letra do nome da pessoa e poem asteristico nas outras letras e mostra os sobrenomes
    if len(nome_partes) > 0:
        primeiro_nome = nome_partes[0]
        nome_partes[0] = primeiro_nome[0] + "*" * (len(primeiro_nome) - 1)
        row_list[1] = " ".join(nome_partes)

    
    row_list[2] = row_list[2][:4] + "***.***-**" # Essa linha é para anonimizar o CPF, ele mostra os 3 primeiros digitos e o restante mostra asteristico

    
    email_partes = row_list[3].split('@') # Esse bloco de código anonimiza o e-mail, ele mostra a primeira letra, mostra asteristico no restante até o @ e do @ em diante mostra o e-mail mesmo
    if len(email_partes) == 2:
        usuario = email_partes[0]
        row_list[3] = usuario[0] + "*" * (len(usuario) - 1) + "@" + email_partes[1]

    
    row_list[4] = row_list[4][-4:] # Essa linha de código anonimiza o telefone, ele só mostra os 4 últimos digitos

    return tuple(row_list)

#  ATIVIDADE 2
@decorator_tempo
def gerar_arquivos_por_ano(): # Função para gerar os arquivos de cada ano de nascimento
    query = "SELECT * FROM usuarios"
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    
    
    dados_anonimos = [LGPD(tuple(row)) for row in df.values] # Varíavel para converter o Data Frame em uma lista de tuplas para podermos usar a função LGPD original
    
    
    df_protegido = pd.DataFrame(dados_anonimos, columns=df.columns) # Varíavel para criar um novo Data Frame com os dados anonimizados
    
    
    df_protegido['ano'] = pd.to_datetime(df_protegido['data_nascimento']).dt.year # Linha de código para extrair o ano da coluna da data de nascimento
    
    
    for ano, grupo in df_protegido.groupby('ano'): # Loop para salvar um arquivo para cada ano
        
        grupo.drop(columns=['ano']).to_csv(f"{int(ano)}.csv", index=False) # Linha de código que remove a coluna auxiliar do ano antes de salvar
    print("Atividade 2 concluída: Arquivos por ano gerados.")

#  ATIVIDADE 3
@decorator_tempo
def gerar_arquivo_todos(): # Função para gerar um arquivo com todos os usuarios do banco de dados, só com o nome e CPF não anonimizados
    query = "SELECT nome, cpf FROM usuarios"
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    
    df.to_csv("todos.csv", index=False) # Linha para salvar apenas o nome e o CPF sem anonimização
    print("Atividade 3 concluída: Arquivo 'todos.csv' gerado.")


if __name__ == "__main__": # Varíavel que é usada para controlar a execução do código, ela permite que um arquivo funcione tanto como um script executável quanto como um módulo importável
    try: # Um tratamento de erro que tenta executar as funções para gerar os arquivos do ano e o arquivo de todos os usuários, caso de algum erro ele printa na tela erro na execução de acordo com o exception
        gerar_arquivos_por_ano()
        gerar_arquivo_todos()
    except Exception as e:
        print(f"Erro na execução: {e}")