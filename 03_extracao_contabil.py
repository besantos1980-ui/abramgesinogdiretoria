import duckdb
import requests
from bs4 import BeautifulSoup
import re
import os
import zipfile
import pandas as pd

def descobrir_ultimo_ano():
    url = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    anos = []
    for link in soup.find_all('a'):
        href = link.get('href')
        match = re.match(r'^(\d{4})/?$', href)
        if match:
            anos.append(int(match.group(1)))
            
    return max(anos) if anos else None

def processar_contabilidade():
    ano_recente = descobrir_ultimo_ano()
    if not ano_recente:
        raise ValueError("Não foi possível determinar o ano mais recente.")
        
    print(f"Extraindo dados contábeis do ano: {ano_recente}...")
    url_base = f"https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/{ano_recente}/"
    
    response = requests.get(url_base)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    arquivos_zip = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith('.zip'):
            arquivos_zip.append(url_base + href)
            
    if not arquivos_zip:
        raise ValueError(f"Nenhum arquivo .zip encontrado na pasta de {ano_recente}.")
        
    pasta_temp = 'dados_contabeis'
    os.makedirs(pasta_temp, exist_ok=True)
    
    print(f"Encontrados {len(arquivos_zip)} trimestres. Baixando e extraindo...")
    
    for url in arquivos_zip:
        nome_arquivo = url.split('/')[-1]
        caminho_zip = os.path.join(pasta_temp, nome_arquivo)
        
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(caminho_zip, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            zip_ref.extractall(pasta_temp)
            
        os.remove(caminho_zip)
    
    print("Iniciando motor analítico no DuckDB...")
    con = duckdb.connect('banco_ans.db')
    
    # 1. Carrega toda a base sem perdas
    query_importacao = f"""
    CREATE OR REPLACE TABLE contabilidade_bruta AS 
    SELECT 
        DATA,
        TRIM(CAST(REG_ANS AS VARCHAR)) AS REG_ANS,
        TRIM(CAST(CD_CONTA_CONTABIL AS VARCHAR)) AS CD_CONTA_CONTABIL,
        (COALESCE(CAST(REPLACE(VL_SALDO_FINAL, ',', '.') AS DOUBLE), 0) - 
         COALESCE(CAST(REPLACE(VL_SALDO_INICIAL, ',', '.') AS DOUBLE), 0)) AS SALDO_TRIMESTRE
    FROM read_csv(
        '{pasta_temp}/*.csv', 
        delim=';', 
        header=true, 
        encoding='CP1252',
        all_varchar=true, 
        quote='"',          
        escape='"',         
        ignore_errors=true  
    )
    """
    con.execute(query_importacao)

   # 2. Aplica a Regra de Negócio com IGUALDADE EXATA.
    # O comando IN () garante que apenas as contas listadas sejam somadas,
    # ignorando qualquer subconta (como 3111, 3112, etc).
    query_financas = """
    WITH CalculoContas AS (
        SELECT 
            DATA,
            REG_ANS,
            
            -- Contraprestações Efetivas: EXATAMENTE 311, 312, 313, 32
            SUM(CASE 
                WHEN CD_CONTA_CONTABIL IN ('311', '312', '313', '32') 
                THEN SALDO_TRIMESTRE ELSE 0 
            END) AS Contraprestacoes_Efetivas,
            
            -- Eventos Indenizáveis: EXATAMENTE 411, 412
            SUM(CASE 
                WHEN CD_CONTA_CONTABIL IN ('411', '412') 
                THEN SALDO_TRIMESTRE ELSE 0 
            END) AS Eventos_Indenizaveis
            
        FROM contabilidade_bruta
        GROUP BY DATA, REG_ANS
    )
    SELECT * 
    FROM CalculoContas
    WHERE Contraprestacoes_Efetivas <> 0 OR Eventos_Indenizaveis <> 0
    ORDER BY REG_ANS, DATA;
    """

    print("Calculando Contraprestações e Eventos Indenizáveis por Operadora...")
    df_auditoria = con.execute(query_financas).df()
    
    arquivo_excel = "resultado_operadoras.xlsx"
    df_auditoria.to_excel(arquivo_excel, index=False)
    
    print(f"SUCESSO! Os dados calculados por REG_ANS e filtrados pelas contas foram salvos em: {arquivo_excel}")
    con.close()

if __name__ == "__main__":
    processar_contabilidade()
