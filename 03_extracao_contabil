import duckdb
import requests
from bs4 import BeautifulSoup
import re

def descobrir_ultimo_ano():
    url = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    anos = []
    for link in soup.find_all('a'):
        href = link.get('href')
        # Busca pastas que sejam números de 4 dígitos (ex: 2025/)
        match = re.match(r'^(\d{4})/?$', href)
        if match:
            anos.append(int(match.group(1)))
            
    return max(anos) if anos else None

def extrair_contabilidade():
    ano_recente = descobrir_ultimo_ano()
    if not ano_recente:
        raise ValueError("Não foi possível determinar o ano mais recente.")
        
    print(f"Extraindo dados contábeis do ano: {ano_recente}...")
    url_dados = f"https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/{ano_recente}/*.zip"
    
    con = duckdb.connect('banco_ans.db')
    
    # all_varchar=true garante que códigos contábeis alfanuméricos não quebrem a importação
    query = f"""
    CREATE OR REPLACE TABLE contabilidade AS 
    SELECT 
        DATA,
        CAST(REG_ANS AS VARCHAR) AS REG_ANS,
        CAST(CD_CONTA_CONTABIL AS VARCHAR) AS CD_CONTA_CONTABIL,
        (COALESCE(CAST(REPLACE(VL_SALDO_FINAL, ',', '.') AS DOUBLE), 0) - 
         COALESCE(CAST(REPLACE(VL_SALDO_INICIAL, ',', '.') AS DOUBLE), 0)) AS SALDO_TRIMESTRE
    FROM read_csv_auto('{url_dados}', filename=true, all_varchar=true)
    """
    con.execute(query)
    print("Dados contábeis extraídos.")
    con.close()

if __name__ == "__main__":
    extrair_contabilidade()
