import duckdb
import requests
from bs4 import BeautifulSoup

def extrair_beneficiarios():
    print("Iniciando contagem de beneficiários (SIB)...")
    
    # 1. Lemos a página do diretório da ANS para descobrir os arquivos reais
    url_diretorio = "https://dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/"
    print(f"Acessando diretório: {url_diretorio}")
    
    resposta = requests.get(url_diretorio)
    soup = BeautifulSoup(resposta.text, 'html.parser')
    
    arquivos = []
    # 2. Varremos todos os links e filtramos os de beneficiários
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.startswith('sib_ativo_'):
            arquivos.append(url_diretorio + href)
            
    if not arquivos:
        raise ValueError("Nenhum arquivo sib_ativo_* foi encontrado na página.")
        
    print(f"Encontrados {len(arquivos)} arquivos estaduais. Processando a contagem...")
    
    # 3. Formatamos a lista de links para o padrão que o DuckDB entende: ['link1', 'link2']
    lista_urls_sql = "[" + ", ".join([f"'{url}'" for url in arquivos]) + "]"
    
    con = duckdb.connect('banco_ans.db')
    
    # 4. Inserimos a lista inteira no read_csv_auto. 
    # Adicionei ignore_errors=true porque arquivos grandes da ANS sempre têm algumas linhas corrompidas.
    query = f"""
    CREATE OR REPLACE TABLE porte_operadoras AS
    SELECT 
        CAST(REGISTRO_OPERADORA AS VARCHAR) AS REG_ANS,
        COUNT(*) AS total_vidas,
        CASE 
            WHEN COUNT(*) <= 19999 THEN 'Pequeno'
            WHEN COUNT(*) BETWEEN 20000 AND 99999 THEN 'Médio'
            ELSE 'Grande'
        END AS Porte
    FROM read_csv_auto({lista_urls_sql}, all_varchar=true, ignore_errors=true)
    GROUP BY REGISTRO_OPERADORA;
    """
    
    con.execute(query)
    print("Porte das operadoras calculado com sucesso.")
    con.close()

if __name__ == "__main__":
    extrair_beneficiarios()
