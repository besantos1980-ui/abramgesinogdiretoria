import duckdb
import requests
from bs4 import BeautifulSoup

def extrair_beneficiarios():
    print("Iniciando contagem de beneficiários (SIB)...")
    
    url_diretorio = "https://dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/"
    print(f"Acessando diretório: {url_diretorio}")
    
    resposta = requests.get(url_diretorio)
    soup = BeautifulSoup(resposta.text, 'html.parser')
    
    arquivos = []
    # Varremos todos os links e filtramos os que são .zip do SIB ativo
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.startswith('sib_ativo_') and href.endswith('.zip'):
            arquivos.append(url_diretorio + href)
            
    if not arquivos:
        raise ValueError("Nenhum arquivo sib_ativo_*.zip foi encontrado na página.")
        
    print(f"Encontrados {len(arquivos)} arquivos estaduais. Processando a contagem...")
    
    lista_urls_sql = "[" + ", ".join([f"'{url}'" for url in arquivos]) + "]"
    
    con = duckdb.connect('banco_ans.db')
    
    # Modo "trator" ativado: strict_mode=false, null_padding=true e encoding CP1252
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
    FROM read_csv(
        {lista_urls_sql}, 
        delim=';', 
        header=true, 
        encoding='CP1252',
        all_varchar=true, 
        ignore_errors=true,
        strict_mode=false,
        null_padding=true
    )
    GROUP BY REGISTRO_OPERADORA;
    """
    
    con.execute(query)
    print("Porte das operadoras calculado com sucesso.")
    con.close()

if __name__ == "__main__":
    extrair_beneficiarios()
