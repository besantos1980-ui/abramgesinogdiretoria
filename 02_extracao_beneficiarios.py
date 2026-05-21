import duckdb
import requests
from bs4 import BeautifulSoup
import os
import zipfile

def extrair_beneficiarios():
    print("Iniciando contagem de beneficiários (SIB)...")
    
    url_diretorio = "https://dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/"
    resposta = requests.get(url_diretorio)
    soup = BeautifulSoup(resposta.text, 'html.parser')
    
    # Cria uma pasta temporária para salvar os arquivos
    pasta_temp = 'dados_sib'
    os.makedirs(pasta_temp, exist_ok=True)
    
    arquivos = []
    # Varre os links e pega os ZIPs do SIB ativo
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.startswith('sib_ativo_') and href.endswith('.zip'):
            arquivos.append(url_diretorio + href)
            
    if not arquivos:
        raise ValueError("Nenhum arquivo sib_ativo_*.zip foi encontrado na página.")
        
    print(f"Encontrados {len(arquivos)} arquivos estaduais. Baixando e extraindo localmente...")
    
    for url in arquivos:
        nome_arquivo = url.split('/')[-1]
        caminho_zip = os.path.join(pasta_temp, nome_arquivo)
        
        # Faz o download em pedaços (chunks) para não consumir muita memória RAM
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(caminho_zip, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        # Extrai o CSV de dentro do ZIP
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            zip_ref.extractall(pasta_temp)
            
        # Apaga o arquivo ZIP original para economizar espaço no GitHub
        os.remove(caminho_zip)
        
    print("Arquivos extraídos. Processando os dados locais com DuckDB...")
    
    con = duckdb.connect('banco_ans.db')
    
    # Agora apontamos para os arquivos CSV locais (pasta_temp/*.csv)
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
        '{pasta_temp}/*.csv', 
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
