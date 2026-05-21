import duckdb
import requests
from bs4 import BeautifulSoup
import re
import os
import zipfile

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

def extrair_contabilidade():
    ano_recente = descobrir_ultimo_ano()
    if not ano_recente:
        raise ValueError("Não foi possível determinar o ano mais recente.")
        
    print(f"Extraindo dados contábeis do ano: {ano_recente}...")
    url_base = f"https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/{ano_recente}/"
    
    # Busca a lista de trimestres (T1, T2, T3, T4) dentro da pasta do ano
    response = requests.get(url_base)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    arquivos_zip = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith('.zip'):
            arquivos_zip.append(url_base + href)
            
    if not arquivos_zip:
        raise ValueError(f"Nenhum arquivo .zip encontrado na pasta de {ano_recente}.")
        
    # Prepara a pasta temporária para a contabilidade
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
    
    print("Processando dados contábeis locais...")
    con = duckdb.connect('banco_ans.db')
    
    # Processa todos os arquivos CSV extraídos na pasta. 
    # Forçamos all_varchar=true para blindar contra os erros de padrão contábil da ANS (311711X1)
    # Lógica customizada baseada na Lei 9656 (Saúde Suplementar) e DIOPS.
    query = f"""
    CREATE OR REPLACE TABLE contabilidade AS 
    SELECT 
        DATA,
        CAST(REG_ANS AS VARCHAR) AS REG_ANS,
        CAST(CD_CONTA_CONTABIL AS VARCHAR) AS CD_CONTA_CONTABIL,
        (COALESCE(CAST(REPLACE(VL_SALDO_FINAL, ',', '.') AS DOUBLE), 0) - 
         COALESCE(CAST(REPLACE(VL_SALDO_INICIAL, ',', '.') AS DOUBLE), 0)) AS SALDO_TRIMESTRE
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
    """
    con.execute(query)
    print("Dados contábeis extraídos e processados com sucesso.")
    con.close()

if __name__ == "__main__":
    extrair_contabilidade()
