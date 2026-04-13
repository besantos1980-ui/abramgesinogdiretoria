import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# Configurações
CONTAS_RECEITA = ['31112', '31172', '31192']
CONTAS_DESPESA = ['41112', '41122', '41132', '41142', '41152', '41162', '41172', '41182', '41192']

def obter_lista_exclusiva_odonto():
    """
    Busca a base cadastral da ANS e filtra apenas operadoras odontológicas.
    """
    url_cadastral = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_planos_de_saude_ativas/Relatorio_cadastral_das_operadoras_ativas.csv"
    try:
        print("🔍 Acessando cadastro da ANS para filtrar operadoras...")
        # Lendo o cadastro (importante: o separador da ANS é ';' e encoding 'latin-1')
        df_cad = pd.read_csv(url_cadastral, sep=';', encoding='latin-1', low_memory=False)
        
        # Filtrando as modalidades exclusivas de Odontologia
        # 'Odontologia de Grupo' e 'Cooperativa Odontológica' são os termos oficiais
        modalidades_odonto = ['Odontologia de Grupo', 'Cooperativa Odontológica']
        df_odonto = df_cad[df_cad['Modalidade'].isin(modalidades_odonto)]
        
        # Criamos um set de strings para comparação rápida
        lista_reg_ans = set(df_odonto['Registro_ANS'].astype(str).str.strip().unique())
        
        print(f"✅ Filtro preparado: {len(lista_reg_ans)} operadoras exclusivamente odontológicas encontradas.")
        return lista_reg_ans
    except Exception as e:
        print(f"❌ Erro ao obter lista de operadoras: {e}")
        return set()

def extrair_dados_ans(url, lista_odonto):
    try:
        print(f"📡 Processando: {url}")
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
            
            soma_receita = 0.0
            soma_despesa = 0.0
            
            chunks = pd.read_csv(
                z.open(csv_file), 
                sep=';', 
                encoding='latin-1', 
                chunksize=250000, # Chunk menor para evitar estouro de memória no processamento
                low_memory=False
            )
            
            for chunk in chunks:
                # Padronização de colunas
                chunk.columns = [c.upper().strip() for c in chunk.columns]
                
                # --- O FILTRO CRUCIAL ---
                # Garante que REG_ANS seja string e remove espaços
                chunk['REG_ANS'] = chunk['REG_ANS'].astype(str).str.strip()
                
                # Mantém no chunk APENAS as linhas onde a operadora está na nossa lista odonto
                chunk = chunk[chunk['REG_ANS'].isin(lista_odonto)].copy()
                
                if chunk.empty:
                    continue
                # ------------------------

                col_conta = 'CD_CONTA_CONTABIL'
                col_valor = 'VL_SALDO_FINAL' if 'VL_SALDO_FINAL' in chunk.columns else 'VALOR'
                
                # Limpeza de valores numéricos
                chunk[col_valor] = pd.to_numeric(
                    chunk[col_valor].astype(str).str.replace(',', '.'), 
                    errors='coerce'
                ).fillna(0)
                
                # Soma das Receitas (31112, 31172, 31192)
                mask_rec = chunk[col_conta].astype(str).str.strip().isin(CONTAS_RECEITA)
                soma_receita += chunk[mask_rec][col_valor].sum()
                
                # Soma das Despesas (411x2)
                mask_desp = chunk[col_conta].astype(str).str.strip().isin(CONTAS_DESPESA)
                soma_despesa += chunk[mask_desp][col_valor].sum()
            
            return {"receita": soma_receita, "despesa": abs(soma_despesa)}

    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        return None

def main():
    # 1. Obtém as operadoras alvo
    lista_odonto = obter_lista_exclusiva_odonto()
    
    if not lista_odonto:
        print("Interrompido: Lista de operadoras vazia.")
        return

    # 2. URLs (Exemplo com 2024 e 2025)
    urls = {
        "2024": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/4T2024.zip",
        "2025": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/1T2025.zip"
    }
    
    # ... Resto da lógica de processamento e exportação para JSON ...
