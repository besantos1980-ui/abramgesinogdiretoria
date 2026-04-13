import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# Configurações de filtragem
CONTAS_RECEITA = ['31112', '31172', '31192']
CONTAS_DESPESA = ['41112', '41122', '41132', '41142', '41152', '41162', '41172', '41182', '41192']
URL_OPERADORAS = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_planos_de_saude_ativas/Relatorio_cadastral_das_operadoras_ativas.csv"

def obter_operadoras_odonto():
    """
    Busca a lista de operadoras ativas e filtra apenas as de modalidade Odontologia.
    """
    try:
        print("🔍 Buscando lista de operadoras exclusivamente odontológicas...")
        # O separador costuma ser ';' e o encoding 'latin-1' nos arquivos da ANS
        df_ops = pd.read_csv(URL_OPERADORAS, sep=';', encoding='latin-1')
        
        # Filtrar pela modalidade "Odontologia de Grupo"
        # Nota: Você pode ajustar esse filtro se quiser incluir Cooperativas Odontológicas
        odonto_filtros = ['Odontologia de Grupo', 'Cooperativa Odontológica']
        df_odonto = df_ops[df_ops['Modalidade'].isin(odonto_filtros)]
        
        # Retorna um set de Registros ANS para busca rápida (O(1))
        return set(df_odonto['Registro_ANS'].astype(str).str.strip())
    except Exception as e:
        print(f"⚠️ Não foi possível filtrar operadoras: {e}. Processando todas.")
        return None

def extrair_dados_ans(url, lista_odonto):
    try:
        print(f"📡 Baixando e filtrando dados: {url}")
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
                chunksize=300000,
                low_memory=False
            )
            
            for chunk in chunks:
                chunk.columns = [c.upper().strip() for c in chunk.columns]
                
                # 1. Filtro de Operadoras (O PONTO CHAVE)
                if lista_odonto:
                    # Garante que a coluna de Registro ANS existe e está limpa
                    col_reg = 'REG_ANS'
                    chunk[col_reg] = chunk[col_reg].astype(str).str.strip()
                    chunk = chunk[chunk[col_reg].isin(lista_odonto)]

                if chunk.empty:
                    continue

                # 2. Normalização de valores
                col_conta = 'CD_CONTA_CONTABIL'
                col_valor = 'VL_SALDO_FINAL' if 'VL_SALDO_FINAL' in chunk.columns else 'VALOR'
                
                chunk[col_conta] = chunk[col_conta].astype(str).str.strip()
                chunk[col_valor] = pd.to_numeric(chunk[col_valor].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                
                # 3. Cálculos
                mask_rec = chunk[col_conta].isin(CONTAS_RECEITA)
                soma_receita += chunk[mask_rec][col_valor].sum()
                
                mask_desp = chunk[col_conta].isin(CONTAS_DESPESA)
                soma_despesa += chunk[mask_desp][col_valor].sum()
            
            return {"receita": soma_receita, "despesa": abs(soma_despesa)}

    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        return None

def fetch_and_process():
    # Passo 1: Obter o filtro de operadoras
    lista_odonto = obter_operadoras_odonto()
    
    urls = {
        "2024": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/4T2024.zip",
        "2025": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/1T2025.zip" # Exemplo (ajuste conforme disponibilidade)
    }
    
    evolucao_geral = []
    
    for ano, url in urls.items():
        dados = extrair_dados_ans(url, lista_odonto)
        if dados and dados['receita'] != 0:
            rec = dados['receita']
            desp = dados['despesa']
            
            evolucao_geral.append({
                "ano": ano,
                "receita": round(rec / 1e6, 2), # Exemplo em Milhões para Odonto (ajuste se preferir Bilhões)
                "despesaAssistencial": round(desp / 1e6, 2),
                "sinistralidade": round((desp / rec * 100), 1) if rec > 0 else 0,
                "combinado": round(((desp + (rec * 0.155)) / rec * 100), 1) if rec > 0 else 0
            })

    # ... (resto do código de salvamento permanece igual)
