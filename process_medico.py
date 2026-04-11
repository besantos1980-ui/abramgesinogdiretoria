import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# Regras sugeridas para Medicina (Sufixo 1)
CONTAS_REC_MED = ['31111', '31171', '31191']
CONTAS_DESP_MED = [f'411{i}1' for i in range(1, 9)] # 41111 até 41191

def extrair_dados_medico(url):
    try:
        response = requests.get(url, timeout=300)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
            s_rec, s_desp = 0.0, 0.0
            
            chunks = pd.read_csv(z.open(csv_file), sep=';', encoding='latin-1', chunksize=200000)
            
            for chunk in chunks:
                chunk.columns = [c.upper().strip() for c in chunk.columns]
                col_c = 'CD_CONTA_CONTABIL'
                col_v = 'VL_SALDO_FINAL' if 'VL_SALDO_FINAL' in chunk.columns else 'VALOR'
                
                chunk[col_c] = chunk[col_c].astype(str).str.strip()
                chunk[col_v] = pd.to_numeric(chunk[col_v].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                
                s_rec += chunk[chunk[col_c].isin(CONTAS_REC_MED)][col_v].sum()
                s_desp += chunk[chunk[col_c].isin(CONTAS_DESP_MED)][col_v].sum()
            
            return {"receita": s_rec, "despesa": abs(s_desp)}
    except Exception as e:
        print(f"Erro: {e}")
        return None

def fetch_and_process():
    urls = {
        "2024": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/4T2024.zip",
        "2025": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/1T2025.zip"
    }
    
    evolução = []
    for ano, url in urls.items():
        d = extrair_dados_medico(url)
        if d:
            evolução.append({
                "ano": ano,
                "receita": round(d['receita'] / 1e9, 2),
                "despesaAssistencial": round(d['despesa'] / 1e9, 2),
                "sinistralidade": round((d['despesa'] / d['receita'] * 100), 1) if d['receita'] > 0 else 0
            })

    with open('dados_medico.json', 'w', encoding='utf-8') as f:
        json.dump({"evoluçãoGeral": evolução}, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    fetch_and_process()
