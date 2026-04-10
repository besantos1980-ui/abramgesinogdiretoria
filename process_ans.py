import pandas as pd
import requests
import zipfile
import io
import json
import os
from datetime import datetime

# Configurações de Contas Contábeis (Padrão ANS - DIOPS)
# 411: Receitas / 412: Eventos (Sinistros) / 431: Despesas Administrativas
CONTAS = ['411', '412', '431']

def fetch_and_process():
    # URL do 4T2025 (Exemplo - Ajuste conforme a disponibilidade no portal)
    url = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/4T2025.zip"
    
    print(f"Iniciando download: {url}")
    try:
        response = requests.get(url, timeout=300)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
            
            # Acumuladores
            totais = {c: 0.0 for c in CONTAS}
            
            # Lógica de Chunk para não estourar RAM no GitHub Actions
            print(f"Processando arquivo: {csv_file}")
            chunks = pd.read_csv(
                z.open(csv_file), 
                sep=';', 
                encoding='latin-1', 
                decimal=',', 
                chunksize=150000,
                dtype={'CD_CONTA_CONTABIL': str}
            )
            
            for chunk in chunks:
                # Filtragem rápida por prefixo de conta
                mask = chunk['CD_CONTA_CONTABIL'].str.startswith(tuple(CONTAS), na=False)
                df_filtered = chunk[mask]
                
                for c in CONTAS:
                    valor = df_filtered[df_filtered['CD_CONTA_CONTABIL'].str.startswith(c)]['VL_SALDO_FINAL'].sum()
                    totais[c] += valor

        # Cálculos de Indicadores
        receita = totais['411']
        sinistro = abs(totais['412'])
        adm = abs(totais['431'])
        
        # Estrutura final para o React
        resultado = {
            "metadata": {
                "ultima_atualizacao": datetime.now().strftime("%d/%m/%Y %H:%M"),
                "status": "sucesso"
            },
            "evoluçãoGeral": [
                {
                    "ano": "2025",
                    "receita": round(receita / 1e9, 2),
                    "despesaAssistencial": round(sinistro / 1e9, 2),
                    "sinistralidade": round((sinistro / receita * 100), 1) if receita > 0 else 0,
                    "combinado": round(((sinistro + adm) / receita * 100), 1) if receita > 0 else 0
                }
            ],
            # Dados de segmentação podem ser fixos ou calculados conforme sua regra de negócio
            "dadosSegmentacaoOdonto": [
                {"nome": "Empresarial", "rec2025": round((receita * 0.7) / 1e9, 2), "share2025": 70.0, "sin2025": 42.1},
                {"nome": "Individual", "rec2025": round((receita * 0.2) / 1e9, 2), "share2025": 20.0, "sin2025": 48.5},
                {"nome": "Adesão", "rec2025": round((receita * 0.1) / 1e9, 2), "share2025": 10.0, "sin2025": 45.2}
            ]
        }

        with open('dados_odonto.json', 'w', encoding='utf-8') as f:
            json.dump(resultado, f, ensure_ascii=False, indent=2)
        
        print("✅ Arquivo dados_odonto.json gerado!")

    except Exception as e:
        print(f"❌ Erro: {str(e)}")

if __name__ == "__main__":
    fetch_and_process()
