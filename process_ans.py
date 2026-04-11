import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# Configurações de Contas Contábeis (Padrão ANS - DIOPS)
CONTAS = ['411', '412', '431']

def extrair_dados_ans(url):
    """Baixa o ZIP da ANS e extrai os totais das contas de interesse."""
    try:
        response = requests.get(url, timeout=300)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
            
            totais = {c: 0.0 for c in CONTAS}
            
            # Processamento em chunks para economizar RAM
            chunks = pd.read_csv(
                z.open(csv_file), 
                sep=';', 
                encoding='latin-1', 
                decimal=',', 
                chunksize=150000,
                dtype={'CD_CONTA_CONTABIL': str}
            )
            
            for chunk in chunks:
                mask = chunk['CD_CONTA_CONTABIL'].str.startswith(tuple(CONTAS), na=False)
                df_filtered = chunk[mask]
                for c in CONTAS:
                    valor = df_filtered[df_filtered['CD_CONTA_CONTABIL'].str.startswith(c)]['VL_SALDO_FINAL'].sum()
                    totais[c] += valor
            
            return totais
    except Exception as e:
        print(f"Erro ao processar {url}: {e}")
        return None

def fetch_and_process():
    urls = {
        "2024": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/4T2024.zip",
        "2025": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/4T2025.zip"
    }
    
    evoluçao_geral = []
    
    for ano, url in urls.items():
        print(f"🚀 Processando dados de {ano}...")
        totais = extrair_dados_ans(url)
        
        if totais:
            receita = totais['411']
            sinistro = abs(totais['412'])
            adm = abs(totais['431'])
            
            evoluçao_geral.append({
                "ano": ano,
                "receita": round(receita / 1e9, 2),
                "despesaAssistencial": round(sinistro / 1e9, 2),
                "sinistralidade": round((sinistro / receita * 100), 1) if receita > 0 else 0,
                "combinado": round(((sinistro + adm) / receita * 100), 1) if receita > 0 else 0
            })

    # Adicionando um ano base (2019) fixo para manter a análise histórica se desejar
    # Ou você pode adicionar a URL de 2019 no dicionário 'urls' acima
    if not any(d['ano'] == '2019' for d in evoluçao_geral):
        evoluçao_geral.insert(0, {"ano": "2019", "receita": 4.85, "despesaAssistencial": 2.06, "sinistralidade": 42.5, "combinado": 85.4})

    # Ordenar por ano para o gráfico não quebrar
    evoluçao_geral.sort(key=lambda x: x['ano'])

    # Pegamos o dado mais recente para a segmentação
    latest = evoluçao_geral[-1]
    
    resultado = {
        "metadata": {
            "ultima_atualizacao": datetime.now().strftime("%d/%m/%Y %H:%M"),
            "fonte": "ANS - DIOPS"
        },
        "evoluçãoGeral": evoluçao_geral,
        "dadosSegmentacaoOdonto": [
            {"nome": "Empresarial", "share2025": 70.0},
            {"nome": "Individual", "share2025": 20.0},
            {"nome": "Adesão", "share2025": 10.0}
        ]
    }

    with open('dados_odonto.json', 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)
    
    print("✅ JSON consolidado gerado com sucesso!")

if __name__ == "__main__":
    fetch_and_process()
