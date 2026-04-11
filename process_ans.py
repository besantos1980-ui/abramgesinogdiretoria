import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# Definição das contas conforme sua especificação
CONTAS_RECEITA = ['31112', '31172', '31192']
CONTAS_DESPESA = ['41112', '41122', '41132', '41142', '41152', '41162', '41172', '41182', '41192']

def extrair_dados_ans(url):
    try:
        print(f"📡 Baixando dados: {url}")
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
            
            # Acumuladores para as suas fórmulas
            soma_receita = 0.0
            soma_despesa = 0.0
            
            chunks = pd.read_csv(
                z.open(csv_file), 
                sep=';', 
                encoding='latin-1', 
                chunksize=200000,
                low_memory=False
            )
            
            for chunk in chunks:
                # Normalização de colunas
                chunk.columns = [c.upper().strip() for c in chunk.columns]
                col_conta = 'CD_CONTA_CONTABIL'
                col_valor = 'VL_SALDO_FINAL' if 'VL_SALDO_FINAL' in chunk.columns else 'VALOR'
                
                # Garantir tipos de dados corretos
                chunk[col_conta] = chunk[col_conta].astype(str).str.strip()
                chunk[col_valor] = pd.to_numeric(chunk[col_valor].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                
                # 1. Cálculo da Receita Assistencial Líquida (Soma direta conforme sua fórmula)
                mask_rec = chunk[col_conta].isin(CONTAS_RECEITA)
                soma_receita += chunk[mask_rec][col_valor].sum()
                
                # 2. Cálculo da Despesa Assistencial (Soma direta das contas 411x2)
                mask_desp = chunk[col_conta].isin(CONTAS_DESPESA)
                soma_despesa += chunk[mask_desp][col_valor].sum()
            
            # Nota: Como 31172 e 31192 já são negativas no DIOPS, a soma direta 
            # (31112 + 31172 + 31192) resultará na Receita Líquida correta.
            
            return {
                "receita": soma_receita,
                "despesa": abs(soma_despesa) # Garantindo valor absoluto para o dashboard
            }

    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        return None

def fetch_and_process():
    urls = {
        "2024": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/4T2024.zip",
        "2025": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/4T2025.zip"
    }
    
    evoluçao_geral = []
    
    for ano, url in urls.items():
        dados = extrair_dados_ans(url)
        if dados and dados['receita'] != 0:
            rec = dados['receita']
            desp = dados['despesa']
            
            evoluçao_geral.append({
                "ano": ano,
                "receita": round(rec / 1e9, 2),
                "despesaAssistencial": round(desp / 1e9, 2),
                "sinistralidade": round((desp / rec * 100), 1) if rec > 0 else 0,
                # Combinado estimado (mantendo ADM em 15.5% para este exemplo)
                "combinado": round(((desp + (rec * 0.155)) / rec * 100), 1) if rec > 0 else 0
            })

    # Ordenação e Metadata
    evoluçao_geral.sort(key=lambda x: x['ano'])
    
    output = {
        "metadata": {"ultima_atualizacao": datetime.now().strftime("%d/%m/%Y %H:%M")},
        "evoluçãoGeral": evoluçao_geral,
        "dadosSegmentacaoOdonto": [
            {"nome": "Empresarial", "share2025": 70.0},
            {"nome": "Individual", "share2025": 20.0},
            {"nome": "Adesão", "share2025": 10.0}
        ]
    }

    with open('dados_odonto.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print("✅ JSON atualizado com as contas específicas!")

if __name__ == "__main__":
    fetch_and_process()
