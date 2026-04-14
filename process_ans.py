import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# --- CONFIGURAÇÕES ---
CONTAS_RECEITA = ['31112', '31172', '31192']
CONTAS_DESPESA = ['41112', '41122', '41132', '41142', '41152', '41162', '41172', '41182', '41192']

def obter_operadoras_odonto():
    """Busca a lista de operadoras exclusivamente odontológicas na ANS."""
    url = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_planos_de_saude_ativas/Relatorio_cadastral_das_operadoras_ativas.csv"
    try:
        print("🔍 Buscando cadastro de operadoras...")
        res = requests.get(url, timeout=60)
        df_cad = pd.read_csv(io.BytesIO(res.content), sep=';', encoding='latin-1', low_memory=False)
        # Filtro estrito por modalidade
        odonto = df_cad[df_cad['Modalidade'].isin(['Odontologia de Grupo', 'Cooperativa Odontológica'])]
        return set(odonto['Registro_ANS'].astype(str).str.strip().unique())
    except:
        print("⚠️ Falha ao obter lista. Usando lista vazia.")
        return set()

def processar_ans(ano, url, lista_odonto):
    try:
        print(f"📡 Processando dados de {ano}...")
        res = requests.get(url, timeout=300)
        
        # Dicionários para guardar valores por operadora
        db_receita = {}
        db_despesa = {}

        with zipfile.ZipFile(io.BytesIO(res.content)) as z:
            csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
            chunks = pd.read_csv(z.open(csv_file), sep=';', encoding='latin-1', chunksize=250000, low_memory=False)
            
            for chunk in chunks:
                chunk.columns = [c.upper().strip() for c in chunk.columns]
                chunk['REG_ANS'] = chunk['REG_ANS'].astype(str).str.strip()
                
                # FILTRO: Mantém apenas as odonto
                chunk = chunk[chunk['REG_ANS'].isin(lista_odonto)].copy()
                
                if not chunk.empty:
                    col_conta = 'CD_CONTA_CONTABIL'
                    col_valor = 'VL_SALDO_FINAL' if 'VL_SALDO_FINAL' in chunk.columns else 'VALOR'
                    chunk[col_valor] = pd.to_numeric(chunk[col_valor].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                    
                    # Soma Receitas por Operadora
                    rec_mask = chunk[chunk[col_conta].astype(str).str.strip().isin(CONTAS_RECEITA)]
                    for reg, val in rec_mask.groupby('REG_ANS')[col_valor].sum().items():
                        db_receita[reg] = db_receita.get(reg, 0) + val
                        
                    # Soma Despesas por Operadora
                    desp_mask = chunk[chunk[col_conta].astype(str).str.strip().isin(CONTAS_DESPESA)]
                    for reg, val in desp_mask.groupby('REG_ANS')[col_valor].sum().items():
                        db_despesa[reg] = db_despesa.get(reg, 0) + val

        # --- GERAÇÃO DO CSV DE AUDITORIA ---
        if db_receita:
            df_auditoria = pd.DataFrame([
                {'REG_ANS': k, 'RECEITA': v, 'DESPESA': abs(db_despesa.get(k, 0))} 
                for k, v in db_receita.items()
            ])
            # Salvando com nome fixo para o GitHub Actions encontrar
            df_auditoria.to_csv(f"auditoria_{ano}.csv", sep=';', index=False, encoding='latin-1')
            print(f"✅ CSV de auditoria gerado para {ano} com {len(df_auditoria)} empresas.")
        
        return {"receita": sum(db_receita.values()), "despesa": sum(db_despesa.values())}
    except Exception as e:
        print(f"❌ Erro: {e}")
        return None

def run():
    lista_odonto = obter_operadoras_odonto()
    urls = {"2024": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/4T2024.zip"}
    
    resultados = []
    for ano, url in urls.items():
        dados = processar_ans(ano, url, lista_odonto)
        if dados:
            resultados.append({
                "ano": ano,
                "receita": round(dados['receita'] / 1e9, 3),
                "despesa": round(dados['despesa'] / 1e9, 3)
            })
    
    with open('dados_odonto.json', 'w', encoding='utf-8') as f:
        json.dump(resultados, f, indent=2)
    print("🚀 Processo finalizado.")

if __name__ == "__main__":
    run()
    print("-> 'dados_odonto.json' atualizado para o dashboard.")

if __name__ == "__main__":
    fetch_and_process()
