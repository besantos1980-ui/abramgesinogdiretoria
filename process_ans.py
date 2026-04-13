import pandas as pd
import requests
import zipfile
import io
import json
from datetime import datetime

# --- CONFIGURAÇÕES ---
CONTAS_RECEITA = ['31112', '31172', '31192']
CONTAS_DESPESA = ['41112', '41122', '41132', '41142', '41152', '41162', '41172', '41182', '41192']

def obter_operadoras_exclusivamente_odonto():
    """
    Busca no portal de Dados Abertos da ANS a lista de operadoras ativas
    e filtra apenas aquelas classificadas como Odontologia.
    """
    url_cadastral = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_planos_de_saude_ativas/Relatorio_cadastral_das_operadoras_ativas.csv"
    try:
        print("🔍 1. Buscando lista de operadoras no cadastro da ANS...")
        response = requests.get(url_cadastral, timeout=60)
        df_cad = pd.read_csv(io.BytesIO(response.content), sep=';', encoding='latin-1', low_memory=False)
        
        # Filtro de Modalidade: Apenas Odontologia de Grupo e Cooperativas Odontológicas
        modalidades_odonto = ['Odontologia de Grupo', 'Cooperativa Odontológica']
        df_odonto = df_cad[df_cad['Modalidade'].isin(modalidades_odonto)].copy()
        
        # Criamos um set com os Registros ANS para busca ultra-rápida
        lista_reg_ans = set(df_odonto['Registro_ANS'].astype(str).str.strip().unique())
        
        print(f"✅ Filtro preparado: {len(lista_reg_ans)} operadoras odontológicas identificadas.")
        return lista_reg_ans
    except Exception as e:
        print(f"❌ Erro ao obter cadastro: {e}")
        return set()

def extrair_dados_ans(ano_rotulo, url, lista_odonto):
    """
    Faz o download do ZIP, processa em blocos e filtra apenas as operadoras da lista.
    """
    try:
        print(f"\n📡 2. Processando dados financeiros de {ano_rotulo}...")
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        # Dicionário para conferência interna { Registro_ANS: Soma_Receita }
        rastreio_receita = {}
        rastreio_despesa = {}

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
            
            chunks = pd.read_csv(
                z.open(csv_file), 
                sep=';', 
                encoding='latin-1', 
                chunksize=250000,
                low_memory=False
            )
            
            for i, chunk in enumerate(chunks):
                chunk.columns = [c.upper().strip() for c in chunk.columns]
                
                # Garante que REG_ANS seja string para bater com a nossa lista
                chunk['REG_ANS'] = chunk['REG_ANS'].astype(str).str.strip()
                
                # --- FILTRO CRUCIAL: Remove tudo que não for Odonto ---
                chunk = chunk[chunk['REG_ANS'].isin(lista_odonto)].copy()
                
                if not chunk.empty:
                    col_conta = 'CD_CONTA_CONTABIL'
                    col_valor = 'VL_SALDO_FINAL' if 'VL_SALDO_FINAL' in chunk.columns else 'VALOR'
                    
                    # Converte valores para numérico
                    chunk[col_valor] = pd.to_numeric(
                        chunk[col_valor].astype(str).str.replace(',', '.'), 
                        errors='coerce'
                    ).fillna(0)
                    
                    # Soma Receitas
                    mask_rec = chunk[col_conta].astype(str).str.strip().isin(CONTAS_RECEITA)
                    resumo_rec = chunk[mask_rec].groupby('REG_ANS')[col_valor].sum()
                    for reg, val in resumo_rec.items():
                        rastreio_receita[reg] = rastreio_receita.get(reg, 0) + val
                        
                    # Soma Despesas
                    mask_desp = chunk[col_conta].astype(str).str.strip().isin(CONTAS_DESPESA)
                    resumo_desp = chunk[mask_desp].groupby('REG_ANS')[col_valor].sum()
                    for reg, val in resumo_desp.items():
                        rastreio_despesa[reg] = rastreio_despesa.get(reg, 0) + val

                if i % 10 == 0 and i > 0:
                    print(f"   ... processando bloco {i}")

            # --- GERAÇÃO DO ARQUIVO DE CONFERÊNCIA ---
            # Aqui você vê o resultado "por dentro" do programa
            df_check = pd.DataFrame([
                {'Registro_ANS': k, 'Receita': v, 'Despesa': abs(rastreio_despesa.get(k, 0))} 
                for k, v in rastreio_receita.items()
            ])
            
            arquivo_csv = f"auditoria_odonto_{ano_rotulo}.csv"
            df_check.to_csv(arquivo_csv, sep=';', index=False, encoding='latin-1')
            print(f"📊 Relatório de auditoria salvo em: {arquivo_csv}")
            
            return {
                "receita": sum(rastreio_receita.values()),
                "despesa": abs(sum(rastreio_despesa.values()))
            }

    except Exception as e:
        print(f"❌ Erro ao processar {ano_rotulo}: {e}")
        return None

def fetch_and_process():
    # 1. Obtém lista de operadoras exclusivas
    lista_odonto = obter_operadoras_exclusivamente_odonto()
    
    if not lista_odonto:
        print("ERRO: Não foi possível carregar a lista de filtros.")
        return

    # 2. Define os períodos (URLs da ANS)
    urls = {
        "2024": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2024/4T2024.zip",
        "2025": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/4T2025.zip" # Ajuste conforme lançamento
    }
    
    evolucao_geral = []
    
    for ano, url in urls.items():
        dados = extrair_dados_ans(ano, url, lista_odonto)
        
        if dados and dados['receita'] > 0:
            rec = dados['receita']
            desp = dados['despesa']
            
            evolucao_geral.append({
                "ano": ano,
                "receita": round(rec / 1e9, 3), # Em Bilhões
                "despesaAssistencial": round(desp / 1e9, 3),
                "sinistralidade": round((desp / rec * 100), 1),
                "combinado": round(((desp + (rec * 0.155)) / rec * 100), 1)
            })

    # 3. Salva o resultado final para o Dashboard
    output = {
        "metadata": {"ultima_atualizacao": datetime.now().strftime("%d/%m/%Y %H:%M")},
        "evoluçãoGeral": sorted(evolucao_geral, key=lambda x: x['ano']),
        "dadosSegmentacaoOdonto": [
            {"nome": "Empresarial", "share2025": 70.0},
            {"nome": "Individual", "share2025": 20.0},
            {"nome": "Adesão", "share2025": 10.0}
        ]
    }

    with open('dados_odonto.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print("\n✅ PROCESSO CONCLUÍDO!")
    print("-> Verifique os arquivos 'auditoria_odonto_XXXX.csv' para ver as empresas somadas.")
    print("-> 'dados_odonto.json' atualizado para o dashboard.")

if __name__ == "__main__":
    fetch_and_process()
