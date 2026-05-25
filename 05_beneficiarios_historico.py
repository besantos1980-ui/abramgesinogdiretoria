import duckdb
import pandas as pd
import os
from datetime import datetime

def atualizar_historico_beneficiarios():
    print("Iniciando o Passo 05: Consolidando Histórico de Beneficiários...")
    con = duckdb.connect('banco_ans.db')
    
    # O DuckDB devolve o mês atual (ex: '2026-05')
    mes_atual = datetime.now().strftime('%Y-%m')
    print(f"Capturando retrato do banco de dados para a referência: {mes_atual}")
    
    # 1. Tirar a "foto" atual do banco de dados
    query_atual = f"""
    SELECT 
        '{mes_atual}' AS DATA_REF,
        CASE 
            WHEN cd.Modalidade IN ('Autogestão', 'Cooperativa Médica', 'Filantropia', 'Medicina de Grupo', 'Seguradora Especializada em Saúde') THEN 'Médico-Hospitalar'
            WHEN cd.Modalidade IN ('Cooperativa Odontológica', 'Odontologia de Grupo') THEN 'Exclusivamente Odontológico'
            ELSE 'Outros'
        END AS Visao,
        cd.Modalidade,
        COALESCE(p.Porte, 'Sem Informação') AS Porte,
        SUM(p.total_vidas) AS Beneficiarios_Ativos
    FROM porte_operadoras p
    INNER JOIN cadop cd ON p.REG_ANS = cd.REG_ANS
    GROUP BY Visao, cd.Modalidade, Porte
    HAVING Visao != 'Outros';
    """
    
    try:
        df_atual = con.execute(query_atual).df()
    except Exception as e:
        print(f"Erro ao ler o banco atual. Detalhe: {e}")
        con.close()
        return

    con.close()
    
    df_completo = df_atual.copy()
    
    # 2. Ler a planilha do passado
    arquivo_passado = 'historico_beneficiarios_base.xlsx'
    if os.path.exists(arquivo_passado):
        print(f"Lendo carga histórica inicial de: {arquivo_passado}")
        df_passado = pd.read_excel(arquivo_passado)
        df_completo = pd.concat([df_passado, df_completo], ignore_index=True)
        
    # 3. Ler o histórico acumulado
    arquivo_robo = 'historico_acumulado_rob.csv'
    if os.path.exists(arquivo_robo):
        print(f"Lendo histórico acumulado do robô de: {arquivo_robo}")
        df_acumulado = pd.read_csv(arquivo_robo, sep=';')
        df_completo = pd.concat([df_acumulado, df_completo], ignore_index=True)
        
    # ----------------------------------------------------------------------
    # 4. LIMPEZA E BLINDAGEM (CONVERSÃO DEFINITIVA PARA TRIMESTRE)
    # ----------------------------------------------------------------------
    def converter_para_trimestre(valor):
        v = str(valor).strip()
        
        # Se for um formato que já tem 'T' (ex: 1T2025), devolve intacto
        if 'T' in v:
            return v
            
        # Se vier com '.0'
        if v.endswith('.0'):
            v = v[:-2]
            
        dt = None
        # Tenta converter se for número serial do Excel
        if v.isdigit() and len(v) >= 4:
            try:
                dt = pd.to_datetime(int(v), unit='D', origin='1899-12-30')
            except:
                pass
        # Tenta converter se for string (ex: '2025-03', '2025-03-01 00:00:00')
        else:
            try:
                # O parâmetro exact=False tenta encontrar o padrão
                dt = pd.to_datetime(v[:10], errors='coerce') 
            except:
                pass
                
        # Se conseguiu transformar numa data real
        if dt is not None and not pd.isna(dt):
            trimestre = (dt.month - 1) // 3 + 1
            ano = dt.year
            return f"{trimestre}T{ano}"
            
        return v # Retorno seguro (fallback)

    # Aplica a função de conversão para o formato 1T2025
    df_completo['DATA_REF'] = df_completo['DATA_REF'].apply(converter_para_trimestre)
    
    # Força os beneficiários a serem números inteiros limpos
    if 'Beneficiarios_Ativos' in df_completo.columns:
        df_completo['Beneficiarios_Ativos'] = pd.to_numeric(df_completo['Beneficiarios_Ativos'], errors='coerce').fillna(0).astype(int)
    
    # Padroniza as outras categorias
    df_completo['Visao'] = df_completo['Visao'].astype(str)
    df_completo['Modalidade'] = df_completo['Modalidade'].astype(str)
    df_completo['Porte'] = df_completo['Porte'].astype(str)
    
    # Remove duplicatas baseadas no novo formato Trimestral
    df_completo = df_completo.drop_duplicates(subset=['DATA_REF', 'Visao', 'Modalidade', 'Porte'], keep='last')
    df_completo = df_completo.sort_values(by=['DATA_REF', 'Visao', 'Modalidade', 'Porte'])
    
    # 5. Salvar o arquivo mestre do robô
    df_completo.to_csv(arquivo_robo, sep=';', index=False)
    print(f"[OK] Arquivo mestre atualizado com sucesso: {arquivo_robo}")
    
    # 6. Gerar o JSON para o React
    arquivo_json = 'beneficiarios.json'
    df_completo.to_json(arquivo_json, orient='records', date_format='iso')
    print(f"[OK] Arquivo exportado para o dashboard: {arquivo_json}")

if __name__ == "__main__":
    atualizar_historico_beneficiarios()
