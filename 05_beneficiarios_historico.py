import duckdb
import pandas as pd
import os

# =====================================================================
# CONFIGURAÇÃO MANUAL DE COMPETÊNCIA
# Informe abaixo qual é o mês real dos dados que estão no banco_ans.db
# (Ex: '2026-03' ou '2026-04'). Isso evita a criação de meses fantasmas.
# =====================================================================
MES_REFERENCIA_BANCO = '2026-03' 

def normalizar_colunas(df):
    # Arranca espaços invisíveis
    df.rename(columns=lambda x: str(x).strip(), inplace=True)
    # Padroniza a coluna de vidas
    colunas_corrigidas = {col: 'Beneficiarios_Ativos' for col in df.columns if str(col).lower() == 'beneficiarios_ativos'}
    df.rename(columns=colunas_corrigidas, inplace=True)
    
    # VACINA DE DADOS: Traduz a nomenclatura do Excel para o padrão do Painel
    if 'Visao' in df.columns:
        df['Visao'] = df['Visao'].replace({
            'ASSISTÊNCIA MÉDICA': 'Médico-Hospitalar',
            'ASSISTENCIA MEDICA': 'Médico-Hospitalar',
            'EXCLUSIVAMENTE ODONTOLÓGICA': 'Exclusivamente Odontológico',
            'EXCLUSIVAMENTE ODONTOLOGICA': 'Exclusivamente Odontológico'
        })
    return df

def atualizar_historico_beneficiarios():
    print(f"Iniciando Passo 05. Lendo banco de dados com referência: {MES_REFERENCIA_BANCO}...")
    con = duckdb.connect('banco_ans.db')
    
    query_atual = f"""
    SELECT 
        '{MES_REFERENCIA_BANCO}' AS DATA_REF,
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
        print(f"Erro ao ler o banco atual: {e}")
        con.close()
        return

    con.close()
    
    df_atual = normalizar_colunas(df_atual)
    df_completo = df_atual.copy()
    
    arquivo_passado = 'historico_beneficiarios_base.xlsx'
    if os.path.exists(arquivo_passado):
        df_passado = pd.read_excel(arquivo_passado)
        df_passado = normalizar_colunas(df_passado) 
        df_completo = pd.concat([df_passado, df_completo], ignore_index=True)
        
    arquivo_robo = 'historico_acumulado_rob.csv'
    if os.path.exists(arquivo_robo):
        df_acumulado = pd.read_csv(arquivo_robo, sep=';')
        df_acumulado = normalizar_colunas(df_acumulado) 
        df_completo = pd.concat([df_acumulado, df_completo], ignore_index=True)
        
    def converter_para_mes(valor):
        v = str(valor).strip()
        if v.endswith('.0'): v = v[:-2]
            
        dt = None
        if v.isdigit() and len(v) >= 4:
            try: dt = pd.to_datetime(int(v), unit='D', origin='1899-12-30')
            except: pass
        else:
            try: dt = pd.to_datetime(v[:10], errors='coerce') 
            except: pass
                
        if dt is not None and not pd.isna(dt):
            return dt.strftime('%Y-%m')
        
        if len(v) >= 7 and v[4] == '-':
            return v[:7]
        return v

    df_completo['DATA_REF'] = df_completo['DATA_REF'].apply(converter_para_mes)
    
    def limpar_beneficiarios(valor):
        if pd.isna(valor): return 0
        if isinstance(valor, (int, float)): return int(valor)
        v = str(valor).strip()
        if v.endswith('.0'): v = v[:-2]
        v_limpo = ''.join(filter(str.isdigit, v))
        return int(v_limpo) if v_limpo else 0

    if 'Beneficiarios_Ativos' in df_completo.columns:
        if isinstance(df_completo['Beneficiarios_Ativos'], pd.DataFrame):
            df_completo['Beneficiarios_Ativos'] = df_completo['Beneficiarios_Ativos'].bfill(axis=1).iloc[:, 0]
            df_completo = df_completo.loc[:, ~df_completo.columns.duplicated()]
            
        df_completo['Beneficiarios_Ativos'] = df_completo['Beneficiarios_Ativos'].apply(limpar_beneficiarios)
    
    df_completo['Visao'] = df_completo['Visao'].astype(str)
    df_completo['Modalidade'] = df_completo['Modalidade'].astype(str)
    df_completo['Porte'] = df_completo['Porte'].astype(str)
    
    # O deduplicador agora vai funcionar perfeitamente, pois os nomes das visões estão idênticos!
    df_completo = df_completo.drop_duplicates(subset=['DATA_REF', 'Visao', 'Modalidade', 'Porte'], keep='last')
    df_completo = df_completo.sort_values(by=['DATA_REF', 'Visao', 'Modalidade', 'Porte'])
    
    df_completo.to_csv(arquivo_robo, sep=';', index=False)
    arquivo_json = 'beneficiarios.json'
    df_completo.to_json(arquivo_json, orient='records', date_format='iso')
    print(f"[OK] Arquivo exportado com sucesso: {arquivo_json}")

if __name__ == "__main__":
    atualizar_historico_beneficiarios()
