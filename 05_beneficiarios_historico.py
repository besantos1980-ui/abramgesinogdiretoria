import duckdb
import pandas as pd
import os
from datetime import datetime

def atualizar_historico_beneficiarios():
    print("Iniciando o Passo 05: Consolidando Histórico de Beneficiários...")
    con = duckdb.connect('banco_ans.db')
    
    mes_atual = datetime.now().strftime('%Y-%m')
    print(f"Capturando retrato do banco de dados para a referência: {mes_atual}")
    
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
    
    arquivo_passado = 'historico_beneficiarios_base.xlsx'
    if os.path.exists(arquivo_passado):
        print(f"Lendo carga histórica inicial de: {arquivo_passado}")
        df_passado = pd.read_excel(arquivo_passado)
        df_completo = pd.concat([df_passado, df_completo], ignore_index=True)
        
    arquivo_robo = 'historico_acumulado_rob.csv'
    if os.path.exists(arquivo_robo):
        print(f"Lendo histórico acumulado do robô de: {arquivo_robo}")
        df_acumulado = pd.read_csv(arquivo_robo, sep=';')
        df_completo = pd.concat([df_acumulado, df_completo], ignore_index=True)
        
    # ----------------------------------------------------------------------
    # 4. LIMPEZA BLINDADA (DATAS, ESPAÇOS E NÚMEROS)
    # ----------------------------------------------------------------------
    
    # Arranca qualquer espaço em branco invisível do nome das colunas
    df_completo.rename(columns=lambda x: str(x).strip(), inplace=True)
    
    def converter_para_trimestre(valor):
        v = str(valor).strip()
        if 'T' in v: return v
        if v.endswith('.0'): v = v[:-2]
            
        dt = None
        if v.isdigit() and len(v) >= 4:
            try: dt = pd.to_datetime(int(v), unit='D', origin='1899-12-30')
            except: pass
        else:
            try: dt = pd.to_datetime(v[:10], errors='coerce') 
            except: pass
                
        if dt is not None and not pd.isna(dt):
            trimestre = (dt.month - 1) // 3 + 1
            return f"{trimestre}T{dt.year}"
        return v

    df_completo['DATA_REF'] = df_completo['DATA_REF'].apply(converter_para_trimestre)
    
    # Função extrema para limpar sujeira nos números (remove pontos, letras e vírgulas)
    def limpar_beneficiarios(valor):
        if pd.isna(valor): return 0
        if isinstance(valor, (int, float)): return int(valor)
        
        v = str(valor).strip()
        if v.endswith('.0'): v = v[:-2]
        
        # Mantém apenas o que for dígito puro (ex: "1.500" vira "1500")
        v_limpo = ''.join(filter(str.isdigit, v))
        return int(v_limpo) if v_limpo else 0

    # Aplica a limpeza bruta se a coluna existir
    if 'Beneficiarios_Ativos' in df_completo.columns:
        df_completo['Beneficiarios_Ativos'] = df_completo['Beneficiarios_Ativos'].apply(limpar_beneficiarios)
    else:
        print("ALERTA CRÍTICO: A coluna 'Beneficiarios_Ativos' não foi encontrada. Verifique o Excel.")
    
    df_completo['Visao'] = df_completo['Visao'].astype(str)
    df_completo['Modalidade'] = df_completo['Modalidade'].astype(str)
    df_completo['Porte'] = df_completo['Porte'].astype(str)
    
    df_completo = df_completo.drop_duplicates(subset=['DATA_REF', 'Visao', 'Modalidade', 'Porte'], keep='last')
    df_completo = df_completo.sort_values(by=['DATA_REF', 'Visao', 'Modalidade', 'Porte'])
    
    df_completo.to_csv(arquivo_robo, sep=';', index=False)
    print(f"[OK] Arquivo mestre atualizado com sucesso: {arquivo_robo}")
    
    arquivo_json = 'beneficiarios.json'
    df_completo.to_json(arquivo_json, orient='records', date_format='iso')
    print(f"[OK] Arquivo exportado para o dashboard: {arquivo_json}")

if __name__ == "__main__":
    atualizar_historico_beneficiarios()
