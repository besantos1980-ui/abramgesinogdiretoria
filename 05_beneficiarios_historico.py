import duckdb
import pandas as pd
import os
from datetime import datetime

def atualizar_historico_beneficiarios():
    print("Iniciando o Passo 05: Consolidando Histórico de Beneficiários...")
    con = duckdb.connect('banco_ans.db')
    
    mes_atual = datetime.now().strftime('%Y-%m')
    print(f"Capturando retrato do banco de dados para a referência: {mes_atual}")
    
    # 1. Tirar a "foto" atual do banco de dados (agora com a coluna correta total_vidas)
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
    # 4. LIMPEZA E BLINDAGEM CONTRA O EXCEL (AQUI ESTÁ A CORREÇÃO)
    # ----------------------------------------------------------------------
    # Força a coluna a virar texto e pega só os primeiros 7 caracteres (ex: 2025-12)
    df_completo['DATA_REF'] = df_completo['DATA_REF'].astype(str).str.slice(0, 7)
    
    # Força as colunas de categorias a serem texto puro (evita erros com valores vazios/NaN)
    df_completo['Visao'] = df_completo['Visao'].astype(str)
    df_completo['Modalidade'] = df_completo['Modalidade'].astype(str)
    df_completo['Porte'] = df_completo['Porte'].astype(str)
    
    # Remove duplicatas
    df_completo = df_completo.drop_duplicates(subset=['DATA_REF', 'Visao', 'Modalidade', 'Porte'], keep='last')
    
    # Agora que tudo é texto puro (str), o sort_values não vai mais dar erro matemático
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
