import duckdb
import json

def consolidar_dados():
    print("Iniciando cruzamento de dados...")
    con = duckdb.connect('banco_ans.db')
    
    # 1. Gerar arquivo de Auditoria (Apenas contas de interesse)
    query_auditoria = """
    COPY (
        SELECT 
            c.REG_ANS,
            c.DATA,
            c.CD_CONTA_CONTABIL,
            c.SALDO_TRIMESTRE,
            CASE WHEN cd.REG_ANS IS NOT NULL THEN 'ATIVA' ELSE 'INATIVA' END AS Status_Auditoria
        FROM contabilidade_bruta c
        LEFT JOIN cadop cd ON c.REG_ANS = cd.REG_ANS
        WHERE c.CD_CONTA_CONTABIL IN ('311', '312', '313', '32', '411', '412')
    ) TO 'auditoria.csv' (HEADER, DELIMITER ';');
    """
    con.execute(query_auditoria)
    
    # 2. Gerar dados agregados para o Dashboard (Com Igualdade Exata)
    query_dashboard = """
    SELECT 
        c.DATA,
        CASE 
            WHEN cd.Modalidade IN ('Autogestão', 'Cooperativa Médica', 'Filantropia', 'Medicina de Grupo', 'Seguradora Especializada em Saúde') THEN 'Médico-Hospitalar'
            WHEN cd.Modalidade IN ('Cooperativa Odontológica', 'Odontologia de Grupo') THEN 'Exclusivamente Odontológico'
            ELSE 'Outros'
        END AS Visao,
        cd.Modalidade,
        COALESCE(p.Porte, 'Sem Informação') AS Porte,
        
        -- Contraprestações Efetivas: IGUALDADE EXATA
        SUM(CASE 
            WHEN c.CD_CONTA_CONTABIL IN ('311', '312', '313', '32') 
            THEN c.SALDO_TRIMESTRE ELSE 0 
        END) AS Contraprestacoes_Efetivas,
        
        -- Eventos Indenizáveis Líquidos: IGUALDADE EXATA
        SUM(CASE 
            WHEN c.CD_CONTA_CONTABIL IN ('411', '412') 
            THEN c.SALDO_TRIMESTRE ELSE 0 
        END) AS Eventos_Indenizaveis
        
    FROM contabilidade_bruta c
    INNER JOIN cadop cd ON c.REG_ANS = cd.REG_ANS
    LEFT JOIN porte_operadoras p ON c.REG_ANS = p.REG_ANS
    
    -- Filtra a base inteira antes de agrupar para ganhar performance
    WHERE c.CD_CONTA_CONTABIL IN ('311', '312', '313', '32', '411', '412')
    
    GROUP BY c.DATA, Visao, cd.Modalidade, Porte
    HAVING Visao != 'Outros';
    """
    
    df_dash = con.execute(query_dashboard).df()
    
    # Exportar para JSON (formato ideal para React)
    df_dash.to_json('dashboard.json', orient='records', date_format='iso')
    print("Arquivos 'auditoria.csv' e 'dashboard.json' gerados com sucesso.")
    con.close()

if __name__ == "__main__":
    consolidar_dados()
