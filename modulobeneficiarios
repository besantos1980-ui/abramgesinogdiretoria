import duckdb

def extrair_beneficiarios():
    print("Iniciando contagem de beneficiários (SIB)...")
    con = duckdb.connect('banco_ans.db')
    
    query = """
    CREATE OR REPLACE TABLE porte_operadoras AS
    SELECT 
        CAST(REGISTRO_OPERADORA AS VARCHAR) AS REG_ANS,
        COUNT(*) AS total_vidas,
        CASE 
            WHEN COUNT(*) <= 19999 THEN 'Pequeno'
            WHEN COUNT(*) BETWEEN 20000 AND 99999 THEN 'Médio'
            ELSE 'Grande'
        END AS Porte
    FROM read_csv_auto('https://dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/sib_ativo_*.csv', all_varchar=true)
    GROUP BY REGISTRO_OPERADORA;
    """
    con.execute(query)
    print("Porte das operadoras calculado com sucesso.")
    con.close()

if __name__ == "__main__":
    extrair_beneficiarios()
