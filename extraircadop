import duckdb

def extrair_cadop():
    print("Iniciando extração do CADOP...")
    con = duckdb.connect('banco_ans.db')
    
    query = """
    CREATE OR REPLACE TABLE cadop AS 
    SELECT 
        CAST(Registro_ANS AS VARCHAR) AS REG_ANS,
        Nome_Fantasia,
        Modalidade
    FROM read_csv_auto('https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv', encoding='WIN1252')
    WHERE Modalidade != 'Administradora de Benefícios';
    """
    con.execute(query)
    print("CADOP atualizado com sucesso.")
    con.close()

if __name__ == "__main__":
    extrair_cadop()
