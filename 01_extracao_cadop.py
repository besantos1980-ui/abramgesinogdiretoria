import duckdb

def extrair_cadop():
    print("Iniciando extração do CADOP...")
    con = duckdb.connect('banco_ans.db')
    
    # Corrigido o nome da coluna para REGISTRO_OPERADORA, conforme padrão da ANS
    query = """
    CREATE OR REPLACE TABLE cadop AS 
    SELECT 
        CAST(REGISTRO_OPERADORA AS VARCHAR) AS REG_ANS,
        Nome_Fantasia,
        Modalidade
    FROM read_csv(
        'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv',
        delim=';',
        header=true,
        encoding='UTF-8',
        ignore_errors=true
    )
    WHERE Modalidade != 'Administradora de Benefícios';
    """
    con.execute(query)
    print("CADOP atualizado com sucesso.")
    con.close()

if __name__ == "__main__":
    extrair_cadop()
