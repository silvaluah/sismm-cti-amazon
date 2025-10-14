# Em scripts/criar_fato_cncflora.py

import pandas as pd
from pathlib import Path
import sys
import csv

def main():
    print("--- INICIANDO CRIAÇÃO DA TABELA FATO ENRIQUECIDA DO CNCFLORA ---")
    
    # --- Configuração de Caminhos ---
    raiz_projeto = Path(__file__).parent.parent
    processed_dir = raiz_projeto / "data/processed/cncflora"
    looker_output_dir = raiz_projeto / "looker_studio_output"
    looker_output_dir.mkdir(exist_ok=True)

    # --- Carregamento dos Dados ---
    print("⏳ Carregando dados do CNCFlora...")
    try:
        df_fato = pd.read_csv(processed_dir / 'fato_gorda_cncflora.csv')
        df_dim_ameacas = pd.read_csv(processed_dir / 'dim_ameacas.csv')
        df_pon_ameaca = pd.read_csv(processed_dir / 'pon_avaliacao_ameaca.csv')
        df_dim_acao = pd.read_csv(processed_dir / 'dim_acoes_conservacao.csv')
        df_pon_acao = pd.read_csv(processed_dir / 'pon_avaliacao_acao.csv')
        print("✔ Arquivos carregados com sucesso.")
    except Exception as e:
        print(f"✖ ERRO ao carregar arquivos: {e}.")
        sys.exit(1)

    # --- Processamento de Ameaças ---
    print("⏳ Processando e agrupando ameaças...")
    # Junta a ponte com a dimensão para obter os nomes
    ameacas_temp = pd.merge(df_pon_ameaca, df_dim_ameacas, on='ameaca_id', how='left')
    # Agrupa todos os nomes de ameaças por avaliação em uma única string
    ameacas_agrupado = ameacas_temp.groupby('avaliacao_id')['ameaca'].apply(lambda x: ' | '.join(x.dropna().unique())).reset_index()
    ameacas_agrupado.rename(columns={'ameaca': 'lista_ameacas'}, inplace=True)

    # --- Processamento de Ações de Conservação ---
    print("⏳ Processando e agrupando ações de conservação...")
    # Junta a ponte com a dimensão
    acoes_temp = pd.merge(df_pon_acao, df_dim_acao, on='acao_conservacao_id', how='left')
    # Agrupa
    acoes_agrupado = acoes_temp.groupby('avaliacao_id')['acao_conservacao'].apply(lambda x: ' | '.join(x.dropna().unique())).reset_index()
    acoes_agrupado.rename(columns={'acao_conservacao': 'lista_acoes_conservacao'}, inplace=True)

    # --- Construção da Tabela Fato Final ---
    print("⏳ Construindo a tabela fato final...")
    # Começa com a tabela fato original
    df_fato_final = df_fato.copy()
    
    # Faz o merge com as ameaças agrupadas
    df_fato_final = pd.merge(df_fato_final, ameacas_agrupado, on='avaliacao_id', how='left')
    
    # Faz o merge com as ações agrupadas
    df_fato_final = pd.merge(df_fato_final, acoes_agrupado, on='avaliacao_id', how='left')
    
    # Preenche valores nulos para as listas
    df_fato_final['lista_ameacas'] = df_fato_final['lista_ameacas'].fillna('Nenhuma Ameaça Registrada')
    df_fato_final['lista_acoes_conservacao'] = df_fato_final['lista_acoes_conservacao'].fillna('Nenhuma Ação Registrada')

    # --- Salvamento do Resultado ---
    caminho_saida = looker_output_dir / "looker_fato_cncflora.csv"
    df_fato_final.to_csv(caminho_saida, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
    print(f"✔ Tabela fato enriquecida do CNCFlora salva em: '{caminho_saida}'")
    print("--- SCRIPT CONCLUÍDO ---")

if __name__ == "__main__":
    main()