# Em scripts/enriquecer_ipc.py

import pandas as pd
from pathlib import Path
import glob
import re
import sys
import csv

def normalizar_ipc_code(ipc_code):
    """
    Cria uma chave de junção padronizada para códigos IPC.
    Ex: 'A01B-0001/02' -> 'A01B1/02'
    Ex: 'A01D44/00' -> 'A01D44/00'
    """
    if not isinstance(ipc_code, str):
        return None
    # Remove hífens e zeros à esquerda dos grupos numéricos
    return re.sub(r'-0*', '', ipc_code).strip()

def main():
    print("--- INICIANDO ENRIQUECIMENTO DA DIMENSÃO IPC ---")
    
    raiz_projeto = Path(__file__).parent.parent
    raw_dir = raiz_projeto / "data/raw/ipc/ipc_full"
    processed_dir = raiz_projeto / "data/processed/espacenet"
    caminho_entrada = processed_dir / 'dim_ipc.csv'
    caminho_saida = processed_dir / 'dim_ipc_enriquecida.csv'

    # 1. Carregamento dos Dados
    print("⏳ Carregando dados de IPC...")
    try:
        df_dim_ipc = pd.read_csv(caminho_entrada)
        
        # Consolida todos os arquivos de dicionário de IPC
        arquivos_ipc_dict = glob.glob(str(raw_dir / "*.csv"))
        if not arquivos_ipc_dict:
            raise FileNotFoundError("Nenhum arquivo de dicionário IPC encontrado na pasta ipc_full.")
            
        lista_dfs_ipc = [pd.read_csv(f, sep=';' if ';' in open(f).readline() else ',') for f in arquivos_ipc_dict]
        df_ipc_dict = pd.concat(lista_dfs_ipc, ignore_index=True)
        
        print(f"✔ Arquivos carregados: {len(df_dim_ipc)} IPCs e {len(df_ipc_dict)} descrições.")
    except Exception as e:
        print(f"✖ ERRO ao carregar arquivos: {e}.")
        sys.exit(1)
        
    # 2. Padronização (Criação da Chave de Junção)
    print("⏳ Padronizando códigos IPC para o cruzamento...")
    df_dim_ipc['chave_merge'] = df_dim_ipc['ipc_code'].apply(normalizar_ipc_code)
    
    # Renomeia a coluna 'code' do dicionário para um nome consistente
    if 'code' in df_ipc_dict.columns:
        df_ipc_dict.rename(columns={'code': 'ipc_code_original'}, inplace=True)

    df_ipc_dict['chave_merge'] = df_ipc_dict['ipc_code_original'].apply(normalizar_ipc_code)
    
    # 3. Merge para Enriquecer os Dados
    print("⏳ Cruzando dados para adicionar as descrições...")
    
    # Mantém apenas a descrição mais útil e remove duplicatas
    df_ipc_dict_limpo = df_ipc_dict[['chave_merge', 'desc_full']].dropna().drop_duplicates(subset=['chave_merge'])
    
    df_ipc_enriquecido = pd.merge(
        df_dim_ipc,
        df_ipc_dict_limpo,
        on='chave_merge',
        how='left'
    )
    
    df_ipc_enriquecido['desc_full'] = df_ipc_enriquecido['desc_full'].fillna('Descrição não encontrada')
    
    # Remove a coluna de merge intermediária
    df_ipc_enriquecido = df_ipc_enriquecido.drop(columns=['chave_merge'])
    
    print("✔ Enriquecimento concluído.")
    
    # 4. Salvamento do Resultado
    df_ipc_enriquecido.to_csv(caminho_saida, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
    print(f"✔ Nova dimensão de IPC salva em: '{caminho_saida}'")
    print("--- SCRIPT CONCLUÍDO ---")

if __name__ == "__main__":
    main()