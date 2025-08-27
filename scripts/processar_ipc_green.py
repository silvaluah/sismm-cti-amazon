"""
Script para classificar patentes com base no IPC Green Inventory.

Este script realiza os seguintes passos:
1. Carrega os dados de patentes e seus respectivos códigos IPC.
2. Carrega a lista oficial de códigos IPC do Green Inventory, incluindo suas descrições.
3. Normaliza os códigos IPC em ambos os arquivos, removendo espaços.
4. Cruza os dados para identificar quais IPCs de patentes são "verdes".
5. Cria uma coluna de descrição que é preenchida APENAS para os IPCs verdes.
6. Salva o resultado em um novo arquivo CSV.

Exemplo de uso via terminal:
python scripts/processar_ipc_green.py \
    --patentes_ipc_input data/processed/espacenet/dim_ipc.csv \
    --ipc_gi_dir data/raw/ipc/ \
    --output data/processed/espacenet/ipc_classificado_green.csv
"""

import pandas as pd
import argparse
import sys
import os
import glob

def carregar_dados_patentes(filepath):
    """Carrega o arquivo de dimensão de IPC das patentes."""
    try:
        df = pd.read_csv(filepath)
        if 'ipc_code' not in df.columns:
            print(f"✖ ERRO: A coluna 'ipc_code' não foi encontrada em '{filepath}'.")
            sys.exit(1)

        # Normalização dos códigos IPC (remove espaços)
        df['ipc_code'] = df['ipc_code'].str.replace(' ', '', regex=False).str.strip()
        print("  - Códigos IPC do arquivo de patentes foram normalizados.")
        
        print(f"✔ Arquivo de IPCs de patentes '{filepath}' carregado com sucesso ({df.shape[0]} registros).")
        return df
    except FileNotFoundError:
        print(f"✖ ERRO: Arquivo de patentes '{filepath}' não encontrado.")
        sys.exit(1)

def carregar_lista_green_inventory(ipc_dir):
    """Carrega a lista de códigos IPC do Green Inventory, incluindo suas descrições."""
    try:
        gi_filepath_list = glob.glob(os.path.join(ipc_dir, 'ipc_gi/processed_ipc_gi_level_9*.csv'))
        if not gi_filepath_list:
            raise IndexError
        
        df_gi = pd.read_csv(gi_filepath_list[0])
        
        # Verifica se as colunas 'code' e 'desc_full' existem
        if 'code' not in df_gi.columns or 'desc_full' not in df_gi.columns:
            print(f"✖ ERRO: As colunas 'code' e/ou 'desc_full' não foram encontradas no arquivo Green Inventory.")
            sys.exit(1)
            
        # Normalização dos códigos IPC (remove espaços)
        df_gi['code'] = df_gi['code'].str.replace(' ', '', regex=False).str.strip()
        print("  - Códigos IPC da lista Green Inventory foram normalizados.")
            
        df_gi['is_green'] = True
        
        # Renomeia as colunas para o merge
        df_gi = df_gi.rename(columns={'code': 'ipc_code', 'desc_full': 'ipc_description'})
        
        print(f"✔ Lista do Green Inventory carregada com sucesso ({df_gi.shape[0]} códigos).")
        # Retorna o código, a flag 'is_green' e a descrição
        return df_gi[['ipc_code', 'is_green', 'ipc_description']].drop_duplicates(subset=['ipc_code'])
    except IndexError:
        print(f"✖ ERRO: Nenhum arquivo 'processed_ipc_gi_level_9*.csv' encontrado em '{os.path.join(ipc_dir, 'ipc_gi/')}'")
        sys.exit(1)

def salvar_dados(df, filepath):
    """Salva o DataFrame final em um arquivo CSV."""
    try:
        output_dir = os.path.dirname(filepath)
        if not os.path.exists(output_dir) and output_dir:
            os.makedirs(output_dir)
        
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"✔ Resultados salvos com sucesso no arquivo: '{filepath}'")
    except Exception as e:
        print(f"✖ ERRO ao salvar o arquivo: {e}")
        sys.exit(1)

def main():
    """Função principal que orquestra a execução do script."""
    parser = argparse.ArgumentParser(description="Script para classificar patentes com base no IPC Green Inventory.")
    parser.add_argument('--patentes_ipc_input', required=True, help="Caminho para o arquivo de entrada com os IPCs das patentes (dim_ipc.csv).")
    parser.add_argument('--ipc_gi_dir', required=True, help="Caminho para a pasta raiz que contém o subdiretório 'ipc_gi'.")
    parser.add_argument('--output', required=True, help="Caminho para o arquivo CSV de saída.")
    args = parser.parse_args()

    print("--- INICIANDO SCRIPT DE CLASSIFICAÇÃO DE PATENTES (IPC GREEN INVENTORY) ---")

    # 1. Carregar os dados de entrada
    df_patentes_ipc = carregar_dados_patentes(args.patentes_ipc_input)
    df_green_list = carregar_lista_green_inventory(args.ipc_gi_dir)

    # 2. Cruzar os dados em um único passo
    print("⏳ Cruzando dados de patentes com a lista do Green Inventory...")
    # 'how=left' mantém todos os IPCs do arquivo de patentes original
    df_final = pd.merge(df_patentes_ipc, df_green_list, on='ipc_code', how='left')
    
    # 3. Limpar o resultado do merge
    # Se 'is_green' for NaN (não encontrou na lista GI), preenche com False
    df_final['is_green'] = df_final['is_green'].fillna(False)
    # A coluna 'ipc_description' permanecerá NaN para os não-verdes, como solicitado.
    
    print("✔ Classificação concluída.")

    # 4. Salvar o resultado
    salvar_dados(df_final, args.output)
    
    # Verificação final
    num_verdes = df_final['is_green'].sum()
    print(f"📊 Verificação final: {num_verdes} registros de IPC foram classificados como verdes.")
    
    print("--- SCRIPT CONCLUÍDO COM SUCESSO ---")

if __name__ == "__main__":
    main()