"""
Script para integrar a classificação manual de ODS aos dados de artigos da Scopus.
"""

import pandas as pd
import argparse
import sys
import os
import csv # <--- ADICIONE ESTE IMPORT

def carregar_dados_scopus(filepath):
    # (Esta função já está correta, não precisa mudar)
    try:
        df = pd.read_csv(filepath)
        print(f"✔ Arquivo 'Artigos Scopus' carregado com sucesso de '{filepath}' ({df.shape[0]} linhas).")
        return df
    except FileNotFoundError:
        print(f"✖ ERRO: O arquivo '{filepath}' não foi encontrado.")
        sys.exit(1)

def carregar_dados_ods_manual(filepath):
    # (Esta função já está correta, não precisa mudar)
    print(f"⏳ Carregando arquivo ODS Manual de '{filepath}' (método robusto)...")
    # Exemplo de leitura robusta usando pandas
    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"✖ ERRO ao carregar o arquivo ODS Manual: {e}")
        sys.exit(1)
    return df

def salvar_dados(df, filepath):
    """Salva o DataFrame final em um arquivo CSV de forma robusta."""
    try:
        output_dir = os.path.dirname(filepath)
        if not os.path.exists(output_dir) and output_dir:
            os.makedirs(output_dir)
        
        # --- CORREÇÃO IMPORTANTE AQUI ---
        # Adiciona quoting=csv.QUOTE_ALL para garantir que o CSV seja salvo corretamente
        df.to_csv(filepath, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        
        print(f"✔ Resultados salvos com sucesso no arquivo: '{filepath}'")
    except Exception as e:
        print(f"✖ ERRO ao salvar o arquivo: {e}")
        sys.exit(1)

def main():
    """Função principal que orquestra a execução do script."""
    # (O resto do seu script main permanece o mesmo)
    # ...
    # ...

if __name__ == "__main__":
    main()