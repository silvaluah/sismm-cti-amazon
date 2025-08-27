"""
Script para integrar a classificação manual de ODS aos dados de artigos da Scopus.
"""

import pandas as pd
import argparse
import sys
import os

def carregar_dados_scopus(filepath):
    """Carrega o arquivo principal de artigos da Scopus."""
    try:
        df = pd.read_csv(filepath)
        print(f"✔ Arquivo 'Artigos Scopus' carregado com sucesso de '{filepath}' ({df.shape[0]} linhas).")
        return df
    except FileNotFoundError:
        print(f"✖ ERRO: O arquivo '{filepath}' não foi encontrado.")
        sys.exit(1)

def carregar_dados_ods_manual(filepath):
    """Função de carregamento extra robusta, específica para o arquivo ODS manual."""
    print(f"⏳ Carregando arquivo ODS Manual de '{filepath}' (método robusto)...")
    
    colunas = ['EID_Artigo', 'nro_ods', 'titulo_ods']
    dados_validos = []
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            # Pula a linha do cabeçalho
            next(f)
            # Lê o arquivo linha por linha
            for i, line in enumerate(f, 2): # Começa a contar da linha 2 para logs de erro
                line = line.strip()
                if not line: continue # Pula linhas vazias
                
                try:
                    # Divide a linha no máximo 2 vezes, da esquerda para a direita.
                    # Isso garante que a 3ª parte pegue todo o resto da linha, com vírgulas e tudo.
                    parts = line.split(',', 2)
                    if len(parts) == 3:
                        dados_validos.append(parts)
                    else:
                        print(f"  - Aviso: Pulando linha malformada {i}. Não foi possível separar em 3 partes.")
                except Exception as e:
                    print(f"  - Aviso: Erro ao processar linha {i}: {e}. Pulando.")
    
        df = pd.DataFrame(dados_validos, columns=colunas)
        print(f"✔ Arquivo ODS Manual carregado com sucesso ({df.shape[0]} linhas válidas).")
        return df
        
    except FileNotFoundError:
        print(f"✖ ERRO: O arquivo '{filepath}' não foi encontrado.")
        sys.exit(1)
    except Exception as e:
        print(f"✖ ERRO crítico ao carregar o arquivo '{filepath}': {e}")
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
    # Função principal que orquestra a execução do script.
    parser = argparse.ArgumentParser(description="Script para integrar classificações manuais de ODS.")
    parser.add_argument('--artigos_input', required=True, help="Caminho para o CSV de artigos processados.")
    parser.add_argument('--ods_manual_input', required=True, help="Caminho para o CSV com a classificação manual de ODS.")
    parser.add_argument('--output', required=True, help="Caminho para o arquivo CSV de saída.")
    args = parser.parse_args()

    print("--- INICIANDO INTEGRAÇÃO DE DADOS MANUAIS DE ODS ---")

    # Usa as funções de carregamento específicas
    df_artigos = carregar_dados_scopus(args.artigos_input)
    df_ods = carregar_dados_ods_manual(args.ods_manual_input)

    # Renomeia colunas para o merge
    df_artigos = df_artigos.rename(columns={'eid': 'EID_Artigo'}, errors='ignore')
    # df_ods já é carregado com os nomes corretos
    
    # Validação
    if 'EID_Artigo' not in df_artigos.columns:
        print(f"✖ ERRO: A coluna 'EID_Artigo' (ou 'eid') não foi encontrada em '{args.artigos_input}'.")
        sys.exit(1)
    if 'EID_Artigo' not in df_ods.columns:
        print(f"✖ ERRO: A coluna 'EID_Artigo' não foi encontrada em '{args.ods_manual_input}'.")
        sys.exit(1)

    # Agrupa múltiplos ODS por artigo
    print("⏳ Agrupando ODS por artigo...")
    df_ods_agrupado = df_ods.groupby('EID_Artigo')['nro_ods'].apply(lambda x: ', '.join(x.astype(str))).reset_index()
    df_ods_agrupado = df_ods_agrupado.rename(columns={'nro_ods': 'ODS_classificados'})
    print("✔ Agrupamento concluído.")

    # Realiza o merge
    print("⏳ Integrando (merge) os dados de ODS aos artigos...")
    df_final = pd.merge(df_artigos, df_ods_agrupado, on='EID_Artigo', how='left')
    df_final['ODS_classificados'] = df_final['ODS_classificados'].fillna('Nenhum ODS identificado')
    print("✔ Integração concluída.")

    salvar_dados(df_final, args.output)
    
    print("--- SCRIPT CONCLUÍDO COM SUCESSO ---")

if __name__ == "__main__":
    main()