# scripts/unificar_fontes.py (VERSÃO CORRIGIDA)
import pandas as pd
from pathlib import Path
import re
import sys # <-- Importar a biblioteca sys

def unificar_fontes_e_criar_ligacoes():
    """
    Lê os dados processados, cria a dimensão de espécies unificada e a tabela ponte artigo-espécie.
    """
    print("Iniciando o processo de unificação das fontes de dados...")

    caminho_script = Path(__file__).parent
    caminho_repo_raiz = caminho_script.parent
    caminho_processados = caminho_repo_raiz / 'data' / 'processed'
    
    caminho_scopus_limpo = caminho_processados / 'scopus_dados_limpos_temp.csv'
    caminho_especies_cncflora = caminho_processados / 'cncflora' / 'dim_especies_cncflora_temp.csv'

    try:
        df_scopus_limpo = pd.read_csv(caminho_scopus_limpo)
        df_especies_cncflora = pd.read_csv(caminho_especies_cncflora)
        print("Arquivos processados da Scopus e CNCFlora carregados com sucesso.")
    except FileNotFoundError as e:
        print(f"ERRO: Não foi possível encontrar um dos arquivos de entrada: {e}")
        print("Certifique-se de que os scripts anteriores foram executados com sucesso.")
        sys.exit(1) # <-- CORREÇÃO: Força o script a parar com um código de erro

    # (O resto da função continua o mesmo)
    lista_de_plantas = df_especies_cncflora['nome_cientifico'].dropna().unique().tolist()
    regex_pattern = r'\b(' + '|'.join(re.escape(nome) for nome in lista_de_plantas) + r')\b'
    print(f"Buscando por {len(lista_de_plantas)} nomes de plantas nos artigos da Scopus...")
    if 'eid' in df_scopus_limpo.columns:
        df_scopus_limpo.rename(columns={'eid': 'article_id'}, inplace=True)
    df_scopus_limpo['texto_busca'] = (df_scopus_limpo['title'].fillna('') + ' ' + df_scopus_limpo['abstract'].fillna(''))
    df_scopus_limpo['especies_encontradas'] = df_scopus_limpo['texto_busca'].str.findall(regex_pattern, flags=re.IGNORECASE)
    df_scopus_com_especies = df_scopus_limpo[df_scopus_limpo['especies_encontradas'].apply(lambda x: len(x) > 0)]
    pon_artigo_especie_temp = df_scopus_com_especies[['article_id', 'especies_encontradas']].explode('especies_encontradas')
    pon_artigo_especie_temp.rename(columns={'especies_encontradas': 'nome_cientifico'}, inplace=True)
    especies_scopus = pd.DataFrame(pon_artigo_especie_temp['nome_cientifico'].unique(), columns=['nome_cientifico'])
    dim_especies_mestre = pd.concat([df_especies_cncflora[['nome_cientifico']], especies_scopus], ignore_index=True)
    dim_especies_mestre.drop_duplicates(inplace=True)
    dim_especies_mestre.reset_index(drop=True, inplace=True)
    dim_especies_mestre.reset_index(inplace=True)
    dim_especies_mestre.rename(columns={'index': 'especie_id'}, inplace=True)
    print("Dimensão Mestre de Espécies criada.")
    pon_artigo_especie = pd.merge(
        pon_artigo_especie_temp,
        dim_especies_mestre,
        on='nome_cientifico',
        how='left'
    )[['article_id', 'especie_id']].drop_duplicates()
    print("Tabela Ponte Artigo-Espécie criada.")

    caminho_dim_especies = caminho_processados / 'dim_especies_mestre.csv'
    caminho_ponte_artigo_especie = caminho_processados / 'pon_artigo_especie.csv'
    dim_especies_mestre.to_csv(caminho_dim_especies, index=False)
    pon_artigo_especie.to_csv(caminho_ponte_artigo_especie, index=False)
    print("\n--- Processo de unificação concluído! ---")
    print(f"Salvo: {caminho_dim_especies.name}")
    print(f"Salvo: {caminho_ponte_artigo_especie.name}")

if __name__ == "__main__":
    unificar_fontes_e_criar_ligacoes()