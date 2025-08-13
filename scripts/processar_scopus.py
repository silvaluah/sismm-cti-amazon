import pandas as pd
from pathlib import Path
import unicodedata
import re
import uuid

# (As funções de limpeza e as de criação de modelos de autores/afiliações continuam aqui)
def carregar_e_consolidar_dados(caminho_pasta_raw):
    print("Iniciando a consolidação dos dados brutos...")
    caminho_base = Path(caminho_pasta_raw)
    arquivos_csv = list(caminho_base.rglob('*.csv'))
    if not arquivos_csv:
        print(f"ERRO: Nenhum arquivo CSV encontrado em '{caminho_base}'.")
        return None
    lista_dfs = [pd.read_csv(f, dtype=str) for f in arquivos_csv]
    df_completo = pd.concat(lista_dfs, ignore_index=True)
    print(f"Dados consolidados com sucesso. Total de {len(df_completo)} linhas.")
    return df_completo

def padronizar_nomes_colunas(df):
    print("Nomes das colunas padronizados.")
    novas_colunas = []
    for col in df.columns:
        col = str(col)
        col_normalizada = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8')
        col_minuscula = col_normalizada.lower()
        col_com_underscore = col_minuscula.replace(' ', '_').replace('-', '_')
        col_final = re.sub(r'[^\w_]', '', col_com_underscore)
        novas_colunas.append(col_final)
    df.columns = novas_colunas
    return df

def limpar_dataframe_scopus(df):
    print("Iniciando limpeza do DataFrame...")
    df = df.copy()
    limiar = 60.0
    percentual_ausente = (df.isnull().sum() / len(df)) * 100
    colunas_para_remover = percentual_ausente[percentual_ausente > limiar].index.tolist()
    df = df.drop(columns=colunas_para_remover)
    print(f"Colunas com mais de {limiar}% de ausência foram removidas.")
    for coluna in df.columns:
        if df[coluna].isnull().any():
            if df[coluna].dtype == 'object':
                df[coluna] = df[coluna].fillna('nao_informado')
            else:
                df[coluna] = df[coluna].fillna(0)
    print("Valores ausentes restantes foram preenchidos.")
    colunas_para_converter = ['year', 'volume', 'issue', 'page_start', 'page_end', 'page_count', 'cited_by']
    for coluna in colunas_para_converter:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0).astype(int)
    print("Tipos de dados corrigidos.")
    num_duplicatas = df.duplicated().sum()
    if num_duplicatas > 0:
        df = df.drop_duplicates(keep='first')
        print(f"{num_duplicatas} linhas duplicadas foram removidas.")
    print("Limpeza do DataFrame concluída.")
    return df

def criar_modelo_autores(df_limpo):
    print("Iniciando a criação do modelo de autores (versão robusta)...")
    # (código da função omitido por brevidade, mas continua o mesmo)
    colunas_autores = ['eid', 'authors', 'authors_id', 'author_full_names']
    if not all(col in df_limpo.columns for col in colunas_autores):
        return None, None
    df_autores_trab = df_limpo[colunas_autores].copy()
    df_sobrenomes = df_autores_trab[['eid', 'authors']].copy()
    df_sobrenomes['authors'] = df_sobrenomes['authors'].str.replace(r'\s*\(\d+\)', '', regex=True)
    df_sobrenomes['authors'] = df_sobrenomes['authors'].str.split(r'\s*;\s*', regex=True)
    df_sobrenomes = df_sobrenomes.explode('authors').dropna(subset=['authors'])
    df_sobrenomes['author_index'] = df_sobrenomes.groupby('eid').cumcount()
    df_nomes_completos = df_autores_trab[['eid', 'author_full_names']].copy()
    df_nomes_completos['author_full_names'] = df_nomes_completos['author_full_names'].str.split(r'\s*;\s*', regex=True)
    df_nomes_completos = df_nomes_completos.explode('author_full_names').dropna(subset=['author_full_names'])
    df_nomes_completos['author_index'] = df_nomes_completos.groupby('eid').cumcount()
    df_nomes_completos['id_from_name'] = df_nomes_completos['author_full_names'].str.extract(r'\((\d+)\)')
    df_nomes_completos['author_full_names'] = df_nomes_completos['author_full_names'].str.replace(r'\s*\(\d+\)$', '', regex=True).str.strip()
    df_ids = df_autores_trab[['eid', 'authors_id']].copy()
    df_ids['authors_id'] = df_ids['authors_id'].str.split(r'\s*;\s*', regex=True)
    df_ids = df_ids.explode('authors_id').dropna(subset=['authors_id'])
    df_nomes_alinhados = pd.merge(df_sobrenomes, df_nomes_completos, on=['eid', 'author_index'], how='outer')
    df_alinhado = pd.merge(
        df_nomes_alinhados, df_ids,
        left_on=['eid', 'id_from_name'], right_on=['eid', 'authors_id'],
        how='right'
    )
    df_alinhado['author_full_names'].fillna(df_alinhado['authors'], inplace=True)
    df_alinhado['author_full_names'] = df_alinhado['author_full_names'].str.upper()
    dim_autores = df_alinhado[['authors_id', 'author_full_names']].copy()
    dim_autores.rename(columns={'author_full_names': 'nome_completo'}, inplace=True)
    dim_autores = dim_autores.drop_duplicates(subset=['authors_id']).dropna(subset=['authors_id'])
    dim_autores = dim_autores[dim_autores['authors_id'] != 'nao_informado']
    print(f"Criada dim_autores com {len(dim_autores)} autores únicos.")
    pon_artigo_autores = df_alinhado[['eid', 'authors_id']].drop_duplicates().dropna()
    pon_artigo_autores.rename(columns={'eid': 'article_id'}, inplace=True)
    print(f"Criada pon_artigo_autores com {len(pon_artigo_autores)} relações.")
    return dim_autores, pon_artigo_autores

def _parse_e_normalizar_afiliacao(texto_afiliacao):
    if not isinstance(texto_afiliacao, str):
        return 'NAO_INFORMADO', 'NAO_INFORMADO', 'NAO_INFORMADO'
    partes = texto_afiliacao.split(',', 1)
    nome_instituicao = partes[0].strip()
    endereco = partes[1].strip() if len(partes) > 1 else 'NAO_INFORMADO'
    texto_sem_acentos = unicodedata.normalize('NFKD', nome_instituicao).encode('ascii', 'ignore').decode('utf-8')
    nome_normalizado = re.sub(r'[.,;:"()]', '', texto_sem_acentos.upper())
    nome_normalizado = re.sub(r'\s+', ' ', nome_normalizado).strip()
    siglas = re.findall(r'\b([A-Z]{2,6})\b', nome_instituicao)
    if siglas:
        sigla = siglas[-1]
    else:
        siglas_parenteses = re.findall(r'\(([A-Z\.]+)\)', nome_instituicao)
        sigla = siglas_parenteses[-1].replace('.', '') if siglas_parenteses else 'SEM SIGLA'
    return nome_normalizado, endereco, sigla

def criar_modelo_afiliacoes(df_limpo):
    print("Iniciando a criação do modelo de afiliações (com separação de endereço)...")
    # (código da função omitido por brevidade, mas continua o mesmo)
    if 'affiliations' not in df_limpo.columns or 'eid' not in df_limpo.columns:
        return None, None
    df_afiliacoes_trab = df_limpo[['eid', 'affiliations']].copy()
    df_afiliacoes_trab = df_afiliacoes_trab[df_afiliacoes_trab['affiliations'] != 'nao_informado']
    df_afiliacoes_trab['affiliations'] = df_afiliacoes_trab['affiliations'].str.split('; ')
    df_explodido = df_afiliacoes_trab.explode('affiliations', ignore_index=True)
    df_explodido.rename(columns={'affiliations': 'affiliation_full_text'}, inplace=True)
    df_explodido.dropna(subset=['affiliation_full_text'], inplace=True)
    textos_unicos_df = pd.DataFrame(df_explodido['affiliation_full_text'].drop_duplicates())
    parsed_data = textos_unicos_df['affiliation_full_text'].apply(_parse_e_normalizar_afiliacao)
    textos_unicos_df[['nome_normalizado', 'endereco', 'sigla']] = pd.DataFrame(parsed_data.tolist(), index=textos_unicos_df.index)
    dim_afiliacoes = textos_unicos_df.drop_duplicates(subset=['nome_normalizado'])
    dim_afiliacoes['affiliation_id'] = [str(uuid.uuid4()) for _ in range(len(dim_afiliacoes))]
    dim_afiliacoes = dim_afiliacoes[['affiliation_id', 'nome_normalizado', 'sigla', 'endereco', 'affiliation_full_text']]
    print(f"Criada dim_afiliacoes com {len(dim_afiliacoes)} afiliações únicas (após normalização).")
    pon_artigo_afiliacoes = pd.merge(
        df_explodido, dim_afiliacoes[['affiliation_id', 'affiliation_full_text']],
        on='affiliation_full_text', how='left'
    )
    pon_artigo_afiliacoes = pon_artigo_afiliacoes[['eid', 'affiliation_id']].drop_duplicates().dropna()
    pon_artigo_afiliacoes.rename(columns={'eid': 'article_id'}, inplace=True)
    print(f"Criada pon_artigo_afiliacoes com {len(pon_artigo_afiliacoes)} relações.")
    return dim_afiliacoes, pon_artigo_afiliacoes


# --- FUNÇÃO GENÉRICA NOVA ---
def criar_modelo_generico(df_limpo, nome_coluna, nome_entidade):
    """
    Função genérica para criar uma dimensão e uma tabela ponte a partir de uma coluna multivalorada.
    
    Args:
        df_limpo (pd.DataFrame): O DataFrame principal e limpo.
        nome_coluna (str): O nome da coluna a ser processada (ex: 'author_keywords').
        nome_entidade (str): O nome base para as novas colunas e tabelas (ex: 'keyword').
        
    Returns:
        (pd.DataFrame, pd.DataFrame): A tabela de dimensão e a tabela ponte.
    """
    print(f"Iniciando criação do modelo genérico para '{nome_coluna}'...")
    
    if nome_coluna not in df_limpo.columns or 'eid' not in df_limpo.columns:
        print(f"Aviso: Coluna '{nome_coluna}' ou 'eid' não encontrada. Pulando.")
        return None, None

    df_trab = df_limpo[['eid', nome_coluna]].copy()
    df_trab = df_trab[df_trab[nome_coluna] != 'nao_informado']

    df_trab[nome_coluna] = df_trab[nome_coluna].str.split(r'\s*;\s*', regex=True)
    df_explodido = df_trab.explode(nome_coluna, ignore_index=True)
    df_explodido.dropna(subset=[nome_coluna], inplace=True)
    
    # Padroniza os valores (ex: caixa alta)
    df_explodido[nome_coluna] = df_explodido[nome_coluna].str.upper().str.strip()

    # Cria a tabela de dimensão
    valores_unicos = df_explodido[nome_coluna].drop_duplicates().tolist()
    dim_df = pd.DataFrame(valores_unicos, columns=[nome_entidade])
    dim_df[f'{nome_entidade}_id'] = [str(uuid.uuid4()) for _ in range(len(dim_df))]
    dim_df = dim_df[[f'{nome_entidade}_id', nome_entidade]]
    print(f"Criada dim_{nome_entidade} com {len(dim_df)} valores únicos.")

    # Cria a tabela ponte
    pon_df = pd.merge(df_explodido, dim_df, left_on=nome_coluna, right_on=nome_entidade, how='left')
    pon_df = pon_df[['eid', f'{nome_entidade}_id']].drop_duplicates()
    pon_df.rename(columns={'eid': 'article_id'}, inplace=True)
    print(f"Criada pon_artigo_{nome_entidade} com {len(pon_df)} relações.")
    
    return dim_df, pon_df

# ==============================================================================
# FUNÇÃO PRINCIPAL (ORQUESTRADOR - ATUALIZADO)
# ==============================================================================

def main():
    """Função principal que orquestra todo o processo."""
    caminho_script = Path(__file__).parent
    caminho_repo_raiz = caminho_script.parent
    caminho_dados_raw = caminho_repo_raiz / 'data' / 'raw' / 'scopus_input'
    caminho_dados_processados = caminho_repo_raiz / 'data' / 'processed'
    caminho_dados_processados.mkdir(parents=True, exist_ok=True)
    
    df_raw = carregar_e_consolidar_dados(caminho_dados_raw)
    
    if df_raw is not None:
        df_padronizado = padronizar_nomes_colunas(df_raw)
        df_final_limpo = limpar_dataframe_scopus(df_padronizado)
        
        # --- Dicionário para guardar todas as tabelas geradas ---
        tabelas_finais = {}
        
        # --- Executa a criação dos modelos dimensionais ---
        dim_autores, pon_artigo_autores = criar_modelo_autores(df_final_limpo)
        tabelas_finais['dim_autores_scopus'] = dim_autores
        tabelas_finais['pon_artigo_autores_scopus'] = pon_artigo_autores
        
        dim_afiliacoes, pon_artigo_afiliacoes = criar_modelo_afiliacoes(df_final_limpo)
        tabelas_finais['dim_afiliacoes_scopus'] = dim_afiliacoes
        tabelas_finais['pon_artigo_afiliacoes_scopus'] = pon_artigo_afiliacoes

        # --- Usa a função genérica para as demais dimensões ---
        # Lista de tuplas: (nome_da_coluna_original, nome_da_entidade_singular)
        colunas_genericas_para_processar = [
            ('author_keywords', 'keyword'),
            ('index_keywords', 'index_keyword')
        ]
        
        for coluna, entidade in colunas_genericas_para_processar:
            dim_gen, pon_gen = criar_modelo_generico(df_final_limpo, coluna, entidade)
            tabelas_finais[f'dim_{entidade}s_scopus'] = dim_gen # ex: dim_keywords_scopus
            tabelas_finais[f'pon_artigo_{entidade}s_scopus'] = pon_gen # ex: pon_artigo_keywords_scopus
            
        # --- Salva todos os arquivos finais ---
        print("\n--- Salvando todos os arquivos processados ---")
        for nome_tabela, df_tabela in tabelas_finais.items():
            if df_tabela is not None:
                caminho_saida = caminho_dados_processados / f'{nome_tabela}.csv'
                df_tabela.to_csv(caminho_saida, index=False)
                print(f"Salvo: {nome_tabela}.csv")
        
        # O DataFrame principal (futura tabela fato) ainda precisa ser ajustado
        df_final_limpo.to_csv(caminho_dados_processados / 'scopus_dados_limpos_temp.csv', index=False)
        print(f"Salvo: scopus_dados_limpos_temp.csv")

        print(f"\nProcessamento concluído. Arquivos salvos em: {caminho_dados_processados}")

if __name__ == "__main__":
    main()