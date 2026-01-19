import pandas as pd
from pathlib import Path
import sys
import csv

def carregar_csv_robusto(filepath, dtypes=None):
    """
    Função auxiliar para carregar um CSV usando o engine='python',
    tratando erros e opcionalmente forçando tipos de dados.
    """
    try:
        # Passa o argumento 'dtype' diretamente para o read_csv
        df = pd.read_csv(filepath, engine='python', dtype=dtypes) 
        
        if df.empty:
            print(f"  - Aviso: O arquivo '{filepath}' está vazio.")
        return df
    except (FileNotFoundError, pd.errors.EmptyDataError):
        print(f"  - Aviso: Arquivo '{filepath}' não encontrado ou vazio. Retornando DataFrame vazio.")
        return pd.DataFrame()
    except Exception as e:
        print(f"✖ ERRO ao ler o arquivo '{filepath}': {e}")
        sys.exit(1)

def to_str_safe(df, col_name):
    """Converte uma coluna para string de forma segura."""
    if df is not None and not df.empty and col_name in df.columns:
        df[col_name] = df[col_name].astype(str)
    return df

def main():
    """
    Script que cria as tabelas finais, otimizadas para o Looker Studio.
    """
    print("--- INICIANDO CRIAÇÃO DE TABELAS FINAIS PARA O LOOKER STUDIO ---")
    
    # --- CORREÇÃO APLICADA AQUI ---
    processed_dir = Path("data/processed")
    raw_dir = Path("data/raw") # <-- Variável que estava faltando
    looker_output_dir = Path("looker_studio_output")
    looker_output_dir.mkdir(exist_ok=True)
    
    # ==============================================================================
    # 1. CARREGAMENTO ROBUSTO DE TODOS OS ARQUIVOS
    # ==============================================================================
    print("⏳ Carregando todos os arquivos de dados processados...")
    # Defina suas chaves de merge aqui
    chaves_artigo = {'article_id': str, 'eid': str, 'EID_Artigo': str}
    chaves_patente = {'publication_number': str}
    chaves_ids_dim = {
        'especie_id': str,
        'authors_id': str,
        'affiliation_id': str,
        'keyword_id': str,
        'index_keyword_id': str,
        'ipc_id': str,
        'ipc_code': str,
        'country_id': str,
        'party_id': str
    }
    
    # CNCFlora
    df_dim_especies = carregar_csv_robusto(processed_dir / 'dim_especies_mestre.csv')
    df_pon_patente_especie = carregar_csv_robusto(processed_dir / 'espacenet/pon_patente_especie.csv')
    df_pon_artigo_especie = carregar_csv_robusto(processed_dir / 'pon_artigo_especie.csv')

    # Scopus
    df_artigos_base = carregar_csv_robusto(processed_dir / 'scopus_dados_limpos_temp.csv')
    df_dim_autores = carregar_csv_robusto(processed_dir / 'dim_autores_scopus.csv', dtypes=chaves_ids_dim)
    df_pon_autores = carregar_csv_robusto(processed_dir / 'pon_artigo_autores_scopus.csv', dtypes={**chaves_artigo, **chaves_ids_dim})
    df_dim_afiliacoes = carregar_csv_robusto(processed_dir / 'dim_afiliacoes_scopus.csv')
    df_pon_afiliacoes = carregar_csv_robusto(processed_dir / 'pon_artigo_afiliacoes_scopus.csv')
    df_dim_keywords = carregar_csv_robusto(processed_dir / 'dim_keywords_scopus.csv')
    df_pon_keywords = carregar_csv_robusto(processed_dir / 'pon_artigo_keywords_scopus.csv')
    df_dim_index_keywords = carregar_csv_robusto(processed_dir / 'dim_index_keywords_scopus.csv')
    df_pon_index_keywords = carregar_csv_robusto(processed_dir / 'pon_artigo_index_keywords_scopus.csv')
    
    df_ods_manual = carregar_csv_robusto(raw_dir / 'scopus_ods_v1.csv')

    # Espacenet
    df_fato_patentes = carregar_csv_robusto(processed_dir / 'espacenet/fato_patentes_espacenet.csv')
    df_ipc_green = carregar_csv_robusto(processed_dir / 'espacenet/ipc_classificado_green.csv')
    df_pon_ipc = carregar_csv_robusto(processed_dir / 'espacenet/pon_patente_ipc.csv')
    df_dim_ipc = carregar_csv_robusto(processed_dir / 'espacenet/dim_ipc_enriquecida.csv')
    df_dim_country = carregar_csv_robusto(processed_dir / 'espacenet/dim_country.csv')
    df_pon_country = carregar_csv_robusto(processed_dir / 'espacenet/pon_patente_country.csv')
    df_dim_partes = carregar_csv_robusto(processed_dir / 'espacenet/dim_parties.csv')
    df_pon_partes = carregar_csv_robusto(processed_dir / 'espacenet/pon_patente_party.csv')
    df_pon_primeira_public = carregar_csv_robusto(processed_dir / 'espacenet/pon_patente_ano_primeira_publicacao.csv')
    df_pon_prioridade = carregar_csv_robusto(processed_dir / 'espacenet/pon_patente_ano_prioridade.csv')
    df_pon_publicacao = carregar_csv_robusto(processed_dir / 'espacenet/pon_patente_ano_publicacao.csv')

    print("✔ Todos os arquivos carregados com sucesso.")

    # ==============================================================================
    # 2. GARANTIR CONSISTÊNCIA DOS TIPOS DE DADOS
    # ==============================================================================
    print("⏳ Garantindo a consistência dos tipos de dados para os merges...")
    
   # --- CHAVES DE ARTIGO ('article_id' ou 'eid') ---
    df_artigos_base = to_str_safe(df_artigos_base, 'eid')
    df_pon_autores = to_str_safe(df_pon_autores, 'article_id')
    df_pon_afiliacoes = to_str_safe(df_pon_afiliacoes, 'article_id')
    df_pon_keywords = to_str_safe(df_pon_keywords, 'article_id')
    df_pon_index_keywords = to_str_safe(df_pon_index_keywords, 'article_id')
    df_pon_artigo_especie = to_str_safe(df_pon_artigo_especie, 'article_id')
    df_ods_manual = to_str_safe(df_ods_manual, 'EID_Artigo')

    # --- CHAVES DE PATENTE ('publication_number') ---
    df_fato_patentes = to_str_safe(df_fato_patentes, 'publication_number')
    df_pon_country = to_str_safe(df_pon_country, 'publication_number')
    df_pon_patente_especie = to_str_safe(df_pon_patente_especie, 'publication_number')
    df_pon_ipc = to_str_safe(df_pon_ipc, 'publication_number')
    df_pon_partes = to_str_safe(df_pon_partes, 'publication_number')
    df_pon_publicacao = to_str_safe(df_pon_publicacao, 'publication_number')
    df_pon_prioridade = to_str_safe(df_pon_prioridade, 'publication_number')
    df_pon_primeira_public = to_str_safe(df_pon_primeira_public, 'publication_number')

    # --- CHAVES DE DIMENSÃO (IDs únicos) ---
    # Espécies
    df_dim_especies = to_str_safe(df_dim_especies, 'especie_id')
    df_pon_artigo_especie = to_str_safe(df_pon_artigo_especie, 'especie_id')
    # Autores
    df_dim_autores = to_str_safe(df_dim_autores, 'authors_id')
    df_pon_autores = to_str_safe(df_pon_autores, 'authors_id')
    # Afiliações
    df_dim_afiliacoes = to_str_safe(df_dim_afiliacoes, 'affiliation_id')
    df_pon_afiliacoes = to_str_safe(df_pon_afiliacoes, 'affiliation_id')
    # Keywords
    df_dim_keywords = to_str_safe(df_dim_keywords, 'keyword_id')
    df_pon_keywords = to_str_safe(df_pon_keywords, 'keyword_id')
    df_dim_index_keywords = to_str_safe(df_dim_index_keywords, 'index_keyword_id')
    df_pon_index_keywords = to_str_safe(df_pon_index_keywords, 'index_keyword_id')
    # IPC
    df_ipc_green = to_str_safe(df_ipc_green, 'ipc_code')
    df_dim_ipc = to_str_safe(df_dim_ipc, 'ipc_id')
    df_pon_ipc = to_str_safe(df_pon_ipc, 'ipc_id') # A chave aqui é o ID numérico
    # Country e Parties
    df_dim_country = to_str_safe(df_dim_country, 'country_id')
    df_pon_country = to_str_safe(df_pon_country, 'country_id')
    df_dim_partes = to_str_safe(df_dim_partes, 'party_id')
    df_pon_partes = to_str_safe(df_pon_partes, 'party_id')
    
    print("✔ Tipos de dados consistentes.")

    # ==============================================================================
    # 3. CRIAÇÃO E SALVAMENTO DAS TABELAS DE RELAÇÃO (ARTIGOS)
    # ==============================================================================
    print("\n⏳ Criando e salvando tabelas de relação para Artigos...")
    
    # --- Tabela: art_fato ---
    if not df_artigos_base.empty:
        colunas = ['eid', 'source', 'open_access', 'language_of_original_document', 'funding_details', 'abstract', 'cited_by', 'doi', 'link', 'year']
        df_art_fato = df_artigos_base[[c for c in colunas if c in df_artigos_base.columns]].rename(columns={'eid': 'article_id', 'language_of_original_document': 'language', 'year': 'Year'})
        df_art_fato.to_csv(looker_output_dir / "art_fato.csv", index=False, quoting=csv.QUOTE_ALL)
        print("  - Salvo: art_fato.csv")
    
    # Relação Artigo <-> Autores
    if not df_pon_autores.empty and not df_dim_autores.empty:
        # O merge agora vai funcionar, pois os tipos foram forçados na leitura
        df_artigo_autor = pd.merge(df_pon_autores, df_dim_autores, on='authors_id', how='left')
        
        df_artigo_autor['nome_completo'] = df_artigo_autor['nome_completo'].fillna('AUTOR NÃO ENCONTRADO')
        
        df_artigo_autor.to_csv(looker_output_dir / "looker_artigo_autor.csv", index=False, quoting=csv.QUOTE_ALL)
        print("  - Salvo: looker_artigo_autor.csv")


    # --- Relação Artigo <-> Keywords (Unificadas) ---
    print("  - Criando tabela unificada de Keywords...")

    # Processa as Author Keywords
    if not df_pon_keywords.empty and not df_dim_keywords.empty:
        df_artigo_keyword = pd.merge(df_pon_keywords, df_dim_keywords, on='keyword_id', how='left')
        df_artigo_keyword['tipo_keyword'] = 'Author' # Adiciona a coluna de tipo
        # Renomeia a coluna 'keyword' para um nome genérico
        df_artigo_keyword.rename(columns={'keyword': 'termo'}, inplace=True)
    else:
        df_artigo_keyword = pd.DataFrame() # Cria um DataFrame vazio se não houver dados

    # Processa as Index Keywords
    if not df_pon_index_keywords.empty and not df_dim_index_keywords.empty:
        df_artigo_index_keyword = pd.merge(df_pon_index_keywords, df_dim_index_keywords, on='index_keyword_id', how='left')
        df_artigo_index_keyword['tipo_keyword'] = 'Index' # Adiciona a coluna de tipo
        # Renomeia a coluna 'index_keyword' para o mesmo nome genérico
        df_artigo_index_keyword.rename(columns={'index_keyword': 'termo'}, inplace=True)
    else:
        df_artigo_index_keyword = pd.DataFrame() # Cria um DataFrame vazio se não houver dados

    # Concatena (une) as duas tabelas em uma só
    df_keywords_unificada = pd.concat([df_artigo_keyword, df_artigo_index_keyword], ignore_index=True)

    # Seleciona e reordena as colunas finais
    if not df_keywords_unificada.empty:
        # Seleciona apenas as colunas que importam para a tabela final
        colunas_finais_keywords = ['article_id', 'termo', 'tipo_keyword']
        df_keywords_unificada_final = df_keywords_unificada[colunas_finais_keywords]
        
        # Salva o arquivo unificado
        df_keywords_unificada_final.to_csv(looker_output_dir / "looker_artigo_keywords_unificadas.csv", index=False, quoting=csv.QUOTE_ALL)
    print("  - Salvo: looker_artigo_keywords_unificadas.csv")
    
    # Relação Artigo <-> Espécie
    if not df_pon_artigo_especie.empty:
        df_art_especie = pd.merge(df_pon_artigo_especie, df_dim_especies, on='especie_id', how='left')
        # Renomeie as colunas de df_dim_especies se os nomes forem diferentes do que você listou
        colunas = ['article_id', 'especie_id', 'nome_cientifico', 'grupo', 'nome_popular', 'categoria_de_risco_de_extinção', 'data_avaliação', 'reavaliação?', 'histórico_de_avaliações', 'classificação_de_ameaças', 'classificação_ações_de_conservação']
        df_art_especie = df_art_especie[[c for c in colunas if c in df_art_especie.columns]]
        df_art_especie.to_csv(looker_output_dir / "art_especie.csv", index=False, quoting=csv.QUOTE_ALL)
        print("  - Salvo: art_especie.csv")

    # --- Tabela: art_ods (LÓGICA SIMPLIFICADA) ---
    if not df_ods_manual.empty:
        # Renomeia as colunas para o padrão final
        df_art_ods = df_ods_manual.rename(columns={
            'EID_Artigo': 'article_id',
            'nro_ods': 'ods_id',
            'titulo_ods': 'ods_titulo'
        })
        
        # Seleciona apenas as colunas desejadas na ordem certa
        colunas = ['article_id', 'ods_id', 'ods_titulo']
        df_art_ods = df_art_ods[[c for c in colunas if c in df_art_ods.columns]]
        
        df_art_ods.to_csv(looker_output_dir / "art_ods.csv", index=False, quoting=csv.QUOTE_ALL)
        print("  - Salvo: art_ods.csv")


    # ==============================================================================
    # 4. CRIAÇÃO E SALVAMENTO DAS TABELAS DE RELAÇÃO (PATENTES)
    # ==============================================================================
    print("\n⏳ Criando e salvando tabelas de relação para Patentes...")

    df_patentes_final = df_fato_patentes.copy()
    df_patentes_final.to_csv(looker_output_dir / "looker_fato_patentes.csv", index=False, quoting=csv.QUOTE_ALL)
    print("  - Salvo: looker_fato_patentes.csv")
    # --- JUNÇÃO COM OS ANOS ---
    # Para cada tipo de ano, fazemos um 'left merge' para adicionar a coluna
    # O 'left merge' garante que todas as patentes da tabela fato sejam mantidas.
    if not df_pon_publicacao.empty:
        # Como uma patente pode ter múltiplos anos de publicação, pegamos apenas o primeiro (ou o mais recente)
        # Usamos .drop_duplicates para garantir uma relação 1-para-1
        primeiro_ano_pub = df_pon_publicacao.sort_values('ano').drop_duplicates('publication_number', keep='first')
        df_patentes_final = pd.merge(
            df_patentes_final,
            primeiro_ano_pub[['publication_number', 'ano']],
            on='publication_number',
            how='left'
        )
        df_patentes_final.rename(columns={'ano': 'ano_publicacao'}, inplace=True)
        print("  - Informação de 'Ano de Publicação' adicionada.")

    if not df_pon_prioridade.empty:
        primeiro_ano_prio = df_pon_prioridade.sort_values('ano').drop_duplicates('publication_number', keep='first')
        df_patentes_final = pd.merge(
            df_patentes_final,
            primeiro_ano_prio[['publication_number', 'ano']],
            on='publication_number',
            how='left'
        )
        df_patentes_final.rename(columns={'ano': 'ano_prioridade'}, inplace=True)
        print("  - Informação de 'Ano de Prioridade' adicionada.")

    if not df_pon_primeira_public.empty:
        primeiro_ano_pp = df_pon_primeira_public.sort_values('ano').drop_duplicates('publication_number', keep='first')
        df_patentes_final = pd.merge(
            df_patentes_final,
            primeiro_ano_pp[['publication_number', 'ano']],
            on='publication_number',
            how='left'
        )
        df_patentes_final.rename(columns={'ano': 'ano_primeira_publicacao'}, inplace=True)
        print("  - Informação de 'Ano de Primeira Publicação' adicionada.")

    # --- Salva a Tabela Fato ENRIQUECIDA ---
    df_patentes_final.to_csv(looker_output_dir / "looker_fato_patentes.csv", index=False, quoting=csv.QUOTE_ALL)
    print("  - Salvo: looker_fato_patentes.csv (agora com colunas de ano)")

    # Relação Patente <-> IPC (com info Green)
    if not df_pon_ipc.empty and not df_dim_ipc.empty and not df_ipc_green.empty:
        # 1. Enriquece a dimensão de IPC com a informação "green"
        # A chave entre dim_ipc e ipc_green é 'ipc_code'
        df_dim_ipc_enriquecido = pd.merge(df_dim_ipc, df_ipc_green.drop(columns=['ipc_id'], errors='ignore'), on='ipc_code', how='left')

    # 2. Junta a tabela ponte com a dimensão já enriquecida
    # A chave entre pon_ipc e a dimensão é 'ipc_id'
    df_patente_ipc = pd.merge(df_pon_ipc, df_dim_ipc_enriquecido, on='ipc_id', how='left')
    
    df_patente_ipc.to_csv(looker_output_dir / "looker_patente_ipc.csv", index=False, quoting=csv.QUOTE_ALL)
    print("  - Salvo: looker_patente_ipc.csv")

    # --- Relação Patente <-> Espécies (COM CORREÇÃO) ---
    if not df_pon_patente_especie.empty and not df_dim_especies.empty:
    
        # 1. Garante que as chaves de junção são do mesmo tipo (string) e sem espaços
        df_pon_patente_especie['especie_id'] = df_pon_patente_especie['especie_id'].astype(str).str.strip()
        df_dim_especies['especie_id'] = df_dim_especies['especie_id'].astype(str).str.strip()

        # 2. Faz o merge com as chaves limpas
        df_patente_especie = pd.merge(df_pon_patente_especie, df_dim_especies, on='especie_id', how='left')
        
        # 3. Preenche nomes não encontrados para depuração
        # Supondo que a coluna de nome em df_dim_especies se chama 'nome_cientifico'
        if 'nome_cientifico' in df_patente_especie.columns:
            df_patente_especie['nome_cientifico'] = df_patente_especie['nome_cientifico'].fillna('ESPÉCIE NÃO ENCONTRADA')
        
        df_patente_especie.to_csv(looker_output_dir / "looker_patente_especie.csv", index=False, quoting=csv.QUOTE_ALL)
        print("  - Salvo: looker_patente_especie.csv")

    # Relação Patente <-> Partes (Inventores, Depositantes)
    if not df_pon_partes.empty and not df_dim_partes.empty:
        # Junta a tabela ponte de partes com a dimensão de partes
        df_patente_partes = pd.merge(df_pon_partes, df_dim_partes, on='party_id', how='left')
        
        # Salva a tabela de relação resultante
        df_patente_partes.to_csv(looker_output_dir / "looker_patente_partes.csv", index=False, quoting=csv.QUOTE_ALL)
        print("  - Salvo: looker_patente_partes.csv")

    # Relação Patente <-> Países
    if not df_pon_country.empty and not df_dim_country.empty:
        df_patente_country = pd.merge(df_pon_country, df_dim_country, on='country_id', how='left')
        df_patente_country.to_csv(looker_output_dir / "looker_patente_country.csv", index=False, quoting=csv.QUOTE_ALL)
        print("  - Salvo: looker_patente_country.csv")
    
    # ==============================================================================
    # 5. SALVAMENTO DOS RESULTADOS
    # ==============================================================================
    looker_output_dir = Path("looker_studio_output")
    looker_output_dir.mkdir(exist_ok=True)
    
    print(f"✔ Tabelas finais salvas na pasta '{looker_output_dir}'")
    print("--- SCRIPT CONCLUÍDO COM SUCESSO ---")

if __name__ == "__main__":
    main()