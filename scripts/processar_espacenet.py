import pandas as pd
from pathlib import Path
import unicodedata
import re

# ==============================================================================
# ETAPA 1: FUNÇÕES DE CARREGAMENTO E LIMPEZA
# ==============================================================================

def carregar_dados_espacenet(caminho_pasta_raw):
    """Carrega e consolida todos os arquivos CSV da pasta de entrada da Espacenet."""
    print("Iniciando carregamento dos dados da Espacenet...")
    caminho_pasta = Path(caminho_pasta_raw)
    arquivos_csv = list(caminho_pasta.glob('*.csv'))
    
    if not arquivos_csv:
        print(f"ERRO: Nenhum arquivo CSV encontrado em '{caminho_pasta}'")
        return None
        
    lista_dfs = [pd.read_csv(f, sep=';') for f in arquivos_csv]
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    print(f"Arquivos consolidados com sucesso! Total de {len(df_consolidado)} registros.")
    return df_consolidado

def limpeza_inicial_espacenet(df):
    """Realiza a limpeza estrutural inicial: remove colunas, padroniza nomes e trata nulos."""
    print("Iniciando limpeza estrutural...")
    df_limpo = df.copy()
    
    colunas_unnamed = [col for col in df_limpo.columns if 'unnamed' in str(col).lower()]
    df_limpo = df_limpo.drop(columns=colunas_unnamed)
    
    novas_colunas = []
    for col in df_limpo.columns:
        col = str(col)
        col_normalizada = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8')
        col_minuscula = col_normalizada.lower()
        col_com_underscore = col_minuscula.replace(' ', '_').replace('-', '_')
        col_final = re.sub(r'[^a-z0-9_]', '', col_com_underscore)
        novas_colunas.append(col_final)
    df_limpo.columns = novas_colunas
    
    colunas_para_tratar = ['inventors', 'applicants', 'ipc', 'cpc', 'publication_number', 'publication_date']
    for col in colunas_para_tratar:
        if col in df_limpo.columns:
            df_limpo[col] = df_limpo[col].astype(str).fillna('NAO INFORMADO')
            
    print("Limpeza inicial concluída.")
    return df_limpo

# ==============================================================================
# ETAPA 2: FUNÇÕES DE MODELAGEM DIMENSIONAL
# ==============================================================================

def criar_modelo_parties(df_limpo):
    """Cria a dimensão de Parties (Inventores e Requerentes) e sua tabela ponte."""
    print("Criando modelo de Parties...")
    df_melted = df_limpo.melt(id_vars=['publication_number'], value_vars=['inventors', 'applicants'], var_name='role', value_name='party_string')
    df_melted = df_melted[df_melted['party_string'] != 'NAO INFORMADO']
    df_melted['party_string'] = df_melted['party_string'].str.split(',')
    df_explodido = df_melted.explode('party_string')
    df_explodido['party_string'] = df_explodido['party_string'].str.strip()
    df_explodido = df_explodido[df_explodido['party_string'] != '']
    
    dim_parties = pd.DataFrame(df_explodido['party_string'].unique(), columns=['party_nome'])
    dim_parties.reset_index(inplace=True); dim_parties.rename(columns={'index': 'party_id'}, inplace=True)
    
    pon_patente_party = pd.merge(df_explodido, dim_parties, left_on='party_string', right_on='party_nome', how='left')
    pon_patente_party['role'] = pon_patente_party['role'].str.replace('s', '')
    pon_patente_party = pon_patente_party[['publication_number', 'party_id', 'role']].drop_duplicates()
    
    return dim_parties, pon_patente_party

def criar_modelo_country(df_limpo, country_codes_dict):
    """Cria a dimensão de Países e sua tabela ponte."""
    print("Criando modelo de Países...")
    dim_country = pd.DataFrame(country_codes_dict.items(), columns=['country_code', 'country_name'])
    dim_country.reset_index(inplace=True); dim_country.rename(columns={'index': 'country_id'}, inplace=True)
    
    df_from_pub = df_limpo[['publication_number']].copy(); df_from_pub['country_code'] = df_from_pub['publication_number'].str[:2]; df_from_pub['origin'] = 'Publication'
    
    df_from_parties = df_limpo[['publication_number', 'inventors', 'applicants']].copy()
    df_melted_parties = df_from_parties.melt(id_vars=['publication_number'], value_vars=['inventors', 'applicants'], var_name='origin')
    df_melted_parties['value'] = df_melted_parties['value'].str.split(',')
    df_exploded_parties = df_melted_parties.explode('value')
    df_exploded_parties['country_code'] = df_exploded_parties['value'].str.extract(r'\[([A-Z]{2})\]')
    df_exploded_parties['origin'] = df_exploded_parties['origin'].str.replace('s', '').str.capitalize()

    df_relations = pd.concat([df_from_pub[['publication_number', 'country_code', 'origin']], df_exploded_parties[['publication_number', 'country_code', 'origin']]]).dropna(subset=['country_code'])
    df_relations = df_relations[df_relations['country_code'].isin(country_codes_dict.keys())]

    pon_patente_country = pd.merge(df_relations, dim_country, on='country_code', how='left')
    pon_patente_country = pon_patente_country[['publication_number', 'country_id', 'origin']].drop_duplicates()
    
    return dim_country, pon_patente_country

def criar_modelo_ipc(df_limpo):
    """Cria a dimensão de Classificação IPC e sua tabela ponte."""
    print("Criando modelo de IPC...")
    def clean_ipc_code(ipc_code_str):
        if pd.isna(ipc_code_str) or not isinstance(ipc_code_str, str): return None
        return ipc_code_str.upper().strip().replace('-', '').replace(' ', '')

    df_ipc_trab = df_limpo[['publication_number', 'ipc']].copy()
    df_ipc_trab = df_ipc_trab[df_ipc_trab['ipc'] != 'NAO INFORMADO']
    df_ipc_trab['ipc'] = df_ipc_trab['ipc'].str.split(',')
    df_ipc_explodido = df_ipc_trab.explode('ipc')
    df_ipc_explodido['ipc_code_normalizado'] = df_ipc_explodido['ipc'].apply(clean_ipc_code)
    df_ipc_explodido.dropna(subset=['ipc_code_normalizado'], inplace=True)
    df_ipc_explodido = df_ipc_explodido[df_ipc_explodido['ipc_code_normalizado'] != '']
    
    dim_ipc = pd.DataFrame(df_ipc_explodido['ipc_code_normalizado'].unique(), columns=['ipc_code'])
    dim_ipc.reset_index(inplace=True); dim_ipc.rename(columns={'index': 'ipc_id'}, inplace=True)
    
    pon_patente_ipc = pd.merge(df_ipc_explodido, dim_ipc, left_on='ipc_code_normalizado', right_on='ipc_code', how='left')
    pon_patente_ipc = pon_patente_ipc[['publication_number', 'ipc_id']].drop_duplicates()
    
    return dim_ipc, pon_patente_ipc

def criar_ligacao_especies_e_fato(df_limpo, df_manual, dim_especies_mestre):
    """Cria a ponte patente-espécie (usando a lista manual) e a tabela fato final."""
    print("Criando ligação com espécies e a tabela fato...")
    # Criar ponte Patente-Espécie
    df_manual_padronizado = df_manual.copy()
    df_manual_padronizado.columns = ['publication_number', 'abstract', 'nome_cientifico']
    pon_patente_especie = pd.merge(df_manual_padronizado, dim_especies_mestre, on='nome_cientifico', how='left')
    pon_patente_especie = pon_patente_especie[['publication_number', 'especie_id']].drop_duplicates().dropna()
    
    # Criar pontes de data
    tabelas_ponte_data = {}
    colunas_de_data = {'earliest_priority': 'ano_prioridade', 'publication_date': 'ano_publicacao', 'earliest_publication': 'ano_primeira_publicacao'}
    for col_original, nome_ponte in colunas_de_data.items():
        if col_original in df_limpo.columns:
            df_data_trab = df_limpo[['publication_number', col_original]].copy().dropna(subset=[col_original])
            df_data_trab[col_original] = df_data_trab[col_original].astype(str).str.split(',')
            df_data_explodido = df_data_trab.explode(col_original)
            df_data_explodido['ano'] = pd.to_datetime(df_data_explodido[col_original], errors='coerce').dt.year
            df_data_explodido.dropna(subset=['ano'], inplace=True)
            df_data_explodido['ano'] = df_data_explodido['ano'].astype(int)
            tabelas_ponte_data[f'pon_patente_{nome_ponte}'] = df_data_explodido[['publication_number', 'ano']].drop_duplicates()

    # Criar Tabela Fato
    colunas_para_remover = ['inventors', 'applicants', 'ipc', 'cpc', 'earliest_priority', 'publication_date', 'earliest_publication']
    colunas_existentes = [col for col in colunas_para_remover if col in df_limpo.columns]
    fato_patentes_espacenet = df_limpo.drop(columns=colunas_existentes)
    
    return fato_patentes_espacenet, pon_patente_especie, tabelas_ponte_data

# ==============================================================================
# FUNÇÃO PRINCIPAL (ORQUESTRADOR)
# ==============================================================================

def main():
    """Função principal que orquestra todo o processo para a Espacenet."""
    # Definição de Caminhos
    caminho_script = Path(__file__).parent
    caminho_repo_raiz = caminho_script.parent
    caminho_dados_raw = caminho_repo_raiz / 'data' / 'raw'
    caminho_dados_processados = caminho_repo_raiz / 'data' / 'processed'
    
    # Dicionário de Países
    COUNTRY_CODES = {'AR': 'Argentina', 'AT': 'Áustria', 'AU': 'Austrália', 'BR': 'Brasil', 'CA': 'Canadá', 'CH': 'Suíça', 'CN': 'China', 'DE': 'Alemanha', 'DK': 'Dinamarca', 'EP': 'Organização Europeia de Patentes (OPE/EPO)', 'ES': 'Espanha', 'FR': 'França', 'GB': 'Reino Unido', 'IL': 'Israel', 'IN': 'India', 'JP': 'Japão', 'KR': 'Coreia do Sul', 'RU': 'Federação Russa', 'US': 'Estados Unidos da América', 'WO': 'Organização Mundial da Propriedade Intelectual (OMPI/WIPO)', 'ZA': 'África do Sul'} # Versão resumida

    # Carga e Limpeza Inicial
    df_espacenet = carregar_dados_espacenet(caminho_dados_raw / 'espacenet_input')
    if df_espacenet is None: return
    df_espacenet_limpo = limpeza_inicial_espacenet(df_espacenet)
    
    # Criação das Dimensões e Pontes
    dim_parties, pon_patente_party = criar_modelo_parties(df_espacenet_limpo)
    dim_country, pon_patente_country = criar_modelo_country(df_espacenet_limpo, COUNTRY_CODES)
    dim_ipc, pon_patente_ipc = criar_modelo_ipc(df_espacenet_limpo)
    
    # Carregar arquivos manuais e mestre para a etapa final
    df_manual = pd.read_csv(caminho_dados_raw / 'espacenet_resumo_plantas.csv')
    dim_especies_mestre = pd.read_csv(caminho_dados_processados / 'dim_especies_mestre.csv')
    
    fato_patentes, pon_especie, pontes_data = criar_ligacao_especies_e_fato(df_espacenet_limpo, df_manual, dim_especies_mestre)
    
    # Salvar todos os arquivos
    caminho_saida = caminho_dados_processados / 'espacenet'
    caminho_saida.mkdir(parents=True, exist_ok=True)
    
    tabelas_para_salvar = {
        "fato_patentes_espacenet": fato_patentes, "dim_parties": dim_parties, "pon_patente_party": pon_patente_party,
        "dim_country": dim_country, "pon_patente_country": pon_patente_country, "dim_ipc": dim_ipc,
        "pon_patente_ipc": pon_patente_ipc, "pon_patente_especie": pon_especie
    }
    tabelas_para_salvar.update(pontes_data)
    
    print("\n--- Salvando todos os arquivos processados ---")
    for nome, df in tabelas_para_salvar.items():
        if df is not None:
            df.to_csv(caminho_saida / f"{nome}.csv", index=False)
            print(f"Salvo: {nome}.csv")
    
    print(f"\nProcessamento da Espacenet concluído. Arquivos salvos em: {caminho_saida}")

if __name__ == "__main__":
    main()