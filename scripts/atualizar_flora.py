import pandas as pd
import os

print("--- INICIANDO PIPELINE DE ATUALIZAÇÃO DA FLORA (FORMATO MATRIZ AMPLA) ---")

def extrair_binomio(nome):
    """
    Remove autores, siglas e classificações extras, mantendo apenas 
    o binômio puro (Gênero + epíteto específico) padronizado.
    Exemplo: 'Justicia birae A.S.Reis' -> 'Justicia birae'
    """
    if pd.isna(nome):
        return ""
    palavras = str(nome).strip().split()
    if len(palavras) >= 2:
        return f"{palavras[0].capitalize()} {palavras[1].lower()}"
    elif len(palavras) == 1:
        return palavras[0].capitalize()
    return ""

def carregar_novos_dados_flora():
    print("Passo 1: Filtrando distribuição (Estritamente Endêmicas Exclusivas da Amazônia)...")
    df_dist = pd.read_csv('/workspaces/sismm-cti-amazon/data/raw/cncflora/atualizacao_plantas_v1/distribution.txt', sep='\t', low_memory=False)
    
    # Filtro estrito de exclusividade amazônica
    filtro_exclusivo = df_dist['occurrenceRemarks'].str.contains(r'"endemism":"Endemica"', na=False) & \
                       df_dist['occurrenceRemarks'].str.contains(r'"phytogeographicDomain":\["Amazônia"\]', na=False)
    
    df_dist_filtrado = df_dist[filtro_exclusivo]
    ids_validos = df_dist_filtrado['id'].unique()
    
    print(f"-> Sucesso: Encontradas {len(ids_validos)} ocorrências estritamente amazônicas.")
    
    print("Passo 2: Cruzando com a taxonomia...")
    df_taxon = pd.read_csv('/workspaces/sismm-cti-amazon/data/raw/cncflora/atualizacao_plantas_v1/taxon.txt', sep='\t', low_memory=False)
    
    df_novas_plantas = df_taxon[df_taxon['id'].isin(ids_validos)].copy()
    
    colunas_chave = ['id', 'scientificName', 'family', 'genus', 'taxonomicStatus', 'acceptedNameUsage']
    return df_novas_plantas[colunas_chave]

def integrar_e_comparar():
    caminho_base_antiga = '/workspaces/sismm-cti-amazon/data/raw/cncflora/termos_plantas.txt'
    caminho_base_nova = '/workspaces/sismm-cti-amazon/data/processed/amazon_endemics_matriz.csv'
    
    # Garante que a pasta destino existe
    pasta_destino = os.path.dirname(caminho_base_nova)
    if not os.path.exists(pasta_destino):
        print(f"Criando a pasta de destino: {pasta_destino}")
        os.makedirs(pasta_destino, exist_ok=True)
    
    # 1. Processa e limpa os dados novos de 2026
    df_novos = carregar_novos_dados_flora()
    df_novos['nome_binomio'] = df_novos['scientificName'].apply(extrair_binomio)
    
    # Ordena para priorizar 'NOME_ACEITO' antes de remover duplicatas de binômio
    df_novos = df_novos.sort_values(by='taxonomicStatus', ascending=True)
    df_novos_unique = df_novos.drop_duplicates(subset=['nome_binomio']).copy()
    df_novos_unique['Busca 2026'] = True
    
    # 2. Processa e limpa a lista antiga do Mestrado (2024)
    if os.path.exists(caminho_base_antiga):
        print("Passo 3: Tratando base do Mestrado e transpondo anos para colunas...")
        df_antigo_cru = pd.read_csv(caminho_base_antiga, sep=',', header=None, engine='python')
        df_antigo_cru['nome_binomio'] = df_antigo_cru[0].apply(extrair_binomio)
        
        df_antigo_unique = df_antigo_cru[['nome_binomio']].drop_duplicates().copy()
        df_antigo_unique['Busca 2024'] = True
        
        # 3. Cruzamento em formato Matriz (Outer Join pelas chaves limpas)
        df_consolidado = pd.merge(df_antigo_unique, df_novos_unique, on='nome_binomio', how='outer')
    else:
        print("Aviso: Base antiga não encontrada. Criando matriz apenas com dados de 2026...")
        df_consolidado = df_novos_unique
        df_consolidado['Busca 2024'] = False
    
    # Preenche valores nulos das buscas com False (indica ausência naquele ano)
    df_consolidado['Busca 2024'] = df_consolidado['Busca 2024'].fillna(False)
    df_consolidado['Busca 2026'] = df_consolidado['Busca 2026'].fillna(False)
    
    # Alinha metadados básicos para espécies que só existiam na lista antiga
    df_consolidado['scientificName'] = df_consolidado['scientificName'].fillna(df_consolidado['nome_binomio'])
    df_consolidado['genus'] = df_consolidado['genus'].fillna(df_consolidado['nome_binomio'].apply(lambda x: x.split()[0] if len(x.split()) > 0 else ""))
    
    # Reorganiza a disposição das colunas colocando os dados prioritários na frente
    colunas_ordenadas = [
        'nome_binomio', 'scientificName', 'Busca 2024', 'Busca 2026', 
        'id', 'family', 'genus', 'taxonomicStatus', 'acceptedNameUsage'
    ]
    df_consolidado = df_consolidado[colunas_ordenadas]
    
    # Salva a matriz consolidada na pasta de processados
    df_consolidado.to_csv(caminho_base_nova, index=False)
    print(f"\nSucesso! Matriz de dados históricos salva em: {caminho_base_nova}")
    
    # Imprime tabela cruzada de conferência no terminal
    print("\nTabela de Contingência Temporal (Resumo da Matriz):")
    print(pd.crosstab(df_consolidado['Busca 2024'], df_consolidado['Busca 2026'], margins=True))

if __name__ == "__main__":
    integrar_e_comparar()