# scripts/processar_cncflora.py (VERSÃO CORRIGIDA)

import pandas as pd
from pathlib import Path
import unicodedata
import re

# (As funções auxiliares no topo do arquivo continuam as mesmas)
# ...
def carregar_dados_cncflora(caminho_pasta_raw):
    print("Iniciando carregamento dos dados da CNCFlora...")
    caminho_csv = caminho_pasta_raw / 'lista_vermelha_cnc_flora.csv'
    caminho_txt = caminho_pasta_raw / 'termos_plantas.txt'
    try:
        df_lista_vermelha = pd.read_csv(caminho_csv, sep=',')
        df_termos = pd.read_csv(caminho_txt, sep=',', header=None, names=['nome_cientifico_completo', 'grupo_taxonomico'])
        print("Arquivos carregados com sucesso.")
        return df_lista_vermelha, df_termos
    except FileNotFoundError as e:
        print(f"ERRO: Arquivo não encontrado. Verifique o caminho: {e}")
        return None, None

def padronizar_colunas_cncflora(df):
    novas_colunas = []
    for col in df.columns:
        col = str(col)
        col_normalizada = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8')
        col_minuscula = col_normalizada.lower()
        col_com_underscore = col_minuscula.replace(' ', '_').replace('/', '_')
        col_final = re.sub(r'[^a-z0-9_]', '', col_com_underscore)
        novas_colunas.append(col_final)
    df.columns = novas_colunas
    print("Nomes das colunas padronizados.")
    return df

def limpar_dados_base_cncflora(df):
    df = df.copy()
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'avaliacao_id'}, inplace=True)
    if 'nome_popular' in df.columns:
        df = df.drop(columns=['nome_popular'])
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('NAO INFORMADO')
    if 'data_avaliacao' in df.columns:
        df['data_avaliacao'] = pd.to_numeric(df['data_avaliacao'], errors='coerce').fillna(0).astype(int)
    print("Limpeza de dados base concluída (nulos, tipos).")
    return df

def criar_dim_e_ponte(df_limpo, id_coluna, coluna_multivalor, nome_entidade, delimitador='|'):
    print(f"Criando modelo para '{nome_entidade}'...")
    df_trab = df_limpo[[id_coluna, coluna_multivalor]].copy()
    df_trab = df_trab[df_trab[coluna_multivalor] != 'NAO INFORMADO']
    df_trab[coluna_multivalor] = df_trab[coluna_multivalor].str.split(delimitador)
    df_explodido = df_trab.explode(coluna_multivalor)
    df_explodido[coluna_multivalor] = df_explodido[coluna_multivalor].str.strip()
    df_explodido.dropna(subset=[coluna_multivalor], inplace=True)
    df_explodido = df_explodido[df_explodido[coluna_multivalor] != '']
    dim_df = pd.DataFrame(df_explodido[coluna_multivalor].unique(), columns=[nome_entidade])
    dim_df.reset_index(inplace=True)
    dim_df.rename(columns={'index': f'{nome_entidade}_id'}, inplace=True)
    pon_df = pd.merge(df_explodido, dim_df, left_on=coluna_multivalor, right_on=nome_entidade, how='left')
    pon_df = pon_df[[id_coluna, f'{nome_entidade}_id']]
    return dim_df, pon_df

def criar_tabela_fato_cncflora(df_limpo, dim_grupo, dim_categoria_risco, dim_especies):
    print("Montando a tabela fato...")
    fato_df = df_limpo.copy()
    fato_df = pd.merge(fato_df, dim_especies, left_on='nome_avaliado_entrada_sistema_cncflora', right_on='nome_cientifico', how='left')
    fato_df = pd.merge(fato_df, dim_grupo, left_on='grupo', right_on='grupo_nome', how='left')
    fato_df = pd.merge(fato_df, dim_categoria_risco, left_on='categoria_de_risco_de_extincao', right_on='categoria_risco', how='left')
    colunas_fato = [
        'avaliacao_id', 'especie_id', 'grupo_id', 'categoria_risco_id',
        'data_avaliacao', 'reavaliacao', 'historico_de_avaliacoes'
    ]
    colunas_existentes = [col for col in colunas_fato if col in fato_df.columns]
    return fato_df[colunas_existentes]

def criar_saida_otimizada_looker(fato_df, dimensoes_simples):
    print("Criando tabela fato 'gorda' para otimização...")
    fato_gorda = fato_df.copy()
    for nome_dim, df_dim in dimensoes_simples.items():
        chave_fk = df_dim.columns[0]
        fato_gorda = pd.merge(fato_gorda, df_dim, on=chave_fk, how='left')
    chaves_para_remover = [dim.columns[0] for dim in dimensoes_simples.values()]
    fato_gorda = fato_gorda.drop(columns=chaves_para_remover)
    return fato_gorda
    
def main():
    """Função principal que orquestra todo o processo para a CNCFlora."""
    caminho_script = Path(__file__).parent
    caminho_repo_raiz = caminho_script.parent
    caminho_dados_raw = caminho_repo_raiz / 'data' / 'raw' / 'cncflora'
    caminho_dados_processados = caminho_repo_raiz / 'data' / 'processed' / 'cncflora'
    caminho_dados_processados.mkdir(parents=True, exist_ok=True)
    
    df_lista_vermelha, df_termos = carregar_dados_cncflora(caminho_dados_raw)
    if df_lista_vermelha is None: return
    df_padronizado = padronizar_colunas_cncflora(df_lista_vermelha)
    df_cncflora_limpo = limpar_dados_base_cncflora(df_padronizado)
    
    dim_acoes, pon_acoes = criar_dim_e_ponte(df_cncflora_limpo, 'avaliacao_id', 'classificacao_acoes_de_conservacao_iucn___acoes_de_conservacao', 'acao_conservacao')
    dim_ameacas, pon_ameacas = criar_dim_e_ponte(df_cncflora_limpo, 'avaliacao_id', 'classificacao_de_ameacas_sistema_iucn___ameacas_cadastradas', 'ameaca')
    
    dim_grupo = pd.DataFrame(df_cncflora_limpo['grupo'].unique(), columns=['grupo_nome']); dim_grupo.reset_index(inplace=True); dim_grupo.rename(columns={'index': 'grupo_id'}, inplace=True)
    dim_categoria_risco = pd.DataFrame(df_cncflora_limpo['categoria_de_risco_de_extincao'].unique(), columns=['categoria_risco']); dim_categoria_risco.reset_index(inplace=True); dim_categoria_risco.rename(columns={'index': 'categoria_risco_id'}, inplace=True)
    dim_especies_temp = df_termos.rename(columns={'nome_cientifico_completo': 'nome_cientifico'}); dim_especies_temp.reset_index(inplace=True); dim_especies_temp.rename(columns={'index': 'especie_id'}, inplace=True)
    
    fato_avaliacoes = criar_tabela_fato_cncflora(df_cncflora_limpo, dim_grupo, dim_categoria_risco, dim_especies_temp)

    dimensoes_simples_para_juntar = {"grupo": dim_grupo, "categoria_risco": dim_categoria_risco, "especie": dim_especies_temp}
    fato_gorda_cncflora = criar_saida_otimizada_looker(fato_avaliacoes, dimensoes_simples_para_juntar)

    # CORREÇÃO: Adicionando a tabela temporária de espécies de volta à lista de salvamento
    tabelas_para_salvar = {
        "fato_gorda_cncflora": fato_gorda_cncflora,
        "dim_acoes_conservacao": dim_acoes,
        "dim_ameacas": dim_ameacas,
        "pon_avaliacao_acao": pon_acoes,
        "pon_avaliacao_ameaca": pon_ameacas,
        "dim_especies_cncflora_temp": dim_especies_temp # <-- ESTA LINHA É A CORREÇÃO
    }
    
    print("\n--- Salvando arquivos otimizados para Looker Studio ---")
    for nome, df in tabelas_para_salvar.items():
        if df is not None:
            df.to_csv(caminho_dados_processados / f"{nome}.csv", index=False)
            print(f"Salvo: {nome}.csv")
    
    print(f"\nProcessamento da CNCFlora concluído. Arquivos salvos em: {caminho_dados_processados}")

if __name__ == "__main__":
    main()