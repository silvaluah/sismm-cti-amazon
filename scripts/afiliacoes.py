# -*- coding: utf-8 -*-

"""
Script para processar, padronizar e enriquecer dados de afiliações do Scopus.
O processo inclui:
1. Carregar dados de artigos, afiliações e um dicionário de mapeamento.
2. Limpar e normalizar os textos das afiliações.
3. Expandir registros que contêm múltiplas instituições (colaborações).
4. Extrair e separar a instituição principal do departamento.
5. Juntar os dados enriquecidos com a tabela de artigos (ponte) para criar a tabela fato final.
"""

# --- 1. IMPORTAÇÕES ---
import pandas as pd
import numpy as np
import re
import unicodedata
import csv
import sys
from pathlib import Path

# --- 2. FUNÇÕES AUXILIARES ---

def pre_processar_texto(texto: str) -> str:
    """Apenas remove acentos e converte para maiúsculas, mantendo a estrutura e pontuação."""
    if not isinstance(texto, str): return ''
    try:
        # Normaliza para decompor acentos e caracteres
        texto_sem_acentos = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    except (TypeError, AttributeError):
        texto_sem_acentos = texto # Fallback caso haja erro de encoding
    return re.sub(r'\s+', ' ', texto_sem_acentos.upper()).strip()

def normalizar_texto_chave(texto: str) -> str:
    """Cria uma chave de busca simplificada, SEM pontuação e em maiúsculas."""
    if not isinstance(texto, str): return ''
    try:
        texto_sem_acentos = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    except (TypeError, AttributeError):
        texto_sem_acentos = texto
    # Remove tudo que não for letra, número ou espaço
    texto_limpo = re.sub(r'[^A-Z0-9\s]', '', texto_sem_acentos.upper())
    return re.sub(r'\s+', ' ', texto_limpo).strip()

def extrair_instituicao_e_departamento(row: pd.Series, mapa_completo: dict) -> pd.Series:
    """
    Extrai a Instituição Principal e o Departamento.
    A Instituição é encontrada via dicionário, e o departamento é o que sobra.
    """
    texto_original = row['affiliation_full_text_modificado']
    chave_normalizada_texto = normalizar_texto_chave(texto_original)
    
    # Retorno padrão
    instituicao_padronizada = "NAO MAPEADO"
    departamento = texto_original
    dados_instituicao = {}

    chaves_ordenadas = sorted(mapa_completo.keys(), key=len, reverse=True)
    chave_encontrada = None

    for chave_mapa in chaves_ordenadas:
        if chave_mapa and chave_mapa in chave_normalizada_texto:
            chave_encontrada = chave_mapa
            break

    if chave_encontrada:
        dados_instituicao = mapa_completo[chave_encontrada]
        instituicao_padronizada = dados_instituicao.get('nome_padronizado_dic')

        partes = texto_original.split(',')
        primeira_parte = partes[0].strip()
        
        if normalizar_texto_chave(primeira_parte) == chave_encontrada:
            departamento = pd.NA
        else:
            departamento = primeira_parte
    
    return pd.Series([
        instituicao_padronizada,
        departamento,
        dados_instituicao.get('sigla_dic', pd.NA),
        dados_instituicao.get('pais_dic', pd.NA),
        dados_instituicao.get('estado_dic', pd.NA),
        dados_instituicao.get('cidade_dic', pd.NA)
    ])


# --- 3. FUNÇÃO PRINCIPAL DE EXECUÇÃO ---

def main():
    """
    Orquestra todo o fluxo de processamento de dados.
    """
    print("--- INICIANDO SCRIPT DE PROCESSAMENTO DE AFILIAÇÕES ---")

    # --- Configuração de Caminhos ---
    # Assume que o script está em uma pasta 'scripts' e os dados em 'data'
    # Ex: /projeto/scripts/processar_afiliacoes.py
    #     /projeto/data/raw/dicionario.csv
    raiz_projeto = Path(__file__).parent.parent
    processed_dir = raiz_projeto / "data/processed"
    raw_dir = raiz_projeto / "data/raw"
    output_dir = raiz_projeto / "looker_studio_output"

    # Caminhos dos arquivos de entrada
    ponte_afiliacoes_path = processed_dir / 'pon_artigo_afiliacoes_scopus.csv'
    dim_afiliacoes_scopus_path = processed_dir / 'dim_afiliacoes_scopus.csv'
    dicionario_afiliacoes_path = raw_dir / 'dicionario_afiliacoes.csv'

    # Caminho do arquivo de saída
    caminho_saida = output_dir / 'fato_afiliacoes_final.csv'

    # --- Carregamento dos Dados ---
    print("\n⏳ 1/6: Carregando dados...")
    try:
        df_ponte = pd.read_csv(ponte_afiliacoes_path)
        df_dim_afiliacoes_scopus = pd.read_csv(dim_afiliacoes_scopus_path)
        df_dicionario = pd.read_csv(dicionario_afiliacoes_path, header=None)
        df_dicionario.columns = ['nome_original_sujo', 'nome_padronizado_dic', 'sigla_dic', 'pais_dic', 'estado_dic', 'cidade_dic']
        print("✔ Arquivos carregados com sucesso.")
    except FileNotFoundError as e:
        print(f"✖ ERRO: Arquivo não encontrado: {e}. Verifique a estrutura de pastas.")
        sys.exit(1)

    # --- Preparação do Dicionário ---
    print("\n⏳ 2/6: Preparando dicionário de mapeamento...")
    df_dicionario['chave_normalizada'] = df_dicionario['nome_original_sujo'].apply(normalizar_texto_chave)
    df_dicionario_unique = df_dicionario.drop_duplicates(subset=['chave_normalizada'], keep='first')
    
    mapeamento_padronizado = df_dicionario_unique.set_index('chave_normalizada').to_dict('index')
    mapa_instituicoes_simples = df_dicionario_unique.set_index('chave_normalizada')['nome_padronizado_dic'].to_dict()
    
    print(f"✔ Dicionário com {len(mapeamento_padronizado)} regras únicas criado.")

    # --- Expansão para Colaborações ---
    print("\n⏳ 3/6: Expandindo afiliações para tratar colaborações...")
    df_dim_afiliacoes_scopus['texto_processado'] = df_dim_afiliacoes_scopus['affiliation_full_text'].apply(pre_processar_texto)
    
    novos_registros = []
    chaves_ordenadas = sorted(mapa_instituicoes_simples.keys(), key=len, reverse=True)

    for _, row in df_dim_afiliacoes_scopus.iterrows():
        texto_proc = row['texto_processado']
        if not texto_proc:
            novos_registros.append(row.to_dict())
            continue

        instituicoes_encontradas = []
        texto_temp = texto_proc
        for chave in chaves_ordenadas:
            if re.search(r'\b' + re.escape(chave) + r'\b', texto_temp):
                valor_limpo = mapa_instituicoes_simples[chave]
                if valor_limpo not in instituicoes_encontradas:
                    instituicoes_encontradas.append(valor_limpo)
                    texto_temp = texto_temp.replace(chave, '')

        if len(instituicoes_encontradas) > 1:
            for instituicao in instituicoes_encontradas:
                nova_linha = row.to_dict()
                nova_linha['affiliation_full_text_modificado'] = f"{instituicao}, (Colaboração de: {row['affiliation_full_text']})"
                nova_linha['instituicao_principal_detectada'] = instituicao
                novos_registros.append(nova_linha)
        else:
            nova_linha = row.to_dict()
            nova_linha['affiliation_full_text_modificado'] = row['affiliation_full_text']
            nova_linha['instituicao_principal_detectada'] = instituicoes_encontradas[0] if instituicoes_encontradas else None
            novos_registros.append(nova_linha)
    
    df_dim_afiliacoes_expandida = pd.DataFrame(novos_registros).drop_duplicates(subset=['affiliation_id', 'instituicao_principal_detectada'])
    print(f"✔ Expansão concluída. DataFrame agora tem {len(df_dim_afiliacoes_expandida)} linhas.")

    # --- Extração de Instituição e Departamento ---
    print("\n⏳ 4/6: Aplicando a extração de instituição e departamento...")
    resultados = df_dim_afiliacoes_expandida.apply(lambda row: extrair_instituicao_e_departamento(row, mapeamento_padronizado), axis=1)
    resultados.columns = ['instituicao_padronizada', 'departamento', 'sigla', 'pais', 'estado', 'cidade']
    df_dim_final = pd.concat([df_dim_afiliacoes_expandida.reset_index(drop=True), resultados], axis=1)
    print("✔ Extração concluída.")

    # --- Criação da Tabela Fato Final ---
    print("\n⏳ 5/6: Criando a Tabela Fato Final...")
    df_fato_afiliacoes_final = pd.merge(df_ponte, df_dim_final, on='affiliation_id', how='left')
    
    colunas_finais = ['article_id', 'affiliation_id', 'instituicao_padronizada', 'departamento', 'sigla', 'pais', 'estado', 'cidade']
    df_fato_afiliacoes_final = df_fato_afiliacoes_final[colunas_finais]
    print("✔ Tabela Fato Final criada.")

    # --- Salvando o Resultado ---
    print(f"\n⏳ 6/6: Salvando resultado em '{caminho_saida}'...")
    df_fato_afiliacoes_final.to_csv(
        caminho_saida, 
        index=False, 
        encoding='utf-8-sig', 
        quoting=csv.QUOTE_ALL
    )
    print("✔ Arquivo final salvo com sucesso.")

    print("\n--- SCRIPT CONCLUÍDO ---")

# --- 4. PONTO DE ENTRADA DO SCRIPT ---
if __name__ == "__main__":
    main()