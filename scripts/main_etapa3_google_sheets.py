"""
Orquestrador da ETAPA 3: Upload para o Google Sheets.

Este script lê os arquivos CSV processados e os envia para uma
Planilha Google específica, criando ou substituindo as abas conforme necessário.
"""

import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
from pathlib import Path
import sys
import time

# --- CONFIGURAÇÃO ---
# Caminho para a chave de autenticação (o arquivo JSON)
SERVICE_ACCOUNT_FILE = Path('/workspaces/sismm-cti-amazon/config/mestrado-cti-sustentabilidade-dc130dbe0471.json') # <-- MUDE AQUI

# Nome da Planilha Google (deve ser exato)
GOOGLE_SHEET_NAME = 'Observatório de Indicadores' #

# --------------------

def autenticar_google_sheets():
    """Autentica com o Google usando a conta de serviço e retorna o cliente."""
    try:
        gc = gspread.service_account(filename=str(SERVICE_ACCOUNT_FILE))
        print("✔ Autenticação com o Google Sheets bem-sucedida.")
        return gc
    except FileNotFoundError:
        print(f"✖ ERRO: Arquivo de chave da conta de serviço não encontrado em '{SERVICE_ACCOUNT_FILE}'")
        print("   - Verifique se o arquivo JSON está na pasta 'config/' e o nome está correto no script.")
        sys.exit(1)
    except Exception as e:
        print(f"✖ ERRO durante a autenticação: {e}")
        sys.exit(1)

def upload_df_to_sheet(spreadsheet, df, sheet_name, retries=3, delay=10):
    """
    Faz o upload de um DataFrame para uma aba específica.
    Inclui lógica de retry com exponential backoff para erros de quota.
    """
    for attempt in range(retries):
        try:
            print(f"  - Verificando a aba '{sheet_name}'...")
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                print(f"  - Aba '{sheet_name}' não encontrada. Criando...")
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=1)
                time.sleep(2) # Pausa extra após criar uma nova aba

            print(f"  - Limpando a aba e fazendo upload de {len(df)} linhas...")
            worksheet.clear()
            set_with_dataframe(worksheet, df, resize=True) # resize=True é mais robusto
            print(f"✔ Upload para '{sheet_name}' concluído.")
            return # Sai da função se o upload for bem-sucedido
            
        except gspread.exceptions.APIError as e:
            # Verifica se o erro é de quota (código 429)
            if e.response.status_code == 429:
                print(f"  - AVISO: Quota da API excedida na tentativa {attempt + 1}/{retries}.")
                if attempt < retries - 1:
                    wait_time = delay * (2 ** attempt) # Aumenta o tempo de espera a cada tentativa
                    print(f"     Aguardando {wait_time} segundos antes de tentar novamente...")
                    time.sleep(wait_time)
                else:
                    print(f"✖ ERRO FATAL: Falha ao fazer upload para '{sheet_name}' após {retries} tentativas.")
                    raise e # Lança o erro novamente se todas as tentativas falharem
            else:
                raise e # Lança outros erros da API imediatamente

def main():
    """Função principal que orquestra o upload dos dados."""
    print("="*55)
    print("=== INICIANDO PIPELINE - ETAPA 3: UPLOAD GOOGLE SHEETS ===")
    print("="*55)
    
    gc = autenticar_google_sheets()
    try:
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        print(f"✖ ERRO: A planilha '{GOOGLE_SHEET_NAME}' não foi encontrada.")
        sys.exit(1)

    data_processed_dir = Path("data/processed")

    # Define quais arquivos CSV processados devem ser enviados e para qual aba
    data_processed_dir = Path("data/processed")
    arquivos_para_enviar = {
        # Nome da Aba : Caminho do Arquivo CSV

        # grupo 1: dados provenientes da scopus
        'S_geral': data_processed_dir / 'scopus_dados_limpos_temp.csv',
        'S_ODS': data_processed_dir / 'artigos_com_ods.csv',
        'S_dim_autores': data_processed_dir / 'dim_autores_scopus.csv',
        'S_pon_autores': data_processed_dir / 'pon_artigo_autores_scopus.csv',
        'S_dim_afiliacoes': data_processed_dir / 'dim_afiliacoes_scopus.csv',
        'S_pon_afiliacoes': data_processed_dir / 'pon_artigo_afiliacoes_scopus.csv',
        'S_dim_keywords': data_processed_dir / 'dim_keywords_scopus.csv',
        'S_pon_keywords': data_processed_dir / 'pon_artigo_keyword_scopus.csv',
        'S_dim_index_keywords': data_processed_dir / 'dim_index_keywords_scopus.csv',
        'S_pon_index_keywords': data_processed_dir / 'pon_artigo_index_keywords_scopus.csv',

        # grupo 2: dados provenientes da espacenet
        'E_fato_patentes': data_processed_dir / 'espacenet/fato_patentes_espacenet.csv',
        'E_dim_country': data_processed_dir / 'espacenet/dim_country.csv',
        'E_pon_country': data_processed_dir / 'espacenet/pon_patente_country.csv',
        'E_dim_partes': data_processed_dir / 'espacenet/dim_parties.csv',
        'E_pon_partes': data_processed_dir / 'espacenet/pon_patente_party.csv',
        'E_IPC_green': data_processed_dir / 'espacenet/ipc_classificado_green.csv',
        'E_pon_ano_prim_publi': data_processed_dir / 'espacenet/pon_patente_ano_primeira_publicacao.csv',
        'E_pon_ano_priori': data_processed_dir / 'espacenet/pon_patente_ano_prioridade.csv',
        'E_pon_ano_public': data_processed_dir / 'espacenet/pon_patente_ano_publicacao.csv',
        'E_pon_especie': data_processed_dir / 'espacenet/pon_patente_especie.csv',
        'E_pon_ipc': data_processed_dir / 'espacenet/pon_patente_ipc.csv',

        # grupo 3: dados provenientes do CNCFlora
        'C_fato_especies': data_processed_dir / 'cncflora/fato_gorda_cncflora.csv',
        'C_dim_especies': data_processed_dir / 'dim_especies_mestre.csv',
        'C_fato_amenaca': data_processed_dir / 'cncflora/dim_ameacas.csv',
        'C_pon_amenaca': data_processed_dir / 'cncflora/pon_avaliacao_ameaca.csv',
        'C_dim_acao': data_processed_dir / 'cncflora/dim_acoes_conservacao.csv',
        'C_pon_acao': data_processed_dir / 'cncflora/pon_avaliacao_acao.csv',

        # grupo 4: dados provenientes da WIPO
    }

    # Itera sobre o dicionário e faz o upload de cada arquivo
    for nome_aba, caminho_arquivo in arquivos_para_enviar.items():
        if not caminho_arquivo.exists():
            print(f"✖ AVISO: Arquivo de entrada '{caminho_arquivo}' não encontrado. Pulando upload.")
            continue
        
        print(f"\n⏳ Processando upload para '{nome_aba}'...")
        try:
            df_to_upload = pd.read_csv(caminho_arquivo)
            upload_df_to_sheet(spreadsheet, df_to_upload, nome_aba)
            
            # --- CORREÇÃO DA QUOTA DA API ---
            # Adiciona uma pausa de 2 segundos entre cada upload para não exceder o limite
            print("  - Pausando por 2 segundos para evitar excesso de quota...")
            time.sleep(2)
            
        except Exception as e:
            print(f"✖ ERRO durante o upload para a aba '{nome_aba}': {e}")
            # Continua para o próximo arquivo mesmo se um falhar
            continue

    print("\n" + "="*58)
    print("✔ PIPELINE - ETAPA 3: UPLOAD GOOGLE SHEETS CONCLUÍDO! ✔")
    print("="*58)

if __name__ == "__main__":
    main()