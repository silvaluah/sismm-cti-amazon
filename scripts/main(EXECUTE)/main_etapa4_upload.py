"""
Orquestrador da ETAPA 4: Upload para o Google Sheets.
"""

import gspread
# A biblioteca 'gspread_dataframe' não é mais necessária com este método
import pandas as pd
from pathlib import Path
import sys
import time

# --- CONFIGURAÇÃO ---
SERVICE_ACCOUNT_FILE = Path('config/mestrado-cti-sustentabilidade-1d3654cc88e3.json')
GOOGLE_SHEET_NAME = 'INPUT_V2' 

# --------------------

def autenticar_google_sheets():
    """Autentica com o Google usando a conta de serviço e retorna o cliente."""
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        gc = gspread.service_account(filename=str(SERVICE_ACCOUNT_FILE), scopes=scopes)
        print("✔ Autenticação com o Google Sheets bem-sucedida.")
        return gc
    except FileNotFoundError:
        print(f"✖ ERRO: Arquivo de chave da conta de serviço não encontrado em '{SERVICE_ACCOUNT_FILE}'")
        sys.exit(1)
    except Exception as e:
        print(f"✖ ERRO durante a autenticação: {e}")
        sys.exit(1)

def upload_df_to_sheet(spreadsheet, df, sheet_name, retries=3, delay=15):
    """
    Faz o upload de um DataFrame para uma aba, com lógica de retry,
    usando o método 'update' e tratando valores NaN.
    """
    for attempt in range(retries):
        try:
            print(f"  - Verificando a aba '{sheet_name}'...")
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                print(f"  - Aba '{sheet_name}' não encontrada. Criando...")
                # Cria a aba já com o tamanho certo para evitar uma chamada de API extra
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=len(df)+1, cols=len(df.columns))
                time.sleep(2)

            print(f"  - Preparando e fazendo upload de {len(df)} linhas...")
            
            # --- CORREÇÃO IMPORTANTE 1: Tratar valores NaN ---
            # Substitui todos os valores NaN por uma string vazia antes de enviar
            df_limpo = df.fillna('')
            
            # --- CORREÇÃO IMPORTANTE 2: Ordem dos argumentos ---
            dados_para_enviar = [df_limpo.columns.values.tolist()] + df_limpo.values.tolist()
            
            worksheet.clear()
            # A ordem correta é: (range_name, values) ou (values)
            worksheet.update(dados_para_enviar, 'A1')
            
            print(f"✔ Upload para '{sheet_name}' concluído.")
            return True 
            
        except gspread.exceptions.APIError as e:
            # (O resto da sua lógica de erro de quota, que está ótima)
            if e.response.status_code == 429: 
                print(f"  - AVISO: Quota da API excedida (tentativa {attempt + 1}/{retries}).")
                if attempt < retries - 1:
                    wait_time = delay * (2 ** attempt)
                    print(f"     Aguardando {wait_time} segundos antes de tentar novamente...")
                    time.sleep(wait_time)
                else:
                    print(f"✖ ERRO FATAL: Falha no upload para '{sheet_name}' após {retries} tentativas.")
                    return False
            else:
                print(f"✖ ERRO DE API inesperado para a aba '{sheet_name}': {e}")
                return False
        except Exception as e:
            print(f"✖ ERRO desconhecido durante o upload para '{sheet_name}': {e}")
            return False

def main():
    print("="*55)
    print("=== INICIANDO PIPELINE - ETAPA 4: UPLOAD GOOGLE SHEETS ===")
    print("="*55)
    
    gc = autenticar_google_sheets()
    try:
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        print(f"✔ Planilha '{GOOGLE_SHEET_NAME}' aberta com sucesso.")
    except gspread.SpreadsheetNotFound:
        print(f"✖ ERRO CRÍTICO: A planilha '{GOOGLE_SHEET_NAME}' não foi encontrada na sua conta Google ou não foi compartilhada com o e-mail da conta de serviço.")
        sys.exit(1)

    looker_output_dir = Path("looker_studio_output")
    other_dir = Path ("data")
    
    arquivos_para_enviar = {
        'art_afiliacao': looker_output_dir / 'fato_afiliacoes_final.csv',
        'art_autor': looker_output_dir / 'looker_artigo_autor.csv',
        'art_especie': looker_output_dir / 'looker_artigo_especie.csv',
        'art_keywords': looker_output_dir / 'looker_artigo_keywords_unificadas.csv',
        'art_completos': looker_output_dir / 'looker_artigos_completo.csv',
        'art_fato': looker_output_dir / 'looker_fato_artigos.csv',
        'pat_fato': looker_output_dir / 'looker_fato_patentes.csv',
        'pat_country': looker_output_dir / 'looker_patente_country.csv',
        'pat_especie': looker_output_dir / 'looker_patente_especie.csv',
        'pat_partes': looker_output_dir / 'looker_patente_partes.csv',
        'pat_completo': looker_output_dir / 'looker_patentes_completo.csv',
        'fat_cnc': looker_output_dir / 'looker_fato_cncflora.csv',

        'art_ods': other_dir / "scopus_ods_v1.csv",
        'dim_pat_especie': other_dir / "processed/dim_especies_mestre.csv"

    }

    for nome_aba, caminho_arquivo in arquivos_para_enviar.items():
        print(f"\n--- Processando '{caminho_arquivo.name}' para a aba '{nome_aba}' ---")
        if not caminho_arquivo.exists():
            print(f"✖ AVISO: Arquivo não encontrado. Pulando.")
            continue
        
        df = pd.read_csv(caminho_arquivo)
        
        if not upload_df_to_sheet(spreadsheet, df, nome_aba):
            print("Pipeline interrompido devido a falha no upload.")
            break 
            
        time.sleep(2)
        
    print("\n--- SCRIPT CONCLUÍDO ---")

if __name__ == "__main__":
    main()