"""
Orquestrador da ETAPA 1: Processamento de Dados Base.

Este script executa o pipeline de ETL para as fontes de dados brutas,
gerando os arquivos limpos e processados que servirão de base para as
análises de sustentabilidade.
"""

import subprocess
import sys
from pathlib import Path

def run_script(script_path, args_list=None):
    # Executa um script Python com argumentos opcionais e verifica se houve erros
    if args_list is None:
        args_list = []
    print(f"\n--- Executando: {script_path.name} ---")
    python_executable = sys.executable
    command = [python_executable, str(script_path)] + args_list
    print(f"Comando: {' '.join(command)}")
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding='utf-8', errors='replace'
    )
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
    return_code = process.poll()
    if return_code != 0:
        print(f"\n✖ ERRO: O script {script_path.name} falhou com o código de saída {return_code}.")
        return False
    else:
        print(f"✔ Sucesso: {script_path.name} concluído. ---")
        return True

def main():
    """Orquestrador principal para a Etapa 1."""
    print("="*49)
    print("=== INICIANDO PIPELINE - ETAPA 1: DADOS BASE ===")
    print("="*49)
    
    scripts_dir = Path("scripts")
    
    # --- ORDEM DE EXECUÇÃO ---
    scripts_para_executar = [
        (scripts_dir / 'processar_scopus.py', []),
        (scripts_dir / 'processar_cncflora.py', []),
        (scripts_dir / 'unificar_fontes.py', []),      # <-- RODA PRIMEIRO PARA CRIAR O ARQUIVO MESTRE
        (scripts_dir / 'processar_espacenet.py', [])   # <-- RODA DEPOIS, POIS DEPENDE DO ARQUIVO MESTRE
    ]
    
    for script, args in scripts_para_executar:
        if not script.exists():
            print(f"\n✖ ERRO: O arquivo de script não foi encontrado em '{script}'.")
            print("Pipeline interrompido.")
            return

        if not run_script(script, args):
            print("\nPipeline da ETAPA 1 interrompido devido a um erro.")
            return

    print("\n" + "="*52)
    print("✔ PIPELINE - ETAPA 1: DADOS BASE CONCLUÍDO COM SUCESSO! ✔")
    print("="*52)

if __name__ == "__main__":
    main()