"""
Orquestrador da ETAPA 3: Achatamento (Denormalização) dos Dados.

Este script executa o processo de 'achatamento' de tabelas,
juntando as tabelas de dimensão e ponte em tabelas finais
otimizadas para o Looker Studio.
"""

import subprocess
import sys
from pathlib import Path

def run_script(script_path, args_list=None):
    """Executa um script Python e verifica se houve erros."""
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
    """Orquestrador principal para a Etapa 3."""
    print("="*58)
    print("=== INICIANDO PIPELINE - ETAPA 3: ACHATAMENTO DE TABELAS ===")
    print("="*58)

    
    scripts_dir = Path("scripts")
    
    # Esta etapa executa apenas o script de criação das tabelas finais
    # Ele não precisa de argumentos, pois os caminhos já estão definidos dentro dele.
    scripts_para_executar = [
        (scripts_dir / 'criar_tabelas_finais.py', []),
        (scripts_dir / 'criar_fato_cncflora.py', [])
    ]
    
    for script, args in scripts_para_executar:
        if not script.exists():
            print(f"\n✖ ERRO: O arquivo de script não foi encontrado em '{script}'.")
            return
        if not run_script(script, args):
            print("\nPipeline da ETAPA 3 interrompido devido a um erro.")
            return

    print("\n" + "="*61)
    print("✔ PIPELINE - ETAPA 3: ACHATAMENTO DE TABELAS CONCLUÍDO! ✔")
    print("="*61)

if __name__ == "__main__":
    main()