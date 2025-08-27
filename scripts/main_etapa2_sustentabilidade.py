# Orquestrador da ETAPA 2: Enriquecimento com Dados de Sustentabilidade.

import subprocess
import sys
from pathlib import Path

def run_script(script_path, args_list=None):
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
    print("="*60)
    print("=== INICIANDO PIPELINE - ETAPA 2: DADOS DE SUSTENTABILIDADE ===")
    print("="*60)
    
    scripts_dir = Path("scripts")
    data_raw_dir = Path("data/raw")
    data_processed_dir = Path("data/processed")
    
    scripts_para_executar = [
        (scripts_dir / 'processar_ods_manual.py', [
            '--artigos_input', str(data_processed_dir / 'scopus_dados_limpos_temp.csv'),
            '--ods_manual_input', str(data_raw_dir / 'scopus_ods_v1.csv'),
            '--output', str(data_processed_dir / 'artigos_com_ods.csv')
        ]),
        
        (scripts_dir / 'processar_ipc_green.py', [
            '--patentes_ipc_input', str(data_processed_dir / 'espacenet/dim_ipc.csv'),
            '--ipc_gi_dir', str(data_raw_dir / 'ipc'),
            '--output', str(data_processed_dir / 'espacenet/ipc_classificado_green.csv')
        ]),
    ]
    
    for script, args in scripts_para_executar:
        if not script.exists():
            print(f"\n✖ ERRO: O arquivo de script não foi encontrado em '{script}'.")
            print("Pipeline interrompido.")
            return

        if not run_script(script, args):
            print("\nPipeline da ETAPA 2 interrompido devido a um erro.")
            return

    print("\n" + "="*63)
    print("✔ PIPELINE - ETAPA 2: DADOS DE SUSTENTABILIDADE CONCLUÍDO! ✔")
    print("="*63)

if __name__ == "__main__":
    main()