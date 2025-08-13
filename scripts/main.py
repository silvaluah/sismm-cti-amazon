import subprocess
import sys
from pathlib import Path

def run_script(script_path):
    """Executa um script Python e verifica se houve erros."""
    print(f"\n--- Executando: {script_path.name} ---")
    
    python_executable = sys.executable
    
    process = subprocess.Popen(
        [python_executable, str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace'
    )

    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())

    return_code = process.poll()

    if return_code != 0:
        print(f"\nERRO: O script {script_path.name} falhou com o código de saída {return_code}.")
        return False
    else:
        print(f"--- Sucesso: {script_path.name} concluído. ---")
        return True

def main():
    """
    Orquestrador principal que executa todo o pipeline de ETL de dados.
    """
    print("=================================================")
    print("=== INICIANDO O PIPELINE DE DADOS DO OBSERVATÓRIO ===")
    print("=================================================")
    
    scripts_dir = Path("scripts")
    
    # --- ORDEM DE EXECUÇÃO CORRIGIDA ---
    scripts_para_executar = [
        scripts_dir / 'processar_scopus.py',
        scripts_dir / 'processar_cncflora.py',
        scripts_dir / 'unificar_fontes.py',       # <-- Roda em 3º para criar a dim_especies_mestre
        scripts_dir / 'processar_espacenet.py'    # <-- Roda por último, pois depende da unificação
    ]
    # ------------------------------------
    
    for script in scripts_para_executar:
        if not script.exists():
            print(f"\nERRO: O arquivo de script não foi encontrado em '{script}'. Verifique a estrutura de pastas.")
            print("Pipeline interrompido.")
            return

        if not run_script(script):
            print("\nPipeline interrompido devido a um erro.")
            return

    print("\n================================================")
    print("=== PIPELINE DE DADOS CONCLUÍDO COM SUCESSO! ===")
    print("================================================")
    print("Todos os arquivos processados estão na pasta 'data/processed/'.")

if __name__ == "__main__":
    main()