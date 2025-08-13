# Observatório de CT&I sobre Plantas Endêmicas da Amazônia

Este repositório contém o código-fonte e a metodologia para o pipeline de dados desenvolvido como parte da dissertação [SEU TÍTULO AQUI]. O objetivo deste projeto é automatizar a coleta, limpeza, modelagem e integração de dados de múltiplas fontes para a criação de um painel de indicadores de Ciência, Tecnologia e Inovação (CT&I) sobre a flora endêmica da Amazônia.

Este trabalho representa a **Fase 1** do projeto, focada na construção da infraestrutura de dados.

---

## 🏛️ Arquitetura do Pipeline

O projeto implementa um pipeline de Extração, Transformação e Carga (ETL) que processa dados brutos e os estrutura em um **Modelo Estrela Híbrido**, otimizado para análise em ferramentas de Business Intelligence (BI) como o Looker Studio.

O pipeline é composto por:
* **Scripts de Processamento por Fonte:** Módulos dedicados para `Scopus`, `CNCFlora` e `Espacenet`.
* **Script de Unificação:** Responsável por criar dimensões conformes (mestras), como a `dim_especies_mestre`, para conectar as diferentes fontes.
* **Script Orquestrador (`main.py`):** Gerencia a execução de todo o pipeline em um único comando, respeitando as dependências de dados.

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python 3.10+
* **Bibliotecas Principais:** Pandas
* **Ambiente:** Visual Studio Code (Codespaces)
* **Controle de Versão:** Git & GitHub

## 🚀 Guia de Instalação e Execução

Siga os passos abaixo para replicar o ambiente e executar o pipeline de dados.

### 1. Configuração do Ambiente

1.  **Clonar o Repositório:**
    ```bash
    git clone <URL_DO_SEU_REPOSITORIO_GITHUB>
    cd <NOME_DA_PASTA_DO_PROJETO>
    ```

2.  **Criar e Ativar o Ambiente Virtual:**
    ```bash
    # Criar o ambiente
    python3 -m venv .venv
    
    # Ativar no macOS/Linux
    source .venv/bin/activate
    
    # Ativar no Windows
    # .venv\Scripts\activate
    ```

3.  **Instalar as Dependências:**
    Certifique-se de que o arquivo `requirements.txt` exista com o conteúdo abaixo e execute o comando de instalação.

    *Conteúdo do `requirements.txt`:*
    ```
    pandas
    ```

    *Comando de Instalação:*
    ```bash
    pip install -r requirements.txt
    ```

### 2. Organização dos Dados Brutos

Antes da execução, os arquivos de dados brutos devem ser posicionados na seguinte estrutura dentro de `data/raw/`:

data/raw/
├── cncflora/
│   ├── lista_vermelha_cnc_flora.csv
│   └── termos_plantas.txt
├── espacenet_input/
│   ├── espacenet_angiosperma.csv
│   └── espacenet_samambaias_e_licofitas.csv
├── scopus_input/
│   ├── scopus_angiospermas.csv
│   └── ... (outros arquivos da Scopus)
└── espacenet_resumo_plantas.csv

### 3. Execução do Pipeline Completo

Com o ambiente configurado e os dados no lugar, execute o pipeline com um único comando a partir da **pasta raiz** do projeto:

```bash
python main.py

O script irá orquestrar todos os passos, exibindo o progresso no terminal. Ao final, todos os arquivos processados e modelados estarão na pasta data/processed/.

⚙️ Descrição dos Componentes do Pipeline
scripts/processar_scopus.py: Limpa, transforma e modela os dados da Scopus.

scripts/processar_cncflora.py: Processa os dados da CNCFlora.

scripts/processar_espacenet.py: Processa os dados de patentes da Espacenet.

scripts/unificar_fontes.py: Integra os outputs dos scripts de processamento, criando as dimensões mestras.

main.py: Orquestrador principal que executa todos os outros scripts na ordem correta.

🗺️ Roadmap de Trabalhos Futuros
Fase 2: Inclusão de novas fontes de dados sobre sustentabilidade e unificação com o modelo atual.

Fase 3: Conexão do Data Warehouse final com o Looker Studio, desenvolvimento dos indicadores visuais e avaliação geral do processo.
