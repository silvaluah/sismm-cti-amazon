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

## Fluxograma do Pipeline
```mermaid
graph TD
    ... (graph TD
    subgraph "ETAPA 0: Fontes de Dados Brutos"
        direction LR
        A1["<font size=5>📄</font><br>Scopus CSVs"]
        A2["<font size=5>📄</font><br>Espacenet CSVs"]
        A3["<font size=5>📄</font><br>CNCFlora CSV"]
        A4["<font size=5>📄</font><br>ODS Manual CSV"]
        A5["<font size=5>📄</font><br>IPC Green Inv. CSVs"]
    end

    subgraph "ETAPA 1: Processamento de Dados Base (main_etapa1_base.py)"
        B1["<font size=5>🐍</font><br>processar_scopus.py"]
        B2["<font size=5>🐍</font><br>processar_espacenet.py"]
        B3["<font size=5>🐍</font><br>processar_cncflora.py"]
        B4["<font size=5>🐍</font><br>unificar_fontes.py"]
    end

    subgraph "Camada de Dados Processados (data/processed)"
        C1["<font size=5>💾</font><br>scopus_limpos_temp.csv"]
        C2["<font size=5>💾</font><br>dim_ipc.csv, etc."]
        C3["<font size=5>💾</font><br>dim_especies_cncflora.csv"]
        C4["<font size=5>💾</font><br>dim_especies_mestre.csv"]
    end

    subgraph "ETAPA 2: Enriquecimento com Sustentabilidade (main_etapa2_sustentabilidade.py)"
        D1["<font size=5>🐍</font><br>processar_ods_manual.py"]
        D2["<font size=5>🐍</font><br>processar_ipc_green.py"]
    end

    subgraph "Camada de Dados Enriquecidos"
        E1["<font size=5>💾</font><br>artigos_com_ods.csv"]
        E2["<font size=5>💾</font><br>ipc_classificado_green.csv"]
    end
    
    subgraph "ETAPA 3: Upload para Apresentação (main_etapa3_google_sheets.py)"
        F1["<font size=5>🐍</font><br>main_etapa3_google_sheets.py"]
    end

    subgraph "Camada de Apresentação"
        G1["<font size=5>📈</font><br>Google Sheets"]
        G2["<font size=5>🎨</font><br>Looker Studio"]
    end

    %% Conexões do Pipeline
    A1 --> B1
    A2 --> B2
    A3 --> B3
    
    B1 --> C1
    B2 --> C2
    B3 --> C3
    
    C1 & C3 --> B4
    B4 --> C4

    C1 & A4 --> D1
    C2 & A5 --> D2
    
    D1 --> E1
    D2 --> E2

    E1 & E2 --> F1
    F1 --> G1
    G1 --> G2
    
    %% Estilos para deixar mais bonito
    style A1 fill:#cce5ff,stroke:#333,stroke-width:2px
    style A2 fill:#cce5ff,stroke:#333,stroke-width:2px
    style A3 fill:#cce5ff,stroke:#333,stroke-width:2px
    style A4 fill:#cce5ff,stroke:#333,stroke-width:2px
    style A5 fill:#cce5ff,stroke:#333,stroke-width:2px
    
    style B1 fill:#ffe5cc,stroke:#333,stroke-width:2px
    style B2 fill:#ffe5cc,stroke:#333,stroke-width:2px
    style B3 fill:#ffe5cc,stroke:#333,stroke-width:2px
    style B4 fill:#ffe5cc,stroke:#333,stroke-width:2px
    
    style C1 fill:#d4edda,stroke:#333,stroke-width:2px
    style C2 fill:#d4edda,stroke:#333,stroke-width:2px
    style C3 fill:#d4edda,stroke:#333,stroke-width:2px
    style C4 fill:#d4edda,stroke:#333,stroke-width:2px
    
    style D1 fill:#ffe5cc,stroke:#333,stroke-width:2px
    style D2 fill:#ffe5cc,stroke:#333,stroke-width:2px

    style E1 fill:#d4edda,stroke:#333,stroke-width:2px
    style E2 fill:#d4edda,stroke:#333,stroke-width:2px
    
    style F1 fill:#ffe5cc,stroke:#333,stroke-width:2px
    
    style G1 fill:#fff0b3,stroke:#333,stroke-width:2px
    style G2 fill:#f8d7da,stroke:#333,stroke-width:2px) ...
```