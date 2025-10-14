# scripts/desenhar_pipeline.py
import graphviz
import os

# Garante que o output seja salvo na pasta raiz do projeto
output_directory = os.path.join(os.path.dirname(__file__), '..')
output_filename = os.path.join(output_directory, 'pipeline_arquitetura_refinada')

dot = graphviz.Digraph(
    'PipelineSISMM_Refinado',
    comment='Arquitetura Refinada e Sequencial do Pipeline de Dados',
    graph_attr={
        'rankdir': 'TB', 
        'splines': 'spline',
        'nodesep': '1.0', 
        'ranksep': '1.5',
        'fontname': 'Helvetica',
        'fontsize': '20',
        'label': 'Arquitetura Sequencial do Pipeline de Dados (ETL)',
        'labelloc': 't',
        'compound': 'true'
    }
)

# --- Estilos dos Nós ---
styles = {
    'fonte_dados': {'shape': 'box', 'style': 'filled,rounded', 'fillcolor': '#a7c7e7', 'fontsize': '18'},
    'orquestrador': {'shape': 'box', 'style': 'filled', 'fillcolor': '#DDA0DD', 'fontcolor': 'black', 'fontsize': '18'},
    'script': {'shape': 'box', 'style': 'filled,rounded', 'fillcolor': '#c1e1c1', 'fontsize': '18'},
    'camada_dados': {'shape': 'note', 'style': 'filled', 'fillcolor': '#fdfd96', 'fontsize': '18'},
    'output_final': {'shape': 'cylinder', 'style': 'filled', 'fillcolor': '#ffb347', 'fontsize': '18'}
}

# --- ETAPA 0: FONTES DE DADOS ---
with dot.subgraph(name='cluster_fontes') as c:
    c.attr(label='Fontes de Dados Brutos', style='rounded,dashed')
    c.node('M0', 'M0: Início do Pipeline', **styles['orquestrador'])
    c.node('scopus_csv', 'Scopus CSVs', **styles['fonte_dados'])
    c.node('espacenet_csv', 'Espacenet CSVs', **styles['fonte_dados'])
    c.node('cncflora_csv', 'CNCFlora CSV', **styles['fonte_dados'])
    c.node('ods_csv', 'ODS Manual.CSV', **styles['fonte_dados'])
    c.node('ipc_csv', 'IPC Green Env. CSVs', **styles['fonte_dados'])

# --- ETAPA 1: PROCESSAMENTO BASE ---
with dot.subgraph(name='cluster_etapa1') as c:
    c.attr(label='ETAPA 1: Processamento e Unificação Base', style='rounded')
    c.node('M1', 'main_\netapa1_base.py', **styles['orquestrador'])
    c.node('P_scopus', 'processar_scopus.py', **styles['script'])
    c.node('P_espacenet', 'processar_espacenet.py', **styles['script'])
    c.node('P_cncflora', 'processar_cncflora.py', **styles['script'])
    c.node('P_unificar', 'unificar_fontes.py', **styles['script']) # Script unificador adicionado
    c.node('D_mestre', 'dim_especies_mestre.csv', **styles['camada_dados']) # Único output relevante

# --- ETAPA 2: ENRIQUECIMENTO ---
with dot.subgraph(name='cluster_etapa2') as c:
    c.attr(label='ETAPA 2: Enriquecimento de Sustentabilidade', style='rounded')
    c.node('M2', 'main_\netapa2_sustentabilidade.py', **styles['orquestrador'])
    c.node('P_ods', 'processar_ods_manual.py', **styles['script'])
    c.node('P_ipc', 'processar_ipc_green.py', **styles['script'])
    c.node('D_artigos_ods', 'artigos_com_ods.csv', **styles['camada_dados']) # Output final da etapa

# --- ETAPA 3: ACHATAMENTO ---
with dot.subgraph(name='cluster_etapa3') as c:
    c.attr(label='ETAPA 3: Achatamento para BI', style='rounded')
    c.node('M3', 'main_\netapa3_achatamento.py', **styles['orquestrador'])
    c.node('D_achatados', 'Tabelas Finais Achatadas', **styles['camada_dados'])

# --- ETAPA 4: CARGA E APRESENTAÇÃO ---
with dot.subgraph(name='cluster_etapa4') as c:
    c.attr(label='ETAPA 4: Carga e Apresentação', style='rounded')
    c.node('M4', 'main_\netapa4_upload.py', **styles['orquestrador'])
    c.node('sheets', 'Google Sheets', **styles['output_final'])
    c.node('looker', 'Looker Studio', **styles['output_final'])

# --- CRIANDO SUBGRÁFICO PARA A LEGENDA ---
with dot.subgraph(name='cluster_legend') as c:
    c.attr(
        label='Legenda',
        style='dotted',
        rank='sink' # Tenta posicionar este cluster no final do ranking (embaixo)
    )
    # Criamos nós "invisíveis" (key*) que servirão de âncora para o alinhamento
    c.node('key_orq', label='', style='invis')
    c.node('key_scr', label='', style='invis')
    c.node('key_dat', label='', style='invis')
    c.node('key_fon', label='', style='invis')
    c.node('key_out', label='', style='invis')
    
    # Alinhamento vertical das chaves
    c.edge('key_orq', 'key_scr', style='invis')
    c.edge('key_scr', 'key_dat', style='invis')
    c.edge('key_dat', 'key_fon', style='invis')
    c.edge('key_fon', 'key_out', style='invis')

    # Criamos os itens da legenda
    with c.subgraph() as s:
        s.attr(rank='same') # Alinha a forma e o texto horizontalmente
        s.node('leg_orq', '', **styles['orquestrador'], fixedsize='true', width='0.5', height='0.5')
        s.node('leg_orq_txt', 'Orquestrador (main)', shape='plaintext')
    with c.subgraph() as s:
        s.attr(rank='same')
        s.node('leg_scr', '', **styles['script'], fixedsize='true', width='0.5', height='0.5')
        s.node('leg_scr_txt', 'Script de Processamento', shape='plaintext')
    with c.subgraph() as s:
        s.attr(rank='same')
        s.node('leg_dat', '', **styles['camada_dados'], fixedsize='true', width='0.5', height='0.5')
        s.node('leg_dat_txt', 'Artefato de Dados (.csv)', shape='plaintext')
    with c.subgraph() as s:
        s.attr(rank='same')
        s.node('leg_fon', '', **styles['fonte_dados'], fixedsize='true', width='0.5', height='0.5')
        s.node('leg_fon_txt', 'Fonte de Dados Brutos', shape='plaintext')
    with c.subgraph() as s:
        s.attr(rank='same')
        s.node('leg_out', '', **styles['output_final'], fixedsize='true', width='0.5', height='0.5')
        s.node('leg_out_txt', 'Saída / Apresentação', shape='plaintext')
    
    # Conectamos as âncoras aos itens para alinhamento
    c.edge('key_orq', 'leg_orq', style='invis')
    c.edge('key_scr', 'leg_scr', style='invis')
    c.edge('key_dat', 'leg_dat', style='invis')
    c.edge('key_fon', 'leg_fon', style='invis')
    c.edge('key_out', 'leg_out', style='invis')

# --- CONEXÕES DO FLUXO LÓGICO ---

# Conexões da Etapa 0
dot.edge('M0', 'scopus_csv', style='dashed')
dot.edge('M0', 'espacenet_csv', style='dashed')
dot.edge('M0', 'cncflora_csv', style='dashed')
dot.edge('M0', 'ods_csv', style='dashed')
dot.edge('M0', 'ipc_csv', style='dashed')

# Conexões dentro da Etapa 1
dot.edge('scopus_csv', 'P_scopus')
dot.edge('espacenet_csv', 'P_espacenet')
dot.edge('cncflora_csv', 'P_cncflora')
dot.edge('M1', 'P_scopus', style='dashed')
dot.edge('M1', 'P_espacenet', style='dashed')
dot.edge('M1', 'P_cncflora', style='dashed')
dot.edge('P_scopus', 'P_unificar')
dot.edge('P_espacenet', 'P_unificar')
dot.edge('P_cncflora', 'P_unificar')
dot.edge('P_unificar', 'D_mestre')

# Conexões dentro da Etapa 2
dot.edge('ods_csv', 'P_ods')
dot.edge('ipc_csv', 'P_ipc')
dot.edge('D_mestre', 'P_ods') # Mostrando que P_ods enriquece a base mestre
dot.edge('M2', 'P_ods', style='dashed')
dot.edge('M2', 'P_ipc', style='dashed')
dot.edge('P_ods', 'D_artigos_ods')

# Conexões da Etapa 3
dot.edge('D_mestre', 'M3')
dot.edge('D_artigos_ods', 'M3')
dot.edge('M3', 'D_achatados', label='gera')

# Conexões da Etapa 4
dot.edge('D_achatados', 'M4')
dot.edge('M4', 'sheets', label='envia para')
dot.edge('sheets', 'looker', label='alimenta')

# === TÉCNICA PARA FORÇAR O LAYOUT SEQUENCIAL ===
# Criamos arestas invisíveis entre os orquestradores de cada etapa
# para garantir que o cluster N+1 sempre apareça abaixo do cluster N.
dot.edge('M0', 'M1', style='invis')
dot.edge('M1', 'M2', style='invis')
dot.edge('M2', 'M3', style='invis')
dot.edge('M3', 'M4', style='invis')


# --- Renderização do Gráfico ---
try:
    dot.render(output_filename, format='png', view=False, cleanup=True)
    print(f"✅ Diagrama da arquitetura REFINADO gerado com sucesso em '{output_filename}.png'")
except Exception as e:
    print(f"❌ Erro ao gerar o diagrama. Verifique se o Graphviz está instalado no sistema.")
    print(f"   Erro original: {e}")