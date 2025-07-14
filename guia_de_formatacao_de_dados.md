
# Padrão base
- arquivos no formato txt não terão cabeçalhos, mas deverão adotar padrões de colunas sugeridas abaixo
- arquivos no formato csv terão cabeçalhos e todas colunas devem estar na mesma ordem/sequência sugerida abaixo

# *Título do arquivo.formato* 
*colunas ou cabeçalhos*

## termos_plantas.txt
- nome científico da espécie em latim,grupo taxonômico
-                Ex.:  Justicia birae,Angiospermas
                      Justicia carajensis,Angiospermas
                      Justicia cowanii,Angiospermas

## espacenet_samambaias_e_licofitas.csv OU espacenet_angiosperma_csv
- No
- Title
- Inventors
- Applicants
- Publication number
- Earliest
- priority
- IPC
- CPC
- Publication date
- Earliest publication
- Family number
-       Ex.: 1	MARINE BIOMASS REACTOR AND METHODS RELATED THERETO	GRIECO WILLIAM JOSEPH [US]	SOUTHERN RES INST [US]	US2018206423A1	2017-01-24	A01D44/00 
              A01G31/02 
              A01G33/00	A01D44/00 (US) 
              A01G31/02 (US) 
              A01G33/00 (EP,US) 
              C12M21/02 (EP,US) 
              C12M23/06 (EP,US) 
              C12M23/26 (EP,US)	2018-07-26	2018-07-26	61189536
  
## espacenet_resumo_plantas.csv
- Publication_number
- Abstract
- planta_levantada_manualmente
-     Ex. US10781458B2	Compositions and methods for controlling pests are provided. The methods involve transforming organisms with a nucleic acid sequence encoding an insecticidal protein. In particular, the nucleic acid sequences are useful for preparing plants and microorganisms that possess insecticidal activity. Thus, transformed bacteria, plants, plant cells, plant tissues and seeds are provided. Compositions are insecticidal nucleic acids and proteins of bacterial species. The sequences find use in the construction of expression vectors for subsequent transformation into organisms of interest including plants, as probes for the isolation of other homologous (or partially homologous) genes. The pesticidal proteins find use in controlling, inhibiting growth or killing Lepidopteran, Coleopteran, Dipteran, fungal, Hemipteran and nematode pest populations and for producing compositions with insecticidal activity	Cyathea tortuosa
      US2018206423A1	Disclosed herein is a marine biomass reactor and methods related thereto that are capable of growing large quantities of a marine biomass material, such as, for example, macroalgae and/or aquatic macrophyte, at low cost	Salvinia sprucei

## EN_ipc_section_H_title_list_2025_01_01.txt (A até H)
- codico IPC descrição
-     Ex.: H	ELECTRICITY
      H01	ELECTRIC ELEMENTS
      H01B	CABLES; CONDUCTORS; INSULATORS; SELECTION OF MATERIALS FOR THEIR CONDUCTIVE, INSULATING OR DIELECTRIC PROPERTIES (selection for magnetic properties H01F0001000000;waveguides H01P)
      H01B0001000000	Conductors or conductive bodies characterised by the conductive materials; Selection of materials as conductors (superconductive or hyperconductive conductors, cables or transmission lines characterised by the materials

## ipc_green_inventory.csv
- Nome
- Tem filhos?
- Pai 1
- Pai 2
- Pai 3
- Pai 4
- IPC
- PatentScope
- Espacenet
-       Ex.: ALTERNATIVE ENERGY PRODUCTION	Sim							
             BIO-FUELS	Sim	ALTERNATIVE ENERGY PRODUCTION						
             SOLID FUELS	Sim	BIO-FUELS	ALTERNATIVE ENERGY PRODUCTION			C10L 5/00	C10L 5/00	C10L5/00
  
