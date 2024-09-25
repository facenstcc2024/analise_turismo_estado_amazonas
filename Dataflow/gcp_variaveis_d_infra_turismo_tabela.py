import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os

sa = r'D:\FACENS\tcc_facens\teste_15_08\security\service-account.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa

pipeline_options_dict = {
    'project': 'tccfacens2024' ,
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://dados_tcc_facens_sp/tmp',
    'temp_location': 'gs://dados_tcc_facens_sp/tmp',
    'template_location': 'gs://dados_tcc_facens_sp/template/d_Infra_Turismo_lucas' }

# Crie as opções de pipeline a partir do dicionário

pipeline_options = PipelineOptions.from_dictionary(pipeline_options_dict)

# Tabela de Mapa de Turismo

with beam.Pipeline(options=pipeline_options) as p1:
    products_1 = (
        p1

        | 'Importando os Dados Tabela 1' >> beam.io.ReadFromText(
            r'gs://dados_tcc_facens_sp/inputs/MAPA - Relatorio Atividades Turisticas.xlsx - atividade_turistica.csv', 
            skip_header_lines=4)

        | 'Criando o Objeto Tabela d_Infra_Turismo_lucas' >> beam.Map(lambda line: line.split(','))

        | 'Selecionando as Colunas' >> beam.Map(
            lambda line: [line[1], line[7], line[9], line[11], line[16], line[18], line[20], line[26], line[27], line[42], line[49], line[51], line[52], line[54], line[55], line[57], line[66], line[67], line[68], line[71], line[79], line[80], line[82], line[107], line[112], line[114]])

        | 'Segmentando Patrimonio Cultural' >> beam.Map(
            lambda line: line[:22] + [  
                line[22],  
                1 if 'Historico' in line[22] else 0,        
                1 if 'Equipamentos' in line[22] else 0,     
                1 if 'Outros' in line[22] else 0            
            ] + line[23:]
        )

        | 'Segmentando Áreas Naturais' >> beam.Map(
            lambda line: line[:-9] + [  
                line[-9],  
                1 if 'Parques Naturais' in line[-9] else 0,    
                1 if 'Unidade de Conservacao' in line[-9] else 0,  
                1 if 'Reservas ecologicas' in line[-9] else 0,  
                1 if 'Outros' in line[-9] else 0              
            ] + line[-8:]
        )

        | 'Segmentando Tipos de Transporte' >> beam.Map(
            lambda line: line[:-21] + [  
                line[-21],  
                1 if 'Hidrovia' in line[-21] else 0,    
                1 if 'Rodovia' in line[-21] else 0,     
                1 if 'Aeroporto' in line[-21] else 0,   
                1 if 'Ferrovia' in line[-21] else 0,    
                1 if 'Outros' in line[-21] else 0       
            ] + line[-20:]
        )

        | 'Segmentando Tipos de Transporte de Passageiros' >> beam.Map(
            lambda line: line[:-27] + [  
                line[-27],  
                1 if 'Transporte rodoviario de passageiros' in line[-27] else 0,   
                1 if 'Transporte aquaviario' in line[-27] else 0,                 
                1 if 'Transporte aereo de passageiros' in line[-27] else 0,       
                1 if 'Outros' in line[-27] else 0                                
            ] + line[-26:]
        )

        | 'Segmentando Atividades Economicas' >> beam.Map(
            lambda line: line[:2] + [  
                line[2],  
                1 if 'Agricultura e Pecuaria' in line[2] else 0,  
                1 if 'Turismo' in line[2] else 0,                 
                1 if 'Outros' in line[2] else 0,                  
                1 if 'Servicos' in line[2] else 0,                
                1 if 'Comercio' in line[2] else 0,                
                1 if 'Industria de base' in line[2] else 0        
            ] + line[3:]
        )

        | 'Transformando em Maiúsculas' >> beam.Map(
            lambda line: [x.upper() if isinstance(x, str) else x for x in line]
        )

        | 'Formatando Saída' >> beam.Map(lambda line: ','.join(map(str, line)))  # Formata a saída como uma string CSV

        | 'Salvando no Bucket Tabela d_Infra_Turismo_lucas' >> beam.io.WriteToText(
            r'gs://dados_tcc_facens_sp/output/d_infra_turismo_lucas', file_name_suffix='.csv')
    )