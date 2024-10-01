import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

sa = r'C:\Users\nayan\OneDrive\Área de Trabalho\TCC\apache-beam-labs\CHAVE_GCP.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa

pipeline_options = {
    'project': 'tccfacens2024' ,
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://dados_tcc_facens_sp/tmp',
    'temp_location': 'gs://dados_tcc_facens_sp/tmp',
    'template_location': 'gs://dados_tcc_facens_sp/template/dataflow-leitos-pipeline-nayane' }
    
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
l = beam.Pipeline(options=pipeline_options)

leitos = (
    l
        | 'Importando os Dados' >> beam.io.ReadFromText(f'gs://dados_tcc_facens_sp/inputs/Leitos_2022.csv',skip_header_lines = 1)
 
        | 'Criando o Objeto' >> beam.Map(lambda line: line.replace('""', '').split(','))

        | 'Selecionando UF' >> beam.Filter(lambda line: line[2] == 'AM')

        | 'Criar Pares Chave-Valor (municipio,quantidade)' >> beam.Map(lambda line: (line[3], int(line[20])))

        | 'Agrupar por Município' >> beam.GroupByKey()
            
        | 'Somar Quantidades' >> beam.Map(lambda kv: (kv[0], sum(kv[1])))

        | 'Formatar para Template' >> beam.Map(lambda kv: f"{kv[0]},{kv[1]}")

        | 'Salvando no Bucket' >> beam.io.WriteToText(f'gs://dados_tcc_facens_sp/output/d_leitos.csv')
)

l.run()