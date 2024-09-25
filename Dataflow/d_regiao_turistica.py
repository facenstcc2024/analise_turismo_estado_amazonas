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
    'template_location': 'gs://dados_tcc_facens_sp/template/tab_d_regiao_turistica_lucas' }

# Crie as opções de pipeline a partir do dicionário

pipeline_options = PipelineOptions.from_dictionary(pipeline_options_dict)

# Tabela de d_Populacao

with beam.Pipeline(options=pipeline_options) as p1:
    products_1 = (
        p1

        | 'Importando os Dados Tabela 1' >> beam.io.ReadFromText(
            r'gs://dados_tcc_facens_sp/inputs/municipios_categorias_turisticas_2019.csv', 
            skip_header_lines=1)

        | 'Criando o Objeto Tabela 1' >> beam.Map(lambda line: line.split(','))

        | 'Filtrar apenas UF para AM' >> beam.Filter(
            lambda line: line[1] == 'AM')

        | 'Selecionando e Limpando as Colunas' >> beam.Map(
            lambda line: [
                line[3].strip(), 
                line[2].strip(), 
                line[10].strip().replace('"', '')  # Remove aspas da última coluna
            ])

        # Convertendo a última coluna para inteiro
        | 'Convertendo Última Coluna para Inteiro' >> beam.Map(
            lambda line: [
                line[0], 
                line[1], 
                int(line[2]) if line[2].isdigit() else line[2]  # Converte para inteiro se for um número
            ])

        # Transformando em maiúsculas
        | 'Transformando em Maiúsculas' >> beam.Map(
            lambda line: [x.upper() if isinstance(x, str) else x for x in line])

        # Formatando a saída como uma string CSV
        | 'Formatando Saída' >> beam.Map(lambda line: ','.join(map(str, line)))

        | 'Salvando no Bucket Tabela 1' >> beam.io.WriteToText(
            r'gs://dados_tcc_facens_sp/output/tabela_d_regiao_turistica.csv')
    )