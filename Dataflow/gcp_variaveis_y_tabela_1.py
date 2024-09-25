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
    'template_location': 'gs://dados_tcc_facens_sp/template/variavel_dependente_dados_visitantes_tab_1_lucas' }

# Crie as opções de pipeline a partir do dicionário

pipeline_options = PipelineOptions.from_dictionary(pipeline_options_dict)

# Tabela de IDH e PIB

with beam.Pipeline(options=pipeline_options) as p1:
    products_1 = (
        p1

        | 'Importando os Dados Tabela 1' >> beam.io.ReadFromText(
            r'gs://dados_tcc_facens_sp/inputs/Base_dados_IBGE.xlsx - Worksheet.csv', 
            skip_header_lines=1)

        | 'Criando o Objeto Tabela 1' >> beam.Map(lambda line: line.split(','))

        | 'Selecionando as Colunas Tabela 1' >> beam.Map(
            lambda line: [line[0], line[8], line[12]])

        | 'Transformando em Maiúsculas' >> beam.Map(
            lambda line: [x.upper() if isinstance(x, str) else x for x in line])

        | 'Formatando Saída' >> beam.Map(lambda line: ','.join(map(str, line)))  # Formata a saída como uma string CSV

        | 'Salvando no Bucket Tabela 1' >> beam.io.WriteToText(
            r'gs://dados_tcc_facens_sp/output/variavel_dependente_visitantes_tabela_1.csv')
    )