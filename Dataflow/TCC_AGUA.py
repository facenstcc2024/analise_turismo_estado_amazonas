import apache_beam as beam
import os
import unicodedata
from apache_beam.options.pipeline_options import PipelineOptions
import re


# Configuração da chave de autenticação do Google Cloud
sa = r'C:\Users\nayan\OneDrive\Área de Trabalho\TCC\apache-beam-labs\CHAVE_GCP.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa

pipeline_options = {
    'project': 'tccfacens2024',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://dados_tcc_facens_sp/tmp',
    'temp_location': 'gs://dados_tcc_facens_sp/tmp',
    'template_location': 'gs://dados_tcc_facens_sp/template/dataflow-agua-pipeline-nayane',
    'save_main_session':True,
}

def remove_special_characters(text):
    """Remove acentos e caracteres especiais do texto."""
    text = unicodedata.normalize('NFD', text.upper())
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    text = re.sub(r'[^A-Z0-9\s]', '', text)  # Remove caracteres não alfanuméricos e não espaciais
    return text

def process_line(line):
    """Processa uma linha do CSV, dividindo e transformando os dados."""
    # Divide a linha em colunas usando ponto e vírgula
    columns = line.split(';')
    # Ignorar linhas vazias
    if len(columns) < 3 or not any(columns):
        return None
    # Transformar a coluna O_MUNICIPIO (assumido como coluna 0)
    columns[0] = remove_special_characters(columns[0])
    return columns

def filter_empty_lines(element):
    """Filtra linhas vazias do PCollection."""
    return element is not None

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

with beam.Pipeline(options=pipeline_options) as p:
    agua = (
        p
        | 'Importando os Dados' >> beam.io.ReadFromText(
            'gs://dados_tcc_facens_sp/inputs/AGUA_dados_tratados.csv',
            skip_header_lines=1)

        # Processar linha diretamente, dividindo por ponto e vírgula
        | 'Processar Linha' >> beam.Map(process_line)

        | 'Filtrar Linhas Vazias' >> beam.Filter(filter_empty_lines)

        | 'Formatando Saída' >> beam.Map(lambda line: ','.join(map(str, line)))  # Formata a saída como uma string CSV

        | 'Salvar no Bucket' >> beam.io.WriteToText('gs://dados_tcc_facens_sp/output/d_agua', file_name_suffix='.csv')
    )
