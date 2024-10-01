# Código Funcionando perfeitamente

# Para rodar os dados de Ensino Técnico

import apache_beam as beam
import unicodedata
import re
from apache_beam.options.pipeline_options import PipelineOptions
import os

# Configuração da chave de autenticação do Google Cloud
sa = r'D:\FACENS\tcc_facens\teste_15_08\security\service-account.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa

pipeline_options = {
    'project': 'tccfacens2024',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://dados_tcc_facens_sp/tmp',
    'temp_location': 'gs://dados_tcc_facens_sp/tmp',
    'template_location': 'gs://dados_tcc_facens_sp/template/dataflow-edtecnica-pipeline-lucas',
    'save_main_session': True,
    'num_workers': 8,
    'max_num_workers': 8,
    'worker_machine_type': 'n2-standard-8',
    'autoscaling_algorithm': 'THROUGHPUT_BASED',
}

file_path = 'gs://dados_tcc_facens_sp/inputs/suplemento_cursos_tecnicos_2023_utf8.csv'

def remove_special_characters(text):
    """Remove acentos e caracteres especiais do texto."""
    text = unicodedata.normalize('NFD', text.upper())
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    text = re.sub(r'[^A-Z0-9\s]', '', text)  # Remove caracteres não alfanuméricos e não espaciais
    return text

def safe_int_convert(text):
    """Tenta converter um texto para inteiro, retorna 0 se falhar."""
    try:
        return int(text)
    except ValueError:
        return 0

def process_line(line):
    """Processa uma linha do CSV, dividindo e transformando os dados."""
    columns = line.split(';')

    if len(columns) > 18:
        municipio = columns[6]
        area = columns[13]
        curso = columns[15]
        matriculas = safe_int_convert(columns[18])

        municipio = remove_special_characters(municipio)
        area = remove_special_characters(area)
        curso = remove_special_characters(curso)

        return ((municipio, area, curso), matriculas)

    return (('UNKNOWN', 'UNKNOWN', 'UNKNOWN'), 0)

def filter_uf(line):
    """Filtra linhas com base no UF desejado."""
    columns = line.split(';')
    if len(columns) > 4:
        return columns[4] == 'AM'
    return False

def filter_empty_lines(element):
    """Filtra linhas vazias do PCollection."""
    return element is not None and element[1] > 0

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

with beam.Pipeline(options=pipeline_options) as p:
    filtered_data = (
        p
        | 'Importando os Dados' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
        | 'Filtrar por UF' >> beam.Filter(filter_uf)
    )
    
    processed_data = (
        filtered_data
        | 'Processar Linha' >> beam.Map(process_line)
        | 'Filtrar Linhas Vazias' >> beam.Filter(filter_empty_lines)
    )

    aggregated_data = (
        processed_data
        | 'Agrupar por Chave' >> beam.GroupByKey()
        | 'Somar Matrículas' >> beam.Map(lambda kv: (
            kv[0],  # (Município, Curso, Área)
            sum(kv[1])  # Soma das matrículas
        ))
    )

    formatted_data = (
        aggregated_data
        | 'Formatar para Template' >> beam.Map(lambda kv: f"{kv[0][0]},{kv[0][1]},{kv[0][2]},{kv[1]}")
        | 'Salvar no Bucket' >> beam.io.WriteToText('gs://dados_tcc_facens_sp/output/d_ed_tecnica_lucas', file_name_suffix='.csv')
    )
