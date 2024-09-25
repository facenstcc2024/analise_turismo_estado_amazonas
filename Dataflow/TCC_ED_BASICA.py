# Código Funcionando perfeitamente

# Para rodar os dados de Ensino Básico

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
    'template_location': 'gs://dados_tcc_facens_sp/template/dataflow-edbasica-pipeline-lucas',
    'save_main_session': True,
    'num_workers': 8,             # Número inicial de workers
    'max_num_workers': 8,         # Número máximo de workers
    'worker_machine_type': 'n2-standard-8',  # Tipo de máquina para workers
    'autoscaling_algorithm': 'THROUGHPUT_BASED',  # Escalabilidade automática
}

# Caminho do arquivo no Google Cloud Storage
file_path = 'gs://dados_tcc_facens_sp/inputs/microdados_ed_basica_2023_utf8.csv'

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

    # Verificar se a linha tem colunas suficientes
    if len(columns) > 285:  # Certifique-se de que há pelo menos 286 colunas
        # Selecionar as colunas de interesse
        municipio = columns[6]
        column_285 = safe_int_convert(columns[285])

        # Remover caracteres especiais do município
        municipio = remove_special_characters(municipio)

        # Retornar a chave (Município) e o valor da coluna 285
        return (municipio, column_285)
    
    # Se a linha não tem colunas suficientes, retorne uma tupla com valores padrão
    return ('UNKNOWN', 0)

def filter_uf(line):
    """Filtra linhas com base no UF desejado."""
    columns = line.split(';')
    # Assumindo que o UF está na coluna 4
    if len(columns) > 4:
        return columns[4] == 'AM'
    return False

def filter_empty_lines(element):
    """Filtra linhas vazias do PCollection."""
    return element is not None and element != ('UNKNOWN', 0)

# Opções do pipeline
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

with beam.Pipeline(options=pipeline_options) as p:
    filtered_data = (
        p
        | 'Importando os Dados' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
        | 'Filtrar por UF' >> beam.Filter(filter_uf)  # Filtra por UF antes de processar
    )
    
    processed_data = (
        filtered_data
        | 'Processar Linha' >> beam.Map(process_line)
        | 'Filtrar Linhas Vazias' >> beam.Filter(filter_empty_lines)
    )

    aggregated_data = (
        processed_data
        | 'Agrupar por Município' >> beam.GroupByKey()
        | 'Somar Quantidades' >> beam.Map(lambda kv: (kv[0], sum(kv[1])))
    )

    formatted_data = (
        aggregated_data
        | 'Formatar para Template' >> beam.Map(lambda kv: f"{kv[0]},{kv[1]}")
        
        
        | 'Salvar no Bucket' >> beam.io.WriteToText('gs://dados_tcc_facens_sp/output/d_ed_basica_lucas', file_name_suffix='.csv')
    )

