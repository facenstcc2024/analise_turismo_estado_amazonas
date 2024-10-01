
# Código Funcionando perfeitamente

# Para rodar os dados de Ensino Superior

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
    'template_location': 'gs://dados_tcc_facens_sp/template/dataflow-edsuperior-pipeline-lucas',
    'save_main_session': True,
    'num_workers': 8,
    'max_num_workers': 8,
    'worker_machine_type': 'n2-standard-8',
    'autoscaling_algorithm': 'THROUGHPUT_BASED',
}

# Caminho do arquivo no Google Cloud Storage
file_path = 'gs://dados_tcc_facens_sp/inputs/MICRODADOS_CADASTRO_CURSOS_2022.CSV'

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
    # Decodifica manualmente os bytes para string usando latin1
    line = line.decode('latin1')
    columns = line.split(';')

    if len(columns) > 76:
        municipio = columns[6]
        curso = columns[14]
        rotulo = columns[16]
        area_geral = columns[19]
        area_especifica = columns[21]
        area_detalhada = columns[23]
        matriculas = safe_int_convert(columns[75])
        
        municipio = remove_special_characters(municipio)
        curso = remove_special_characters(curso)
        rotulo = remove_special_characters(rotulo)
        area_geral = remove_special_characters(area_geral)
        area_especifica = remove_special_characters(area_especifica)
        area_detalhada = remove_special_characters(area_detalhada)
        
        return ((municipio, curso, rotulo, area_geral, area_especifica, area_detalhada), matriculas)
    
    return (('UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'), 0)

def filter_uf(line):
    """Filtra linhas com base no UF desejado."""
    # Decodifica manualmente os bytes para string usando latin1
    line = line.decode('latin1')
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
        | 'Importando os Dados' >> beam.io.ReadFromText(file_path, skip_header_lines=1, coder=beam.coders.BytesCoder())  # Usando BytesCoder para evitar problemas de decodificação
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
            kv[0], 
            sum(kv[1])
        ))
    )

    formatted_data = (
        aggregated_data
        | 'Formatar para Template' >> beam.Map(lambda kv: f"{kv[0][0]},{kv[0][1]},{kv[0][2]},{kv[0][3]}, {kv[0][4]}, {kv[0][5]}, {kv[1]}")
        | 'Salvar no Bucket' >> beam.io.WriteToText('gs://dados_tcc_facens_sp/output/d_ed_superior_lucas', file_name_suffix='.csv')
    )