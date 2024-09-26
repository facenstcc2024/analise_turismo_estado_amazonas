# analise_turismo_regiao_amazonica

## Overview



## Table of Contents

- [Introdução](#introdução)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Montando o Ambiente no GCP](#montando-o-ambiente-no-gcp)
  - [Criando Conta no GCP](#criando-conta-no-gcp)
  - [Como Executar os Scripts](#como-executar-os-scripts)
- [Criando Buckets no Cloud Storage](#criando-buckets-no-cloud-storage)
  - [Criando Buckets](#criando-buckets)
  - [Copiando Arquivos CSV de Input](#copiando-arquivos-csv-de-input)
- [Processando Dados com Google Dataflow](#processando-dados-com-google-dataflow)
  - [Criando Script Dataflow](#criando-script-dataflow)
- [Criando os Datasets BigQuery](#criando-os-datasets-bigquery)
  - [Criando o Dataset TCC_Turismo_staging e Suas Tabelas](#criando-o-dataset-tcc_turismo_staging-e-suas-tabelas)
  - [Criando o Dataset TCC_Turismo e Suas Tabelas](#criando-o-dataset-tcc_turismo-e-suas-tabelas)
  - [Criando as Procedures BigQuery de Merge](#criando-as-procedures-bigquery-de-merge)
- [Criando Cloud Function para Carga e Merge](#criando-cloud-function-para-carga-e-merge)
  - [Ativando os Serviços Necessários](#ativando-os-serviços-necessários)
  - [Criando e Fazendo Deploy da Cloud Function](#criando-e-fazendo-deploy-da-cloud-function)
- [Criando Cloud Scheduler para Agendamento](#criando-cloud-scheduler-para-agendamento)
  - [Criando Conta de Serviço](#criando-conta-de-serviço)
  - [Criando Role com Permissões para Conta de Serviço](#criando-role-com-permissões-para-conta-de-serviço)
  - [Adicionando Role de Permissões para Conta de Serviço](#adicionando-role-de-permissões-para-conta-de-serviço)
  - [Criando Agendamentos Cloud Scheduler](#criando-agendamentos-cloud-scheduler)

## Introdução

Este projeto é focado na construção de um pipeline de dados no Google Cloud Platform (GCP) utilizando várias tecnologias.

## Tecnologias Utilizadas

- **GCP**  

Google Cloud Platform é o provedor de nuvem publico da google, criado em 2008. O Google Cloud Platform utiliza a mesma infraestrutura de seus produtos para usuários, como 
o buscador google e youtube.
O provedor google Cloud platform oferece uma imensa gama de
serviços de computação na nuvem, entre eles :
virtualização de maquinas
armazenamento de dados
bancos de dados relacionais
processamento de dados massivos
analises de dados
processamento de dados em tempo real
serviços de containers
serviços de computação sem servidor

O google é considerado o terceiro maior provedor de nuvem, sendo superado somente pela AWS (Amazon) e Azure (Microsoft)

- **Google Dataflow**  

O Google Dataflow é um serviço de processamento de dados, baseado na biblioteca Apache Beam, disponivel para linguagem de programação como Python, Java .
O Dataflow permite a ingestão , processamento e analise de dados em fluxos em lote ou em tempo real, sem ser necessários provisionamente de servidores ou clusters, sendo recomendados para fluxos de dados de grandes volumetrias ou volumetrias menores de dados.
Caracteristicas do Google Dataflow
Automaticamente escalavel - Ajuste de recursos de processamento de acordo com a carga de trabalho.

Compativel com processamento em Lote e em tempo real - utilização da mesma linguagem para processamento de dados, tanto em tempo real como em Lote.

Integração - Fácil integração com demais serviços do google cloud, como  Pub/Sub, Big Quer, Cloud Storage.

- **Google Cloud Storage**  

- **Google Big Query**  

Serviço de  datawarehouse do google, totalmente gerenciada e sem servidor,
 trabalhando com dados na escala de peyabytes, com a possibilidade de trabalhar com dados estrututados e semi estruturados, onde as consultas são feitas com linguagem SQL like.
O Big query pode ser facilmente utilizado com diversas outras ferramentas, seja dentro do google cloud platform ou fora dele. O Big query pode ser utilizados para processamentos de dados em Lote ou em tempo real, alem de ter suporte para machine learning utiliza linguagem proxima ao SQL.


- **Cloud Functions**  

- **Cloud Scheduler**  

## Montando o Ambiente no GCP

### Criando Conta no GCP

Você precisará de uma conta no Google Cloud Platform.

### Como Executar os Scripts

Siga as instruções abaixo para executar os scripts.

## Criando Buckets no Cloud Storage

### Criando Buckets

Crie os buckets no Google Cloud Storage.

### Copiando Arquivos CSV de Input

Copie os arquivos CSV necessários para o processamento.

## Processando Dados com Google Dataflow

### Criando Script Dataflow

Crie o script de processamento utilizando Google Dataflow.

## Criando os Datasets BigQuery


```bash
bq --location=southamerica-east1 mk \
--default_table_expiration 0 \
--dataset TCC_Turismo_staging
```

```
bq --location=southamerica-east1 mk \
--default_table_expiration 0 \
--dataset TCC_Turismo
```


### Criando o Dataset TCC_Turismo_staging e Suas Tabelas


abrir console gcp
abrir clod shell
opções do cloud shell, upload pasta BigQuerySchema_staging
rodar os comando abaixo

```
bq mk --table \
TCC_Turismo_staging.d_Agua \
BigQuerySchema_staging/d_Agua.json
```


```
bq mk --table \
TCC_Turismo_staging.d_Ensino_Basico \
BigQuerySchema_staging/d_Ensino_Basico.json
```


```
bq mk --table \
TCC_Turismo_staging.d_Ensino_Superior \
BigQuerySchema_staging/d_Ensino_Superior.json

```

```
bq mk --table \
TCC_Turismo_staging.d_Ensino_tecnico \
BigQuerySchema_staging/d_Ensino_tecnico.json
```


```
bq mk --table \
TCC_Turismo_staging.d_infra_Turismo \
BigQuerySchema_staging/d_infra_Turismo.json
```


```
bq mk --table \
TCC_Turismo_staging.d_Localizacao \
BigQuerySchema_staging/d_Localizacao.json
```


```
bq mk --table \
TCC_Turismo_staging.d_Regiao_Turistica \
BigQuerySchema_staging/d_Regiao_Turistica.json
```


```
bq mk --table \
TCC_Turismo_staging.d_saude \
BigQuerySchema_staging/d_saude.json
```


```
bq mk --table \
TCC_Turismo_staging.variavel_dependente_visitantes_tabela_1 \
BigQuerySchema_staging/variavel_dependente_visitantes_tabela_1.json
```


```
bq mk --table \
TCC_Turismo_staging.variavel_dependente_visitantes_tabela_2 \
BigQuerySchema_staging/variavel_dependente_visitantes_tabela_2.json

```

### Criando o Dataset TCC_Turismo e Suas Tabelas

abrir console gcp
abrir clod shell
opções do cloud shell, upload pasta BigQuerySchema
rodar os comando abaixo

```
bq mk --table \
TCC_Turismo.d_Agua \
BigQuerySchema/d_Agua.json
```

```
bq mk --table \
TCC_Turismo.d_Ensino_Basico \
BigQuerySchema/d_Ensino_Basico.json
```



```
bq mk --table \
TCC_Turismo.d_Ensino_Superior \
BigQuerySchema/d_Ensino_Superior.json
```



```
bq mk --table \
TCC_Turismo.d_Ensino_tecnico \
BigQuerySchema/d_Ensino_tecnico.json
```


```
bq mk --table \
TCC_Turismo.d_infra_Turismo \
BigQuerySchema/d_infra_Turismo.json
```

```
bq mk --table \
TCC_Turismo.d_Localizacao \
BigQuerySchema/d_Localizacao.json
```


```
bq mk --table \
TCC_Turismo.d_Regiao_Turistica \
BigQuerySchema/d_Regiao_Turistica.json
```


```
bq mk --table \
TCC_Turismo.d_saude \
BigQuerySchema/d_saude.json
```


```
bq mk --table \
TCC_Turismo.variavel_dependente_visitantes_tabela_1 \
BigQuerySchema/variavel_dependente_visitantes_tabela_1.json
```


```
bq mk --table \
TCC_Turismo.variavel_dependente_visitantes_tabela_2 \
BigQuerySchema/variavel_dependente_visitantes_tabela_2.json
```

### Criando as Procedures BigQuery de Merge

Apos Abrir o Big Query, no menu vertical a esquerdam clique na primeira opção big query Studio, e abra uma aba de consulta SQL .
Na aba de consulta SQL executar cada bloco de codigo abaixo,
para criar as procedures de carga e tratamento de dados das tabelas intermediarias para as tabelas finais

#####################

```
CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_d_Agua`()
BEGIN


MERGE `tccfacens2024.TCC_Turismo.d_Agua` tgt
USING (SELECT NO_MUNICIPIO,
cast(ANO_REF as Integer) as ANO_REF,
cast(AGUA_POPULACAO_ATEND_QT as Integer) as AGUA_POPULACAO_ATEND_QT 
FROM `tccfacens2024.TCC_Turismo_staging.d_Agua` ) as src
ON
  tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO and 
  tgt.ANO_REF  = src.ANO_REF and 
  tgt.AGUA_POPULACAO_ATEND_QT  = src.AGUA_POPULACAO_ATEND_QT 

WHEN MATCHED THEN
  UPDATE SET 
    tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO,
    tgt.ANO_REF  = src.ANO_REF,
    tgt.AGUA_POPULACAO_ATEND_QT  = src.AGUA_POPULACAO_ATEND_QT 
  
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    NO_MUNICIPIO ,
    ANO_REF ,
    AGUA_POPULACAO_ATEND_QT  )
  VALUES (
    src.NO_MUNICIPIO ,
    src.ANO_REF ,
    src.AGUA_POPULACAO_ATEND_QT 
    );
    
DELETE FROM `tccfacens2024.TCC_Turismo_staging.d_Agua` WHERE 1 = 1 ;
END;
```



#############################



```
CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_d_Ensino_Basico`()
BEGIN


MERGE `tccfacens2024.TCC_Turismo.d_Ensino_Basico` tgt
USING (SELECT NO_MUNICIPIO,
cast(QT_MAT_BAS as Integer) as QT_MAT_BAS
FROM `tccfacens2024.TCC_Turismo_staging.d_Ensino_Basico` ) as src
ON
  tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO and 
  tgt.QT_MAT_BAS  = src.QT_MAT_BAS 

WHEN MATCHED THEN
  UPDATE SET 
    tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO,
    tgt.QT_MAT_BAS  = src.QT_MAT_BAS
  
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    NO_MUNICIPIO ,
    QT_MAT_BAS  )
  VALUES (
    src.NO_MUNICIPIO ,
    src.QT_MAT_BAS 
    );
    
DELETE FROM `tccfacens2024.TCC_Turismo_staging.d_Ensino_Basico`  WHERE 1 = 1;
END;

```

#################################

```
CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_d_Ensino_Superior`()
BEGIN


MERGE `tccfacens2024.TCC_Turismo.d_Ensino_Superior` tgt
USING (
  SELECT 
    NO_MUNICIPIO,
    NO_CURSO,
    NO_CINE_ROTULO,
    NO_CINE_AREA_GERAL,
    NO_CINE_AREA_DETALHADA,
    NO_CINE_AREA_ESPECIFICA,
  cast(QT_MAT as Integer) as QT_MAT
  FROM `tccfacens2024.TCC_Turismo_staging.d_Ensino_Superior` ) as src
ON
  tgt.NO_MUNICIPIO = src.NO_MUNICIPIO	and
  tgt.NO_CURSO= src.NO_CURSO	and
  tgt.NO_CINE_ROTULO = src.NO_CINE_ROTULO and	
  tgt.NO_CINE_AREA_GERAL = src.NO_CINE_AREA_GERAL and	
  tgt.NO_CINE_AREA_DETALHADA = src.NO_CINE_AREA_DETALHADA and	
  tgt.NO_CINE_AREA_ESPECIFICA = src.NO_CINE_AREA_ESPECIFICA	and
  tgt.QT_MAT = src.QT_MAT	

WHEN MATCHED THEN
  UPDATE SET 
    tgt.NO_MUNICIPIO = src.NO_MUNICIPIO	,
    tgt.NO_CURSO= src.NO_CURSO	,
    tgt.NO_CINE_ROTULO = src.NO_CINE_ROTULO	,
    tgt.NO_CINE_AREA_GERAL = src.NO_CINE_AREA_GERAL	,
    tgt.NO_CINE_AREA_DETALHADA = src.NO_CINE_AREA_DETALHADA	,
    tgt.NO_CINE_AREA_ESPECIFICA = src.NO_CINE_AREA_ESPECIFICA	,
    tgt.QT_MAT = src.QT_MAT	
  
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    NO_MUNICIPIO ,
    NO_CURSO,
    NO_CINE_ROTULO,
    NO_CINE_AREA_GERAL,
    NO_CINE_AREA_DETALHADA,
    NO_CINE_AREA_ESPECIFICA,
    QT_MAT  )
  VALUES (
    src.NO_MUNICIPIO ,
    src.NO_CURSO ,
    src.NO_CINE_ROTULO ,
    src.NO_CINE_AREA_GERAL ,
    src.NO_CINE_AREA_DETALHADA ,
    src.NO_CINE_AREA_ESPECIFICA ,
    src.QT_MAT 
    );
    
DELETE FROM `tccfacens2024.TCC_Turismo_staging.d_Ensino_Superior` WHERE 1 = 1;
END;
```

############################################


```
CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_d_Ensino_Tecnico`()
BEGIN


MERGE `tccfacens2024.TCC_Turismo.d_Ensino_tecnico` tgt
USING (
  SELECT 
    NO_MUNICIPIO,
    NO_AREA_CURSO_PROFISSIONAL,
    NO_CURSO_EDUC_PROFISSIONAL,
  cast(QT_MAT_CURSO_TEC as Integer) as QT_MAT_CURSO_TEC
  FROM `tccfacens2024.TCC_Turismo_staging.d_Ensino_tecnico` ) as src
ON
  tgt.NO_MUNICIPIO = src.NO_MUNICIPIO	and
  tgt.NO_AREA_CURSO_PROFISSIONAL= src.NO_AREA_CURSO_PROFISSIONAL	and
  tgt.NO_CURSO_EDUC_PROFISSIONAL = src.NO_CURSO_EDUC_PROFISSIONAL and	
  tgt.QT_MAT_CURSO_TEC = src.QT_MAT_CURSO_TEC 

WHEN MATCHED THEN
  UPDATE SET 
    tgt.NO_MUNICIPIO = src.NO_MUNICIPIO	,
    tgt.NO_AREA_CURSO_PROFISSIONAL= src.NO_AREA_CURSO_PROFISSIONAL	,
    tgt.NO_CURSO_EDUC_PROFISSIONAL = src.NO_CURSO_EDUC_PROFISSIONAL	,
    tgt.QT_MAT_CURSO_TEC = src.QT_MAT_CURSO_TEC	
  
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    NO_MUNICIPIO ,
    NO_AREA_CURSO_PROFISSIONAL,
    NO_CURSO_EDUC_PROFISSIONAL,
    QT_MAT_CURSO_TEC)
  VALUES (
    src.NO_MUNICIPIO ,
    src.NO_AREA_CURSO_PROFISSIONAL ,
    src.NO_CURSO_EDUC_PROFISSIONAL ,
    src.QT_MAT_CURSO_TEC 
    );
    
DELETE FROM `tccfacens2024.TCC_Turismo_staging.d_Ensino_tecnico` WHERE 1 = 1;
END;

```


#############################################

```
CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_d_Localizacao`()
BEGIN


MERGE `tccfacens2024.TCC_Turismo.d_Localizacao` tgt
USING (
  select 
    NO_MUNICIPIO,
    cast(ANO_REF as integer) as ANO_REF,
    UF,
    ESTADO,
    REGIAO,
    MICRORREGIAO,
    cast(replace(POP_TOTAL,'.','') as integer) as POP_TOTAL,
    cast(replace(POP_URB,'.','') as integer) as POP_URB ,
    cast(FAIXA_POP as integer) as FAIXA_POP, 
    DESC_FAIXA,
    cast(replace(AREA_KM2,'.','') as integer) as AREA_KM2
  from `TCC_Turismo_staging.d_Localizacao`
 ) as src
ON
  tgt.NO_MUNICIPIO = src.NO_MUNICIPIO	and 
  tgt.ANO_REF = src.ANO_REF	and
  tgt.UF = src.UF and	
  tgt.ESTADO = src.ESTADO and
  tgt.REGIAO = src.REGIAO	and
  tgt.MICRORREGIAO = src.MICRORREGIAO	and
  tgt.POP_TOTAL = src.POP_TOTAL and	
  tgt.POP_URB = src.POP_URB and
  tgt.FAIXA_POP = src.FAIXA_POP	and
  tgt.DESC_FAIXA = src.DESC_FAIXA	and
  tgt.AREA_KM2 = src.AREA_KM2 

WHEN MATCHED THEN
  UPDATE SET 
    tgt.NO_MUNICIPIO = src.NO_MUNICIPIO	,
    tgt.ANO_REF = src.ANO_REF	,
    tgt.UF = src.UF	,
    tgt.ESTADO = src.ESTADO, 
    tgt.REGIAO = src.REGIAO	,
    tgt.MICRORREGIAO = src.MICRORREGIAO	,
    tgt.POP_TOTAL = src.POP_TOTAL	,
    tgt.POP_URB = src.POP_URB, 
    tgt.FAIXA_POP = src.FAIXA_POP	,
    tgt.DESC_FAIXA = src.DESC_FAIXA	,
    tgt.AREA_KM2 = src.AREA_KM2	

  
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    NO_MUNICIPIO ,
    ANO_REF,
    UF,
    ESTADO,
    REGIAO,
    MICRORREGIAO,
    POP_TOTAL,
    POP_URB,
    FAIXA_POP,
    DESC_FAIXA,
    AREA_KM2
    )
  VALUES (
    src.NO_MUNICIPIO ,
    src.ANO_REF ,
    src.UF ,
    src.ESTADO ,
    src.REGIAO ,
    src.MICRORREGIAO ,
    src.POP_TOTAL ,
    src.POP_URB ,
    src.FAIXA_POP ,
    src.DESC_FAIXA ,   
    src.AREA_KM2
    );
    
DELETE   from `TCC_Turismo_staging.d_Localizacao` WHERE 1 = 1;
END;
```


############################################


```
CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_d_Regiao_Turistica`()
BEGIN


MERGE `tccfacens2024.TCC_Turismo.d_Regiao_Turistica` tgt
USING (SELECT 
  NO_MUNICIPIO,
  REGIAO_TURISTICA,
  CATEGORIA_TURISMO
FROM `tccfacens2024.TCC_Turismo_staging.d_Regiao_Turistica` ) as src
ON
  tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO and 
  tgt.REGIAO_TURISTICA  = src.REGIAO_TURISTICA and 
  tgt.CATEGORIA_TURISMO  = src.CATEGORIA_TURISMO 

WHEN MATCHED THEN
  UPDATE SET 
    tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO,
    tgt.REGIAO_TURISTICA  = src.REGIAO_TURISTICA,
    tgt.CATEGORIA_TURISMO  = src.CATEGORIA_TURISMO 
  
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    NO_MUNICIPIO ,
    REGIAO_TURISTICA ,
    CATEGORIA_TURISMO  )
  VALUES (
    src.NO_MUNICIPIO ,
    src.REGIAO_TURISTICA ,
    src.CATEGORIA_TURISMO 
    );
    
DELETE FROM `tccfacens2024.TCC_Turismo_staging.d_Regiao_Turistica` WHERE 1 = 1;
END;

```

################################################

```

CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_d_infra_Turismo`()
BEGIN
  MERGE `tccfacens2024.TCC_Turismo.d_infra_Turismo` tgt
  USING (
    SELECT
      NO_MUNICIPIO,
      PROJETOS_MTUR,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_AGRICULTURA_E_PECUARIA,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_TURISMO,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_OUTROS,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_SERVICOS,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_COMERCIO,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_INDUSTRIA_DE_BASE,
      FUNDO_MUNICIPAL_TURISMO,
      PLANO_MUNICIPAL_TURISMO,
      PLANO_MKT_TURISMO,
      PROJETOS_ATIVIDADE_TURISTICA,
      CAST(HOSPEDAGEM_QT AS INT64) AS HOSPEDAGEM_QT,
      CAST(LEITOS_QT AS INT64) AS LEITOS_QT,
      LOCADORAS_VEICULOS,
      CAST(AGENCIAS_BANCARIAS_QT AS INT64) AS AGENCIAS_BANCARIAS_QT,
      AEROPORTO,
      TIPOS_TRANSPORTE,
      TIPOS_TRANSPORTE_RODOVIARIO,
      TIPOS_TRANSPORTE_AQUAVIARIO,
      TIPOS_TRANSPORTE_AEREO,
      TIPOS_TRANSPORTE_OUTROS,
      ACESSO_DESTINO_TURISTICO,
      ACESSO_DESTINO_TURISTICO_HIDROVIA,
      ACESSO_DESTINO_TURISTICO_RODOVIA,
      ACESSO_DESTINO_TURISTICO_AEROPORTO,
      ACESSO_DESTINO_TURISTICO_FERROVIA,
      ACESSO_DESTINO_TURISTICO_OUTROS,
      SITUACAO_SINALIZACAO,
      ROTA_TURISTICA,
      INTERLIGACAO_ENTRE_ATRATIVOS,
      SINALIZACAO_ACESSIBILIDADE,
      RESERVA_ESPACO_ACESSIBILIDADE,
      PROFISSIONAIS_ACESSIBILIDADE,
      CAST(EMPRESAS_TURISMO_QT AS INT64) AS EMPRESAS_TURISMO_QT,
      TIPOS_PATRIMONIO_NATURAL,
      TIPOS_PATRIMONIO_NATURAL_PARQUES_NATURAIS,
      TIPOS_PATRIMONIO_NATURAL_UNIDADE_DE_CONSERVACAO,
      TIPOS_PATRIMONIO_NATURAL_RESERVAS_ECOLOGICAS,
      TIPOS_PATRIMONIO_NATURAL_OUTROS,
      UNIDADES_CONSERVACAO,
      TIPOS_PATRIMONIO_CULTURAL,
      PATRIMONIO_CULTURAL_HISTORICO,
      PATRIMONIO_CULTURAL_EQUIPAMENTOS,
      PATRIMONIO_CULTURAL_OUTROS,
      BARCOS_TURISMO,
      TURISMO_PESCA,
      TURISMO_MERGULHO
    FROM `tccfacens2024.TCC_Turismo_staging.d_infra_Turismo`
  ) AS src
  ON tgt.NO_MUNICIPIO = src.NO_MUNICIPIO

  WHEN MATCHED THEN
    UPDATE SET
      tgt.NO_MUNICIPIO = src.NO_MUNICIPIO,
      tgt.PROJETOS_MTUR = src.PROJETOS_MTUR,
      tgt.PRINCIPAIS_ATIVIDADES_ECONOMICAS = src.PRINCIPAIS_ATIVIDADES_ECONOMICAS,
      tgt.PRINCIPAIS_ATIVIDADES_ECONOMICAS_AGRICULTURA_E_PECUARIA = src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_AGRICULTURA_E_PECUARIA,
      tgt.PRINCIPAIS_ATIVIDADES_ECONOMICAS_TURISMO = src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_TURISMO,
      tgt.PRINCIPAIS_ATIVIDADES_ECONOMICAS_OUTROS = src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_OUTROS,
      tgt.PRINCIPAIS_ATIVIDADES_ECONOMICAS_SERVICOS = src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_SERVICOS,
      tgt.PRINCIPAIS_ATIVIDADES_ECONOMICAS_COMERCIO = src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_COMERCIO,
      tgt.PRINCIPAIS_ATIVIDADES_ECONOMICAS_INDUSTRIA_DE_BASE = src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_INDUSTRIA_DE_BASE,
      tgt.FUNDO_MUNICIPAL_TURISMO = src.FUNDO_MUNICIPAL_TURISMO,
      tgt.PLANO_MUNICIPAL_TURISMO = src.PLANO_MUNICIPAL_TURISMO,
      tgt.PLANO_MKT_TURISMO = src.PLANO_MKT_TURISMO,
      tgt.PROJETOS_ATIVIDADE_TURISTICA = src.PROJETOS_ATIVIDADE_TURISTICA,
      tgt.HOSPEDAGEM_QT = src.HOSPEDAGEM_QT,
      tgt.LEITOS_QT = src.LEITOS_QT,
      tgt.LOCADORAS_VEICULOS = src.LOCADORAS_VEICULOS,
      tgt.AGENCIAS_BANCARIAS_QT = src.AGENCIAS_BANCARIAS_QT,
      tgt.AEROPORTO = src.AEROPORTO,
      tgt.TIPOS_TRANSPORTE = src.TIPOS_TRANSPORTE,
      tgt.TIPOS_TRANSPORTE_RODOVIARIO = src.TIPOS_TRANSPORTE_RODOVIARIO,
      tgt.TIPOS_TRANSPORTE_AQUAVIARIO = src.TIPOS_TRANSPORTE_AQUAVIARIO,
      tgt.TIPOS_TRANSPORTE_AEREO = src.TIPOS_TRANSPORTE_AEREO,
      tgt.TIPOS_TRANSPORTE_OUTROS = src.TIPOS_TRANSPORTE_OUTROS,
      tgt.ACESSO_DESTINO_TURISTICO = src.ACESSO_DESTINO_TURISTICO,
      tgt.ACESSO_DESTINO_TURISTICO_HIDROVIA = src.ACESSO_DESTINO_TURISTICO_HIDROVIA,
      tgt.ACESSO_DESTINO_TURISTICO_RODOVIA = src.ACESSO_DESTINO_TURISTICO_RODOVIA,
      tgt.ACESSO_DESTINO_TURISTICO_AEROPORTO = src.ACESSO_DESTINO_TURISTICO_AEROPORTO,
      tgt.ACESSO_DESTINO_TURISTICO_FERROVIA = src.ACESSO_DESTINO_TURISTICO_FERROVIA,
      tgt.ACESSO_DESTINO_TURISTICO_OUTROS = src.ACESSO_DESTINO_TURISTICO_OUTROS,
      tgt.SITUACAO_SINALIZACAO = src.SITUACAO_SINALIZACAO,
      tgt.ROTA_TURISTICA = src.ROTA_TURISTICA,
      tgt.INTERLIGACAO_ENTRE_ATRATIVOS = src.INTERLIGACAO_ENTRE_ATRATIVOS,
      tgt.SINALIZACAO_ACESSIBILIDADE = src.SINALIZACAO_ACESSIBILIDADE,
      tgt.RESERVA_ESPACO_ACESSIBILIDADE = src.RESERVA_ESPACO_ACESSIBILIDADE,
      tgt.PROFISSIONAIS_ACESSIBILIDADE = src.PROFISSIONAIS_ACESSIBILIDADE,
      tgt.EMPRESAS_TURISMO_QT = src.EMPRESAS_TURISMO_QT,
      tgt.TIPOS_PATRIMONIO_NATURAL = src.TIPOS_PATRIMONIO_NATURAL,
      tgt.TIPOS_PATRIMONIO_NATURAL_PARQUES_NATURAIS = src.TIPOS_PATRIMONIO_NATURAL_PARQUES_NATURAIS,
      tgt.TIPOS_PATRIMONIO_NATURAL_UNIDADE_DE_CONSERVACAO = src.TIPOS_PATRIMONIO_NATURAL_UNIDADE_DE_CONSERVACAO,
      tgt.TIPOS_PATRIMONIO_NATURAL_RESERVAS_ECOLOGICAS = src.TIPOS_PATRIMONIO_NATURAL_RESERVAS_ECOLOGICAS,
      tgt.TIPOS_PATRIMONIO_NATURAL_OUTROS = src.TIPOS_PATRIMONIO_NATURAL_OUTROS,
      tgt.UNIDADES_CONSERVACAO = src.UNIDADES_CONSERVACAO,
      tgt.TIPOS_PATRIMONIO_CULTURAL = src.TIPOS_PATRIMONIO_CULTURAL,
      tgt.PATRIMONIO_CULTURAL_HISTORICO = src.PATRIMONIO_CULTURAL_HISTORICO,
      tgt.PATRIMONIO_CULTURAL_EQUIPAMENTOS = src.PATRIMONIO_CULTURAL_EQUIPAMENTOS,
      tgt.PATRIMONIO_CULTURAL_OUTROS = src.PATRIMONIO_CULTURAL_OUTROS,
      tgt.BARCOS_TURISMO = src.BARCOS_TURISMO,
      tgt.TURISMO_PESCA = src.TURISMO_PESCA

,
      tgt.TURISMO_MERGULHO = src.TURISMO_MERGULHO

  WHEN NOT MATCHED BY TARGET THEN
    INSERT (
      NO_MUNICIPIO,
      PROJETOS_MTUR,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_AGRICULTURA_E_PECUARIA,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_TURISMO,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_OUTROS,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_SERVICOS,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_COMERCIO,
      PRINCIPAIS_ATIVIDADES_ECONOMICAS_INDUSTRIA_DE_BASE,
      FUNDO_MUNICIPAL_TURISMO,
      PLANO_MUNICIPAL_TURISMO,
      PLANO_MKT_TURISMO,
      PROJETOS_ATIVIDADE_TURISTICA,
      HOSPEDAGEM_QT,
      LEITOS_QT,
      LOCADORAS_VEICULOS,
      AGENCIAS_BANCARIAS_QT,
      AEROPORTO,
      TIPOS_TRANSPORTE,
      TIPOS_TRANSPORTE_RODOVIARIO,
      TIPOS_TRANSPORTE_AQUAVIARIO,
      TIPOS_TRANSPORTE_AEREO,
      TIPOS_TRANSPORTE_OUTROS,
      ACESSO_DESTINO_TURISTICO,
      ACESSO_DESTINO_TURISTICO_HIDROVIA,
      ACESSO_DESTINO_TURISTICO_RODOVIA,
      ACESSO_DESTINO_TURISTICO_AEROPORTO,
      ACESSO_DESTINO_TURISTICO_FERROVIA,
      ACESSO_DESTINO_TURISTICO_OUTROS,
      SITUACAO_SINALIZACAO,
      ROTA_TURISTICA,
      INTERLIGACAO_ENTRE_ATRATIVOS,
      SINALIZACAO_ACESSIBILIDADE,
      RESERVA_ESPACO_ACESSIBILIDADE,
      PROFISSIONAIS_ACESSIBILIDADE,
      EMPRESAS_TURISMO_QT,
      TIPOS_PATRIMONIO_NATURAL,
      TIPOS_PATRIMONIO_NATURAL_PARQUES_NATURAIS,
      TIPOS_PATRIMONIO_NATURAL_UNIDADE_DE_CONSERVACAO,
      TIPOS_PATRIMONIO_NATURAL_RESERVAS_ECOLOGICAS,
      TIPOS_PATRIMONIO_NATURAL_OUTROS,
      UNIDADES_CONSERVACAO,
      TIPOS_PATRIMONIO_CULTURAL,
      PATRIMONIO_CULTURAL_HISTORICO,
      PATRIMONIO_CULTURAL_EQUIPAMENTOS,
      PATRIMONIO_CULTURAL_OUTROS,
      BARCOS_TURISMO,
      TURISMO_PESCA,
      TURISMO_MERGULHO
    )
    VALUES (
      src.NO_MUNICIPIO,
      src.PROJETOS_MTUR,
      src.PRINCIPAIS_ATIVIDADES_ECONOMICAS,
      src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_AGRICULTURA_E_PECUARIA,
      src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_TURISMO,
      src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_OUTROS,
      src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_SERVICOS,
      src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_COMERCIO,
      src.PRINCIPAIS_ATIVIDADES_ECONOMICAS_INDUSTRIA_DE_BASE,
      src.FUNDO_MUNICIPAL_TURISMO,
      src.PLANO_MUNICIPAL_TURISMO,
      src.PLANO_MKT_TURISMO,
      src.PROJETOS_ATIVIDADE_TURISTICA,
      src.HOSPEDAGEM_QT,
      src.LEITOS_QT,
      src.LOCADORAS_VEICULOS,
      src.AGENCIAS_BANCARIAS_QT,
      src.AEROPORTO,
      src.TIPOS_TRANSPORTE,
      src.TIPOS_TRANSPORTE_RODOVIARIO,
      src.TIPOS_TRANSPORTE_AQUAVIARIO,
      src.TIPOS_TRANSPORTE_AEREO,
      src.TIPOS_TRANSPORTE_OUTROS,
      src.ACESSO_DESTINO_TURISTICO,
      src.ACESSO_DESTINO_TURISTICO_HIDROVIA,
      src.ACESSO_DESTINO_TURISTICO_RODOVIA,
      src.ACESSO_DESTINO_TURISTICO_AEROPORTO,
      src.ACESSO_DESTINO_TURISTICO_FERROVIA,
      src.ACESSO_DESTINO_TURISTICO_OUTROS,
      src.SITUACAO_SINALIZACAO,
      src.ROTA_TURISTICA,
      src.INTERLIGACAO_ENTRE_ATRATIVOS,
      src.SINALIZACAO_ACESSIBILIDADE,
      src.RESERVA_ESPACO_ACESSIBILIDADE,
      src.PROFISSIONAIS_ACESSIBILIDADE,
      src.EMPRESAS_TURISMO_QT,
      src.TIPOS_PATRIMONIO_NATURAL,
      src.TIPOS_PATRIMONIO_NATURAL_PARQUES_NATURAIS,
      src.TIPOS_PATRIMONIO_NATURAL_UNIDADE_DE_CONSERVACAO,
      src.TIPOS_PATRIMONIO_NATURAL_RESERVAS_ECOLOGICAS,
      src.TIPOS_PATRIMONIO_NATURAL_OUTROS,
      src.UNIDADES_CONSERVACAO,
      src.TIPOS_PATRIMONIO_CULTURAL,
      src.PATRIMONIO_CULTURAL_HISTORICO,
      src.PATRIMONIO_CULTURAL_EQUIPAMENTOS,
      src.PATRIMONIO_CULTURAL_OUTROS,
      src.BARCOS_TURISMO,
      src.TURISMO_PESCA,
      src.TURISMO_MERGULHO
    );

DELETE FROM `tccfacens2024.TCC_Turismo_staging.d_infra_Turismo` WHERE 1 = 1;

END;

```


#####################################



```
CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_d_saude`()
BEGIN


MERGE `tccfacens2024.TCC_Turismo.d_saude` tgt
USING (SELECT NO_MUNICIPIO,
cast(LEITOS_QT as Integer) as LEITOS_QT
FROM `tccfacens2024.TCC_Turismo_staging.d_saude` ) as src
ON
  tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO  

WHEN MATCHED THEN
  UPDATE SET 
    tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO,
    tgt.LEITOS_QT  = src.LEITOS_QT
  
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    NO_MUNICIPIO ,
    LEITOS_QT  )
  VALUES (
    src.NO_MUNICIPIO ,
    src.LEITOS_QT
 
    );
    
DELETE FROM `tccfacens2024.TCC_Turismo_staging.d_saude` WHERE 1 = 1;
END;

```


######################################


```
CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_variavel_dependente_visitantes_tabela_1`()
BEGIN


MERGE `tccfacens2024.TCC_Turismo.variavel_dependente_visitantes_tabela_1` tgt
USING (
  SELECT 
    NO_MUNICIPIO,
    cast(IDHM as decimal) as IDHM,
    cast(PIB_PER_CAPITA as decimal) as PIB_PER_CAPITA
FROM `tccfacens2024.TCC_Turismo_staging.variavel_dependente_visitantes_tabela_1` ) as src
ON
  tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO and 
  tgt.IDHM  = src.IDHM and  
  tgt.PIB_PER_CAPITA  = src.PIB_PER_CAPITA 
WHEN MATCHED THEN
  UPDATE SET 
    tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO,
    tgt.IDHM  = src.IDHM,
    tgt.PIB_PER_CAPITA  = src.PIB_PER_CAPITA
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    NO_MUNICIPIO ,
    IDHM,
    PIB_PER_CAPITA  )
  VALUES (
    src.NO_MUNICIPIO ,
    src.IDHM ,
    src.PIB_PER_CAPITA
    );
    
DELETE FROM `tccfacens2024.TCC_Turismo_staging.variavel_dependente_visitantes_tabela_1`  WHERE 1 = 1;

END;
```


#####################################



```
CREATE OR REPLACE PROCEDURE `tccfacens2024.TCC_Turismo.merge_variavel_dependente_visitantes_tabela_2`()
BEGIN


MERGE `tccfacens2024.TCC_Turismo.variavel_dependente_visitantes_tabela_2` tgt
USING (
  SELECT 
    NO_MUNICIPIO,
    cast(replace(VISITAS_INTERNACIONAL_QT,'.','') as integer) as VISITAS_INTERNACIONAL_QT,
    cast(replace(VISITAS_NACIONAL_QT,'.','') as integer) as VISITAS_NACIONAL_QT
FROM `tccfacens2024.TCC_Turismo_staging.variavel_dependente_visitantes_tabela_2` ) as src
ON
  tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO and 
  tgt.VISITAS_INTERNACIONAL_QT  = src.VISITAS_INTERNACIONAL_QT and  
  tgt.VISITAS_NACIONAL_QT  = src.VISITAS_NACIONAL_QT 
WHEN MATCHED THEN
  UPDATE SET 
    tgt.NO_MUNICIPIO  = src.NO_MUNICIPIO,
    tgt.VISITAS_INTERNACIONAL_QT  = src.VISITAS_INTERNACIONAL_QT,
    tgt.VISITAS_NACIONAL_QT  = src.VISITAS_NACIONAL_QT
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    NO_MUNICIPIO ,
    VISITAS_INTERNACIONAL_QT,
    VISITAS_NACIONAL_QT  )
  VALUES (
    src.NO_MUNICIPIO ,
    src.VISITAS_INTERNACIONAL_QT ,
    src.VISITAS_NACIONAL_QT
    );
    
DELETE FROM `tccfacens2024.TCC_Turismo_staging.variavel_dependente_visitantes_tabela_2`  WHERE 1 = 1;

END;
```


######################################




## Criando Cloud Function para Carga e Merge

### Ativando os Serviços Necessários

Ative os serviços no GCP necessários para o funcionamento da Cloud Function, para isso digite os comando abaixo no shell do google cloud e aguarde alguns minutos até que os serviços estejam habilitados

```
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com
```

### Criando e Fazendo Deploy da Cloud Function

Faça o file upload da pasta CloudFunction para o cloud shell do google cloud platform,isso pode ser feito facilmente acessando o botão mais no cloud shell , fazer upload, pasta Após o upload, digite no cloud shell cd CloudFunction para entrar na pasta e digite o comando abaixo para implantar a cloud function

```
gcloud functions deploy load_data_cloudstorage_to_bigquery \
    --runtime python310 \
    --trigger-http \
    --allow-unauthenticated \
    --region southamerica-east1 \
    --entry-point load_data_cloudstorage_to_bigquery
```


## Criando Cloud Scheduler para Agendamento

### Criando Conta de Serviço
Crie uma conta de serviço com permissões específicas.

```
gcloud iam service-accounts create  account-tcc-facens-scheduler \
--display-name="scheduler tcc facens service account"

```

### Criando Role com Permissões para Conta de Serviço

Defina as permissões necessárias para a conta de serviço.

```bash
gcloud iam roles create customroletccfacensscheduler --project tccfacens2024 \
--title "TCC facens scheduler" \
--description "Custom role for scheduling Cloud Function to copy data from Cloud Storage to BigQuery and trigger a stored procedure" \
--permissions "cloudfunctions.functions.invoke,storage.objects.get,storage.objects.list,bigquery.jobs.create,bigquery.tables.get,iam.serviceAccounts.actAs" \
--stage ALPHA
```

### Adicionando Role de Permissões para Conta de Serviço

Adicione a role com permissões adequadas à conta de serviço.

```bash
gcloud projects add-iam-policy-binding tccfacens2024 \
--member=serviceAccount:account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com \
--role=projects/tccfacens2024/roles/customroletccfacensscheduler
```

### Criando Agendamentos Cloud Scheduler

Configure o Cloud Scheduler para realizar o agendamento e execução das Cloud Functions.



Cria o agendamento de carga para a função d_saude  

```
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_saude \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"tccfacens2024.TCC_Turismo_staging.d_Agua\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_agua-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Agua()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"AGUA_POPULACAO_ATEND_QT\"]}" \
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```


Cria o agendamento de carga para a função ensino_basico  


```
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_ensino_basico \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Ensino_Basico\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_ed_basica_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_Basico()\",\"tablefields\":[\"NO_MUNICIPIO\",\"QT_MAT_BAS\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```



Cria o agendamento de carga para a função ensino_superior 


```
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_ensino_superior \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Ensino_Superior\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_ed_superior_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_Superior()\",\"tablefields\":[\"NO_MUNICIPIO\",\"NO_CURSO\",\"NO_CINE_ROTULO\",\"NO_CINE_AREA_GERAL\",\"NO_CINE_AREA_DETALHADA\",\"NO_CINE_AREA_ESPECIFICA\",\"QT_MAT\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```



Cria o agendamento de carga para a função ensino_tecnico 



```
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_ensino_tecnico \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Ensino_tecnico\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_ed_tecnica_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_tecnico()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"UF\",\"ESTADO\",\"REGIAO\",\"MICRORREGIAO\",\"POP_TOTAL\",\"POP_URB\",\"FAIXA_POP\",\"DESC_FAIXA\",\"AREA_KM2\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```



Cria o agendamento de carga para a função infra turismo 



```
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_infra_Turismo \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_infra_Turismo\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_infra_turismo_novinho.csv\",\"query\":\"TCC_Turismo.merge_d_infra_Turismo()\",\"tablefields\":[\"NO_MUNICIPIO\",\"PROJETOS_MTUR\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_AGRICULTURA_E_PECUARIA\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_TURISMO\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_OUTROS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_SERVICOS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_COMERCIO\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_INDUSTIA_DE_BASE\",\"FUNDO_MUNICIPAL_TURISMO\",\"PLANO_MUNICIPAL_TURISMO\",\"PLANO_MKT_TURISMO\",\"PROJETOS_ATIVIDADE_TURISTICA\",\"HOSPEDAGEM_QT\",\"LEITOS_QT\",\"LOCADORAS_VEICULOS\",\"AGENCIAS_BANCARIAS_QT\",\"AEROPORTO\",\"TIPOS_TRANSPORTE\",\"TIPOS_TRANSPORTE_RODOVIARIO\",\"TIPOS_TRANSPORTE_AQUAVIARIO\",\"TIPOS_TRANSPORTE_AEREO\",\"TIPOS_TRANSPORTE_OUTROS\",\"ACESSO_DESTINO_TURISTICO\",\"ACESSO_DESTINO_TURISTICO_HIDROVIA\",\"ACESSO_DESTINO_TURISTICO_RODOVIA\",\"ACESSO_DESTINO_TURISTICO_AEROPORTO\",\"ACESSO_DESTINO_TURISTICO_FERROVIA\",\"ACESSO_DESTINO_TURISTICO_OUTROS\",\"SITUACAO_SINALIZACAO\",\"ROTA_TURISTICA\",\"INTERLIGACAO_ENTRE_ATRATIVOS\",\"SINALIZACAO_ACESSIBILIDADE\",\"RESERVA_ESPACO_ACESSIBILIDADE\",\"PROFISSIONAIS_ACESSIBILIDADE\",\"EMPRESAS_TURISMO_QT\",\"TIPOS_PATRIMONIO_NATURAL\",\"TIPOS_PATRIMONIO_NATURAL_PARQUES_NATURAIS\",\"TIPOS_PATRIMONIO_NATURAL_UNIDADE_DE_CONSERVACAO\",\"TIPOS_PATRIMONIO_NATURAL_RESERVAS_ECOLOGICAS\",\"TIPOS_PATRIMONIO_NATURAL_OUTROS\",\"UNIDADES_CONSERVACAO\",\"TIPOS_PATRIMONIO_CULTURAL\",\"PATRIMONIO_CULTURAL_HISTORICO\",\"PATRIMONIO_CULTURAL_EQUIPAMENTOS\",\"PATRIMONIO_CULTURAL_OUTROS\",\"BARCOS_TURISMO\",\"TURISMO_PESCA\",\"TURISMO_MERGULHO\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```





Cria o agendamento de carga para a função d_localizacao 



```
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_localizacao \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Localizacao\",\"uri\":\"gs://dados_tcc_facens_sp/output/tabela_d_populacao.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_Localizacao()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"UF\",\"ESTADO\",\"REGIAO\",\"MICRORREGIAO\",\"POP_TOTAL\",\"POP_URB\",\"FAIXA_POP\",\"DESC_FAIXA\",\"AREA_KM2\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```




Cria o agendamento de carga para a função d_regiao_turistica 

```

gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_Regiao_Turistica \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Regiao_Turistica\",\"uri\":\"gs://dados_tcc_facens_sp/output/tabela_d_regiao_turistica.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_Regiao_Turistica()\",\"tablefields\":[\"NO_MUNICIPIO\",\"REGIAO_TURISTICA\",\"CATEGORIA_TURISMO\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```





Cria o agendamento de carga para a função d_Saúde 

```
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_saude \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_saude\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_leitos.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_saude()\",\"tablefields\":[\"NO_MUNICIPIO\",\"LEITOS_QT\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```




Cria o agendamento de carga para a tabela Variável Dependente Visitantes (Tabela 1): 


```
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_variavel_dependente_visitantes_tabela_1 \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.variavel_dependente_visitantes_tabela_1\",\"uri\":\"gs://dados_tcc_facens_sp/output/variavel_dependente_visitantes_tabela_1.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_variavel_dependente_visitantes_tabela_1()\",\"tablefields\":[\"NO_MUNICIPIO\",\"IDHM\",\"PIB_PER_CAPITA\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```


Cria o agendamento de carga para a tabela Variável Dependente Visitantes (Tabela 2):



```
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_variavel_dependente_visitantes_tabela_2 \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.variavel_dependente_visitantes_tabela_2\",\"uri\":\"gs://dados_tcc_facens_sp/output/variavel_dependente_visitantes_tabela_2.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_variavel_dependente_visitantes_tabela_2()\",\"tablefields\":[\"NO_MUNICIPIO\",\"VISITAS_INTERNACIONAL_QT\",\"VISITAS_NACIONAL_QT\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
```
