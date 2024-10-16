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

O google cloud storage é a solução para armazenamento de dados na nuvem , salvando so dados em servidores externos e distribuidos.
O serviço é escalavel, com alta disponibilidade e seguro, perfeito para o armazenamento de dados de aplicativos, objeto e dados não estruturados.
O serviço do google clou storage oferece diferentes classes de armazenamento de dados, com diferentes preços, possibilitando menores
precos de armazenamento para objetos com pouco acesso.

- **Google Big Query**  

Serviço de  datawarehouse do google, totalmente gerenciada e sem servidor,
 trabalhando com dados na escala de peyabytes, com a possibilidade de trabalhar com dados estrututados e semi estruturados, onde as consultas são feitas com linguagem SQL like.
O Big query pode ser facilmente utilizado com diversas outras ferramentas, seja dentro do google cloud platform ou fora dele. O Big query pode ser utilizados para processamentos de dados em Lote ou em tempo real, alem de ter suporte para machine learning utiliza linguagem proxima ao SQL.


- **Cloud Functions**  

O Google Cloud Functions é a solução de computação sem servidor do google, possibilitando o desenvolvimento e execução de código sem provisão de servidor , sendo escalavel e flexivel.
As cloud functions são orientadas a eventos como requisições HTTP, alterações em bancos de dados, novos arquivos num storage ou mensagens num
topico de mensageria.
Com suporte a várias linguagens de programação, fácil integração entre serviçosas cloud functions são ótimas opções para desenvolvimento de aplicações modernas.

- **Cloud Scheduler**  

O Google Cloud Scheduler é a solução para agendamento de tarefas do google.
Indicado para automação de tarefas, envio de emails, carga e recebimento de dados, backups de dados e bancos de dados entre outros.
Compativel com a maioria dos serviços google, o serviço do google cloud scheduler é uma opção fácil para agendamento, automatização e execução .de tarefas.

## Montando o Ambiente no GCP

### Criando Conta no GCP

Você precisará de uma conta no Google Cloud Platform.

### Como Executar os Scripts

Para a criação de todos os elementos utilizados no projetos, foram utilizados scripts, possibilidtando a fácil replicação e possivel automatização da criação dos elementos.
As variaveis abaixo contem informações como id_projeto, região zone, nome do storage, entre outras.
para reproduzir o experimento basta substituir os valores das variaveis abaixo pelo sid do seu projeto as demais informações, e copiar e colar as variaveis na interface de linha de comandos do GCP, após isso basta seguir as instruções para cada tipo de elemento criado.
Caso o cloud shell fique desconecte for inatividade , é necessários criar novamente as variveis acima

```bash
PROJECT_ID=tccfacens2024-435322
REGION=southamerica-east1
ZONE=southamerica-east1-a
GCP_BUCKET_TCC_FACENS=dados_tcc_facens_final
GCP_BUCKET_TCC_FACENS_SP=dados_tcc_facens_sp_final
INPUT_FOLDER=inputs
OUTPUT_FOLDER=output
GS_PREFIX=gs://


DATASET_TCC_STAGING=TCC_Turismo_staging
DATASET_TCC=TCC_Turismo


PROJECT_ID=tccfacens2024-435322
REGION=southamerica-east1



SERVICE_ACCOUNT_NAME=account-tcc-facens-scheduler
SERVICE_ACCOUNT_DESCRIPTION=scheduler_tcc_facens_service_account
```

## Criando Buckets no Cloud Storage

### Criando Buckets

```bash
gcloud storage buckets create gs://$GCP_BUCKET_TCC_FACENS --default-storage-class=nearline --location=$REGION

gcloud storage buckets create gs://$GCP_BUCKET_TCC_FACENS_SP --default-storage-class=nearline --location=$REGION
```

### Copiando Arquivos CSV de Input

faça o file upload da pasta inputs para console GCP 

```bash
unzip inputs/inputs.zip -d inputs
```

```bash
TARGET=$INPUT_FOLDER
BUCKET_PATH=$GCP_BUCKET_TCC_FACENS_SP/$INPUT_FOLDER
gsutil cp -r $TARGET gs://$BUCKET_PATH
```
## Processando Dados com Google Dataflow

### Criando Script Dataflow

Crie o script de processamento utilizando Google Dataflow.

## Criando os Datasets BigQuery


```bash
bq --location=$REGION mk \
--default_table_expiration 0 \
--dataset $DATASET_TCC_STAGING
```

```bash
bq --location=$REGION mk \
--default_table_expiration 0 \
--dataset $DATASET_TCC
```


### Criando o Dataset TCC_Turismo_staging e Suas Tabelas


abrir console gcp
abrir clod shell
opções do cloud shell, upload pasta BigQuerySchema_staging
rodar os comando abaixo

```bash
bq mk --table \
$DATASET_TCC_STAGING.d_Agua \
BigQuerySchema_staging/d_Agua.json
```bash

```bash
bq mk --table \
$DATASET_TCC_STAGING.d_Ensino_Basico \
BigQuerySchema_staging/d_Ensino_Basico.json
```bash

```bash
bq mk --table \
$DATASET_TCC_STAGING.d_Ensino_Superior \
BigQuerySchema_staging/d_Ensino_Superior.json
```bash

```bash
bq mk --table \
$DATASET_TCC_STAGING.d_Ensino_tecnico \
BigQuerySchema_staging/d_Ensino_tecnico.json
```bash

```bash
bq mk --table \
$DATASET_TCC_STAGING.d_infra_Turismo \
BigQuerySchema_staging/d_infra_Turismo.json
```
```bash
bq mk --table \
$DATASET_TCC_STAGING.d_Localizacao \
BigQuerySchema_staging/d_Localizacao.json
```

```bash
bq mk --table \
$DATASET_TCC_STAGING.d_Regiao_Turistica \
BigQuerySchema_staging/d_Regiao_Turistica.json

____________________________________________
bq mk --table \
$DATASET_TCC_STAGING.d_saude \
BigQuerySchema_staging/d_saude.json
```

```bash
bq mk --table \
$DATASET_TCC_STAGING.variavel_dependente_visitantes_tabela_1 \
BigQuerySchema_staging/variavel_dependente_visitantes_tabela_1.json
```

```bash
bq mk --table \
$DATASET_TCC_STAGING.variavel_dependente_visitantes_tabela_2 \
BigQuerySchema_staging/variavel_dependente_visitantes_tabela_2.json
```

### Criando o Dataset TCC_Turismo e Suas Tabelas

abrir console gcp
abrir clod shell
opções do cloud shell, upload pasta BigQuerySchema
rodar os comando abaixo

```bash
bq mk --table \
$DATASET_TCC.d_Agua \
BigQuerySchema/d_Agua.json
```

```bash
bq mk --table \
$DATASET_TCC.d_Ensino_Basico \
BigQuerySchema/d_Ensino_Basico.json
```

```bash
bq mk --table \
$DATASET_TCC.d_Ensino_Superior \
BigQuerySchema/d_Ensino_Superior.json
```

```bash
bq mk --table \
$DATASET_TCC.d_Ensino_tecnico \
BigQuerySchema/d_Ensino_tecnico.json
```

```bash
bq mk --table \
$DATASET_TCC.d_infra_Turismo \
BigQuerySchema/d_infra_Turismo.json
```

```bash
bq mk --table \
$DATASET_TCC.d_Localizacao \
BigQuerySchema/d_Localizacao.json
```

```bash
bq mk --table \
$DATASET_TCC.d_Regiao_Turistica \
BigQuerySchema/d_Regiao_Turistica.json
```

```bash
bq mk --table \
$DATASET_TCC.d_saude \
BigQuerySchema/d_saude.json
```

```bash
bq mk --table \
$DATASET_TCC.variavel_dependente_visitantes_tabela_1 \
BigQuerySchema/variavel_dependente_visitantes_tabela_1.json
```

```bash
bq mk --table \
$DATASET_TCC.variavel_dependente_visitantes_tabela_2 \
BigQuerySchema/variavel_dependente_visitantes_tabela_2.json

```

### Criando as Procedures BigQuery de Merge

Apos Abrir o Big Query, no menu vertical a esquerdam clique na primeira opção big query Studio, e abra uma aba de consulta SQL .
Na aba de consulta SQL executar cada bloco de codigo abaixo,
para criar as procedures de carga e tratamento de dados das tabelas intermediarias para as tabelas finais

Faça upload da pasta BigQuery_procedures para o cloud shell do GCP.
Digite os comando abaixo, certifiquesse de estar no diretório home do cloud shell, na duvida
digite comando cd ~ , para voltar ao diretorio home .


```bash
bq query --use_legacy_sql=false < BigQuery_procedures/merge_d_Agua.sql

bq query --use_legacy_sql=false < BigQuery_procedures/merge_d_Ensino_Basico.sql

bq query --use_legacy_sql=false < BigQuery_procedures/merge_d_Ensino_Tecnico.sql

bq query --use_legacy_sql=false < BigQuery_procedures/merge_d_infra_Turismo.sql

bq query --use_legacy_sql=false < BigQuery_procedures/merge_d_Localizacao.sql

bq query --use_legacy_sql=false < BigQuery_procedures/merge_d_Regiao_Turistica.sql

bq query --use_legacy_sql=false < BigQuery_procedures/merge_d_saude.sql

bq query --use_legacy_sql=false < BigQuery_procedures/merge_variavel_dependente_visitantes_tabela_1.sql

bq query --use_legacy_sql=false < BigQuery_procedures/merge_variavel_dependente_visitantes_tabela_2.sql
```


## Criando Cloud Function para Carga e Merge

### Ativando os Serviços Necessários

Ative os serviços no GCP necessários para o funcionamento da Cloud Function, para isso digite os comando abaixo no shell do google cloud e aguarde alguns minutos até que os serviços estejam habilitados

```bash
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com
```

### Criando e Fazendo Deploy da Cloud Function

Faça o file upload da pasta CloudFunction_dinamica para o cloud shell do google cloud platform,isso pode ser feito facilmente acessando o botão mais no cloud shell , fazer upload, pasta Após o upload, digite no cloud shell cd CloudFunction_dinamica para entrar na pasta e digite o comando abaixo para implantar a cloud function

```bash
cd CloudFunction_dinamica

gcloud functions deploy load_data_cloudstorage_to_bigquery \
    --runtime python310 \
    --trigger-http \
    --allow-unauthenticated \
    --trigger-http --allow-unauthenticated \
    --region $REGION \
    --entry-point load_data_cloudstorage_to_bigquery
```
verifique se a função foi criada

```bash
gcloud functions describe load_data_cloudstorage_to_bigquery --region $REGION
```

caso deseje executar a função criada, execute o trecho abaixo

obs : ao usar variaveis dentro de um json, utilize "" ao invés de '' , para expandir corretamente as variaveis

```bash
curl -m 70 -X POST https://$REGION_PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery \
-H "Content-Type: application/json" \
-d "{
    \"table_id\": \"$TABLE_ID\",
    \"uri\": \"$URI\",
    \"query\": \"TCC_Turismo.merge_d_Agua()\",
    \"tablefields\": [
        \"NO_MUNICIPIO\",
        \"ANO_REF\",
        \"AGUA_POPULACAO_ATEND_QT\"
    ]
}"
```

## Criando Cloud Scheduler para Agendamento

### Criando Conta de Serviço
Crie uma conta de serviço com permissões específicas.

```bash
SERVICE_ACCOUNT_NAME=account-tcc-facens-scheduler
SERVICE_ACCOUNT_DESCRIPTION=scheduler_tcc_facens_service_account

gcloud iam service-accounts create  $SERVICE_ACCOUNT_NAME \
--display-name=$SERVICE_ACCOUNT_DESCRIPTION

```

### Criando Role com Permissões para Conta de Serviço

Defina as permissões necessárias para a conta de serviço.

```bash
CUSTOM_ROLE_NAME=customroletccfacensscheduler
CUSTOM_ROLE_TITLE="TCC facens scheduler"
CUSTOM_ROLE_DESCRIPTION="Custom role for scheduling Cloud Function to copy data from Cloud Storage to BigQuery and trigger a stored procedure"
CUSTOM_ROLE_PERMISSIONS="cloudfunctions.functions.invoke,storage.objects.get,storage.objects.list,bigquery.jobs.create,bigquery.tables.get,iam.serviceAccounts.actAs"

gcloud iam roles create $CUSTOM_ROLE_NAME --project $PROJECT_ID \
--title "$CUSTOM_ROLE_TITLE" \
--description "$CUSTOM_ROLE_DESCRIPTION" \
--permissions "$CUSTOM_ROLE_PERMISSIONS"

```

### Adicionando Role de Permissões para Conta de Serviço

Adicione a role com permissões adequadas à conta de serviço.

```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
--member=serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com \
--role=projects/$PROJECT_ID/roles/$CUSTOM_ROLE_NAME
```

### Criando Agendamentos Cloud Scheduler

Configure o Cloud Scheduler para realizar o agendamento e execução das Cloud Functions.



Cria o agendamento de carga para a função d_saude  

```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_ensino_basico \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=$REGION  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.d_Ensino_Basico\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/d_ed_basica_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_Basico()\",\"tablefields\":[\"NO_MUNICIPIO\",\"QT_MAT_BAS\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```

Agendamento de carga para a função ensino_superior
```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_ensino_superior \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=$REGION  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.d_Ensino_Superior\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/d_ed_superior_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_Superior()\",\"tablefields\":[\"NO_MUNICIPIO\",\"NO_CURSO\",\"NO_CINE_ROTULO\",\"NO_CINE_AREA_GERAL\",\"NO_CINE_AREA_DETALHADA\",\"NO_CINE_AREA_ESPECIFICA\",\"QT_MAT\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```

Agendamento de carga para a função ensino_tecnico
```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_ensino_tecnico \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=$REGION  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.d_Ensino_tecnico\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/d_ed_tecnica_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_tecnico()\",\"tablefields\":[\"NO_MUNICIPIO\",\"NO_AREA_CURSO_PROFISSIONAL\",\"NO_CURSO_EDUC_PROFISSIONAL\",\"QT_MAT_CURSO_TEC\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```




Agendamento de carga para a função infra_turismo
```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_infra_Turismo \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=$REGION  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.d_infra_Turismo\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/d_infra_turismo_novinho.csv\",\"query\":\"TCC_Turismo.merge_d_infra_Turismo()\",\"tablefields\":[\"NO_MUNICIPIO\",\"PROJETOS_MTUR\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_AGRICULTURA_E_PECUARIA\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_TURISMO\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_OUTROS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_SERVICOS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_COMERCIO\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_INDUSTRIA_DE_BASE\",\"FUNDO_MUNICIPAL_TURISMO\",\"PLANO_MUNICIPAL_TURISMO\",\"PLANO_MKT_TURISMO\",\"PROJETOS_ATIVIDADE_TURISTICA\",\"HOSPEDAGEM_QT\",\"LEITOS_QT\",\"LOCADORAS_VEICULOS\",\"AGENCIAS_BANCARIAS_QT\",\"AEROPORTO\",\"TIPOS_TRANSPORTE\",\"TIPOS_TRANSPORTE_RODOVIARIO\",\"TIPOS_TRANSPORTE_AQUAVIARIO\",\"TIPOS_TRANSPORTE_AEREO\",\"TIPOS_TRANSPORTE_OUTROS\",\"ACESSO_DESTINO_TURISTICO\",\"ACESSO_DESTINO_TURISTICO_HIDROVIA\",\"ACESSO_DESTINO_TURISTICO_RODOVIA\",\"ACESSO_DESTINO_TURISTICO_AEROPORTO\",\"ACESSO_DESTINO_TURISTICO_FERROVIA\",\"ACESSO_DESTINO_TURISTICO_OUTROS\",\"SITUACAO_SINALIZACAO\",\"ROTA_TURISTICA\",\"INTERLIGACAO_ENTRE_ATRATIVOS\",\"SINALIZACAO_ACESSIBILIDADE\",\"RESERVA_ESPACO_ACESSIBILIDADE\",\"PROFISSIONAIS_ACESSIBILIDADE\",\"EMPRESAS_TURISMO_QT\",\"TIPOS_PATRIMONIO_NATURAL\",\"TIPOS_PATRIMONIO_NATURAL_PARQUES_NATURAIS\",\"TIPOS_PATRIMONIO_NATURAL_UNIDADE_DE_CONSERVACAO\",\"TIPOS_PATRIMONIO_NATURAL_RESERVAS_ECOLOGICAS\",\"TIPOS_PATRIMONIO_NATURAL_OUTROS\",\"UNIDADES_CONSERVACAO\",\"TIPOS_PATRIMONIO_CULTURAL\",\"PATRIMONIO_CULTURAL_HISTORICO\",\"PATRIMONIO_CULTURAL_EQUIPAMENTOS\",\"PATRIMONIO_CULTURAL_OUTROS\",\"BARCOS_TURISMO\",\"TURISMO_PESCA\",\"TURISMO_MERGULHO\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```


Agendamento de carga para a função d_localizacao
```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_localizacao \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=$REGION  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.d_Localizacao\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/tabela_d_populacao.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_Localizacao()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"UF\",\"ESTADO\",\"REGIAO\",\"MICRORREGIAO\",\"POP_TOTAL\",\"POP_URB\",\"FAIXA_POP\",\"DESC_FAIXA\",\"AREA_KM2\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```

Agendamento de carga para a função d_regiao_turistica
```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_Regiao_Turistica \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=$REGION  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.d_Regiao_Turistica\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/tabela_d_regiao_turistica.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_Regiao_Turistica()\",\"tablefields\":[\"NO_MUNICIPIO\",\"REGIAO_TURISTICA\",\"CATEGORIA_TURISMO\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```



Agendamento de carga para a função d_saude

```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_saude \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.d_saude\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/d_leitos.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_saude()\",\"tablefields\":[\"NO_MUNICIPIO\",\"LEITOS_QT\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```

Agendamento de carga para a função variavel_dependente_visitantes_tabela_1

```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_variavel_dependente_visitantes_tabela_1 \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.variavel_dependente_visitantes_tabela_1\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/variavel_dependente_visitantes_tabela_1.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_variavel_dependente_visitantes_tabela_1()\",\"tablefields\":[\"NO_MUNICIPIO\",\"IDHM\",\"PIB_PER_CAPITA\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```


Agendamento de carga para a função variavel_dependente_visitantes_tabela_2


```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_variavel_dependente_visitantes_tabela_2 \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.variavel_dependente_visitantes_tabela_2\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/variavel_dependente_visitantes_tabela_2.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_variavel_dependente_visitantes_tabela_2()\",\"tablefields\":[\"NO_MUNICIPIO\",\"VISITAS_INTERNACIONAL_QT\",\"VISITAS_NACIONAL_QT\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```

Agendamento de carga para a função d_agua

```bash
gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_agua \
--schedule "00 08 * * *" \
--uri "https://$REGION-$PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=$REGION  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"$PROJECT_ID.$DATASET_TCC_STAGING.d_Agua\",\"uri\":\"$GS_PREFIX$GCP_BUCKET_TCC_FACENS_SP/$OUTPUT_FOLDER/d_agua-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Agua()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"AGUA_POPULACAO_ATEND_QT\"]}" \
--oidc-service-account-email "$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```
