# analise_turismo_regiao_amazonica
repositório destinado ao trabalho de conclusão de curso sobre a análise de dados turísticos na região amazonica






############################### cloud function ###########################################


########################## Configurar o Projeto no GCP ############################# 
gcloud config set project [PROJECT_ID]


########################## Habilitar a API de Cloud Functions ############################# 
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com


##########################subir a pasta com o codigo da cloud function #######################
Faça o file upload da pasta CloudFunction para o cloud shell do google cloud platform,
isso pode ser feito facilmente acessando o botão mais no cloud shell , fazer upload, pasta
Após o upload, digite no cloud shell cd CloudFunction para entrar na pasta e digite o comando abaixo 
para implantar a cloud function



gcloud functions deploy load_data_cloudstorage_to_bigquery \
    --runtime python310 \
    --trigger-http \
    --allow-unauthenticated \
    --region southamerica-east1 \
    --entry-point load_data_cloudstorage_to_bigquery


############verificar criação da função #########################
gcloud functions describe load_data_cloudstorage_to_bigquery --region southamerica-east1


###########executar função no cloud shell  #####################
REGION_PROJECT_ID=southamerica-east1-tccfacens2024
curl -m 70 -X POST https://$REGION_PROJECT_ID.cloudfunctions.net/load_data_cloudstorage_to_bigquery \
-H "Content-Type: application/json" \
-d '{
    "table_id": "tccfacens2024.TCC_Turismo_staging.d_Agua",
    "uri": "gs://dados_tcc_facens_sp/output/d_agua-00000-of-00001.csv",
    "query":"TCC_Turismo.merge_d_Agua()",
    "tablefields": [
        "NO_MUNICIPIO",
        "ANO_REF",
        "AGUA_POPULACAO_ATEND_QT"
    ]
}'



########### conta de serviço para execução da cloud scheduler  #####################

gcloud iam service-accounts create  account-tcc-facens-scheduler \
--display-name="scheduler tcc facens service account"


########### cria a custon role  #####################

gcloud iam roles create customroletccfacensscheduler --project tccfacens2024 \
--title "TCC facens scheduler" \
--description "Custom role for scheduling Cloud Function to copy data from Cloud Storage to BigQuery and trigger a stored procedure" \
--permissions "cloudfunctions.functions.invoke,storage.objects.get,storage.objects.list,bigquery.jobs.create,bigquery.tables.get,iam.serviceAccounts.actAs" \
--stage ALPHA


########### associa a custon role com a service account #####################

gcloud projects add-iam-policy-binding tccfacens2024 \
--member=serviceAccount:account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com \
--role=projects/tccfacens2024/roles/customroletccfacensscheduler



gcloud scheduler jobs list --location southamerica-east1 


########### cria o agendamento de carga para a função d_saude  #####################

gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_saude \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"tccfacens2024.TCC_Turismo_staging.d_Agua\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_agua-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Agua()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"AGUA_POPULACAO_ATEND_QT\"]}" \
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"


########### cria o agendamento de carga para a função ensino_basico  #####################


gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_ensino_basico \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Ensino_Basico\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_ed_basica_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_Basico()\",\"tablefields\":[\"NO_MUNICIPIO\",\"QT_MAT_BAS\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"


--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Ensino_Basico\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_ed_basica_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_Basico()\",\"tablefields\":[\"NO_MUNICIPIO\",\"QT_MAT_BAS\"]}"


########### cria o agendamento de carga para a função ensino_superior  #####################


gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_ensino_superior \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Ensino_Superior\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_ed_superior_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_Superior()\",\"tablefields\":[\"NO_MUNICIPIO\",\"NO_CURSO\",\"NO_CINE_ROTULO\",\"NO_CINE_AREA_GERAL\",\"NO_CINE_AREA_DETALHADA\",\"NO_CINE_AREA_ESPECIFICA\",\"QT_MAT\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"


--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Ensino_Superior\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_ed_superior_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_Superior()\",\"tablefields\":[\"NO_MUNICIPIO\",\"NO_CURSO\",\"NO_CINE_ROTULO\",\"NO_CINE_AREA_GERAL\",\"NO_CINE_AREA_DETALHADA\",\"NO_CINE_AREA_ESPECIFICA\",\"QT_MAT\"]}"

########### cria o agendamento de carga para a função ensino_tecnico  #####################




gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_ensino_tecnico \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Ensino_tecnico\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_ed_tecnica_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_tecnico()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"UF\",\"ESTADO\",\"REGIAO\",\"MICRORREGIAO\",\"POP_TOTAL\",\"POP_URB\",\"FAIXA_POP\",\"DESC_FAIXA\",\"AREA_KM2\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"




--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Ensino_tecnico\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_ed_tecnica_lucas-00000-of-00001.csv\",\"query\":\"TCC_Turismo.merge_d_Ensino_tecnico()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"UF\",\"ESTADO\",\"REGIAO\",\"MICRORREGIAO\",\"POP_TOTAL\",\"POP_URB\",\"FAIXA_POP\",\"DESC_FAIXA\",\"AREA_KM2\"]}"


########### cria o agendamento de carga para a função infra turismo #####################



gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_infra_Turismo \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_infra_Turismo\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_infra_turismo_novinho.csv\",\"query\":\"TCC_Turismo.merge_d_infra_Turismo()\",\"tablefields\":[\"NO_MUNICIPIO\",\"PROJETOS_MTUR\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_AGRICULTURA_E_PECUARIA\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_TURISMO\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_OUTROS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_SERVICOS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_COMERCIO\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_INDUSTIA_DE_BASE\",\"FUNDO_MUNICIPAL_TURISMO\",\"PLANO_MUNICIPAL_TURISMO\",\"PLANO_MKT_TURISMO\",\"PROJETOS_ATIVIDADE_TURISTICA\",\"HOSPEDAGEM_QT\",\"LEITOS_QT\",\"LOCADORAS_VEICULOS\",\"AGENCIAS_BANCARIAS_QT\",\"AEROPORTO\",\"TIPOS_TRANSPORTE\",\"TIPOS_TRANSPORTE_RODOVIARIO\",\"TIPOS_TRANSPORTE_AQUAVIARIO\",\"TIPOS_TRANSPORTE_AEREO\",\"TIPOS_TRANSPORTE_OUTROS\",\"ACESSO_DESTINO_TURISTICO\",\"ACESSO_DESTINO_TURISTICO_HIDROVIA\",\"ACESSO_DESTINO_TURISTICO_RODOVIA\",\"ACESSO_DESTINO_TURISTICO_AEROPORTO\",\"ACESSO_DESTINO_TURISTICO_FERROVIA\",\"ACESSO_DESTINO_TURISTICO_OUTROS\",\"SITUACAO_SINALIZACAO\",\"ROTA_TURISTICA\",\"INTERLIGACAO_ENTRE_ATRATIVOS\",\"SINALIZACAO_ACESSIBILIDADE\",\"RESERVA_ESPACO_ACESSIBILIDADE\",\"PROFISSIONAIS_ACESSIBILIDADE\",\"EMPRESAS_TURISMO_QT\",\"TIPOS_PATRIMONIO_NATURAL\",\"TIPOS_PATRIMONIO_NATURAL_PARQUES_NATURAIS\",\"TIPOS_PATRIMONIO_NATURAL_UNIDADE_DE_CONSERVACAO\",\"TIPOS_PATRIMONIO_NATURAL_RESERVAS_ECOLOGICAS\",\"TIPOS_PATRIMONIO_NATURAL_OUTROS\",\"UNIDADES_CONSERVACAO\",\"TIPOS_PATRIMONIO_CULTURAL\",\"PATRIMONIO_CULTURAL_HISTORICO\",\"PATRIMONIO_CULTURAL_EQUIPAMENTOS\",\"PATRIMONIO_CULTURAL_OUTROS\",\"BARCOS_TURISMO\",\"TURISMO_PESCA\",\"TURISMO_MERGULHO\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"


--message-body "{\"table_id\":\"TCC_Turismo_staging.d_infra_Turismo\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_infra_turismo_novinho.csv\",\"query\":\"TCC_Turismo.merge_d_infra_Turismo()\",\"tablefields\":[\"NO_MUNICIPIO\",\"PROJETOS_MTUR\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_AGRICULTURA_E_PECUARIA\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_TURISMO\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_OUTROS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_SERVICOS\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_COMERCIO\",\"PRINCIPAIS_ATIVIDADES_ECONOMICAS_INDUSTIA_DE_BASE\",\"FUNDO_MUNICIPAL_TURISMO\",\"PLANO_MUNICIPAL_TURISMO\",\"PLANO_MKT_TURISMO\",\"PROJETOS_ATIVIDADE_TURISTICA\",\"HOSPEDAGEM_QT\",\"LEITOS_QT\",\"LOCADORAS_VEICULOS\",\"AGENCIAS_BANCARIAS_QT\",\"AEROPORTO\",\"TIPOS_TRANSPORTE\",\"TIPOS_TRANSPORTE_RODOVIARIO\",\"TIPOS_TRANSPORTE_AQUAVIARIO\",\"TIPOS_TRANSPORTE_AEREO\",\"TIPOS_TRANSPORTE_OUTROS\",\"ACESSO_DESTINO_TURISTICO\",\"ACESSO_DESTINO_TURISTICO_HIDROVIA\",\"ACESSO_DESTINO_TURISTICO_RODOVIA\",\"ACESSO_DESTINO_TURISTICO_AEROPORTO\",\"ACESSO_DESTINO_TURISTICO_FERROVIA\",\"ACESSO_DESTINO_TURISTICO_OUTROS\",\"SITUACAO_SINALIZACAO\",\"ROTA_TURISTICA\",\"INTERLIGACAO_ENTRE_ATRATIVOS\",\"SINALIZACAO_ACESSIBILIDADE\",\"RESERVA_ESPACO_ACESSIBILIDADE\",\"PROFISSIONAIS_ACESSIBILIDADE\",\"EMPRESAS_TURISMO_QT\",\"TIPOS_PATRIMONIO_NATURAL\",\"TIPOS_PATRIMONIO_NATURAL_PARQUES_NATURAIS\",\"TIPOS_PATRIMONIO_NATURAL_UNIDADE_DE_CONSERVACAO\",\"TIPOS_PATRIMONIO_NATURAL_RESERVAS_ECOLOGICAS\",\"TIPOS_PATRIMONIO_NATURAL_OUTROS\",\"UNIDADES_CONSERVACAO\",\"TIPOS_PATRIMONIO_CULTURAL\",\"PATRIMONIO_CULTURAL_HISTORICO\",\"PATRIMONIO_CULTURAL_EQUIPAMENTOS\",\"PATRIMONIO_CULTURAL_OUTROS\",\"BARCOS_TURISMO\",\"TURISMO_PESCA\",\"TURISMO_MERGULHO\"]}"


########### cria o agendamento de carga para a função d_localizacao #####################



gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_localizacao \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Localizacao\",\"uri\":\"gs://dados_tcc_facens_sp/output/tabela_d_populacao.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_Localizacao()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"UF\",\"ESTADO\",\"REGIAO\",\"MICRORREGIAO\",\"POP_TOTAL\",\"POP_URB\",\"FAIXA_POP\",\"DESC_FAIXA\",\"AREA_KM2\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"



--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Localizacao\",\"uri\":\"gs://dados_tcc_facens_sp/output/tabela_d_populacao.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_Localizacao()\",\"tablefields\":[\"NO_MUNICIPIO\",\"ANO_REF\",\"UF\",\"ESTADO\",\"REGIAO\",\"MICRORREGIAO\",\"POP_TOTAL\",\"POP_URB\",\"FAIXA_POP\",\"DESC_FAIXA\",\"AREA_KM2\"]}"


########### cria o agendamento de carga para a função d_regiao_turistica #####################


gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_Regiao_Turistica \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Regiao_Turistica\",\"uri\":\"gs://dados_tcc_facens_sp/output/tabela_d_regiao_turistica.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_Regiao_Turistica()\",\"tablefields\":[\"NO_MUNICIPIO\",\"REGIAO_TURISTICA\",\"CATEGORIA_TURISMO\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"




--message-body "{\"table_id\":\"TCC_Turismo_staging.d_Regiao_Turistica\",\"uri\":\"gs://dados_tcc_facens_sp/output/tabela_d_regiao_turistica.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_Regiao_Turistica()\",\"tablefields\":[\"NO_MUNICIPIO\",\"REGIAO_TURISTICA\",\"CATEGORIA_TURISMO\"]}"



########### cria o agendamento de carga para a função d_Saúde #####################


gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_d_saude \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.d_saude\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_leitos.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_saude()\",\"tablefields\":[\"NO_MUNICIPIO\",\"LEITOS_QT\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"


--message-body "{\"table_id\":\"TCC_Turismo_staging.d_saude\",\"uri\":\"gs://dados_tcc_facens_sp/output/d_leitos.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_d_saude()\",\"tablefields\":[\"NO_MUNICIPIO\",\"LEITOS_QT\"]}"


########### cria o agendamento de carga para a tabela Variável Dependente Visitantes (Tabela 1): #####################


gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_variavel_dependente_visitantes_tabela_1 \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.variavel_dependente_visitantes_tabela_1\",\"uri\":\"gs://dados_tcc_facens_sp/output/variavel_dependente_visitantes_tabela_1.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_variavel_dependente_visitantes_tabela_1()\",\"tablefields\":[\"NO_MUNICIPIO\",\"IDHM\",\"PIB_PER_CAPITA\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"



--message-body "{\"table_id\":\"TCC_Turismo_staging.variavel_dependente_visitantes_tabela_1\",\"uri\":\"gs://dados_tcc_facens_sp/output/variavel_dependente_visitantes_tabela_1.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_variavel_dependente_visitantes_tabela_1()\",\"tablefields\":[\"NO_MUNICIPIO\",\"IDHM\",\"PIB_PER_CAPITA\"]}"


########### cria o agendamento de carga para a tabela Variável Dependente Visitantes (Tabela 2): #####################



gcloud scheduler jobs create http load_data_cloudstorage_to_bigquery_variavel_dependente_visitantes_tabela_2 \
--schedule "00 08 * * *" \
--uri "https://southamerica-east1-tccfacens2024.cloudfunctions.net/load_data_cloudstorage_to_bigquery" \
--http-method "POST" \
--location=southamerica-east1  \
--headers "Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
--time-zone "America/Sao_Paulo" \
--message-body "{\"table_id\":\"TCC_Turismo_staging.variavel_dependente_visitantes_tabela_2\",\"uri\":\"gs://dados_tcc_facens_sp/output/variavel_dependente_visitantes_tabela_2.csv-00000-of-00001\",\"query\":\"TCC_Turismo.merge_variavel_dependente_visitantes_tabela_2()\",\"tablefields\":[\"NO_MUNICIPIO\",\"VISITAS_INTERNACIONAL_QT\",\"VISITAS_NACIONAL_QT\"]}"
--oidc-service-account-email "account-tcc-facens-scheduler@tccfacens2024.iam.gserviceaccount.com"
