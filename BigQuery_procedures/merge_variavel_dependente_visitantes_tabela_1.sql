CREATE OR REPLACE PROCEDURE `tccfacens2024-435322.TCC_Turismo.merge_variavel_dependente_visitantes_tabela_1`()
BEGIN


MERGE `tccfacens2024-435322.TCC_Turismo.variavel_dependente_visitantes_tabela_1` tgt
USING (
  SELECT 
    NO_MUNICIPIO,
    cast(IDHM as decimal) as IDHM,
    cast(PIB_PER_CAPITA as decimal) as PIB_PER_CAPITA
FROM `tccfacens2024-435322.TCC_Turismo_staging.variavel_dependente_visitantes_tabela_1` ) as src
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
    
DELETE FROM `tccfacens2024-435322.TCC_Turismo_staging.variavel_dependente_visitantes_tabela_1`  WHERE 1 = 1;

END;