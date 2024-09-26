
CREATE OR REPLACE PROCEDURE `tccfacens2024-435322.TCC_Turismo.merge_variavel_dependente_visitantes_tabela_2`()
BEGIN


MERGE `tccfacens2024-435322.TCC_Turismo.variavel_dependente_visitantes_tabela_2` tgt
USING (
  SELECT 
    NO_MUNICIPIO,
    cast(replace(VISITAS_INTERNACIONAL_QT,'.','') as integer) as VISITAS_INTERNACIONAL_QT,
    cast(replace(VISITAS_NACIONAL_QT,'.','') as integer) as VISITAS_NACIONAL_QT
FROM `tccfacens2024-435322.TCC_Turismo_staging.variavel_dependente_visitantes_tabela_2` ) as src
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
    
DELETE FROM `tccfacens2024-435322.TCC_Turismo_staging.variavel_dependente_visitantes_tabela_2`  WHERE 1 = 1;

END;