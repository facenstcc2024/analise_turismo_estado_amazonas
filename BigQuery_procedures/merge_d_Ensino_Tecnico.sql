CREATE OR REPLACE PROCEDURE `tccfacens2024-435322.TCC_Turismo.merge_d_Ensino_Tecnico`()
BEGIN


MERGE `tccfacens2024-435322.TCC_Turismo.d_Ensino_tecnico` tgt
USING (
  SELECT 
    NO_MUNICIPIO,
    NO_AREA_CURSO_PROFISSIONAL,
    NO_CURSO_EDUC_PROFISSIONAL,
  cast(QT_MAT_CURSO_TEC as Integer) as QT_MAT_CURSO_TEC
  FROM `tccfacens2024-435322.TCC_Turismo_staging.d_Ensino_tecnico` ) as src
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
    
DELETE FROM `tccfacens2024-435322.TCC_Turismo_staging.d_Ensino_tecnico` WHERE 1 = 1;
END;