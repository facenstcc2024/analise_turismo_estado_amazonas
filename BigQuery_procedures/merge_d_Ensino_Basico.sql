

CREATE OR REPLACE PROCEDURE `tccfacens2024-435322.TCC_Turismo.merge_d_Ensino_Basico`()
BEGIN


MERGE `tccfacens2024-435322.TCC_Turismo.d_Ensino_Basico` tgt
USING (SELECT NO_MUNICIPIO,
cast(QT_MAT_BAS as Integer) as QT_MAT_BAS
FROM `tccfacens2024-435322.TCC_Turismo_staging.d_Ensino_Basico` ) as src
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
    
DELETE FROM `tccfacens2024-435322.TCC_Turismo_staging.d_Ensino_Basico`  WHERE 1 = 1;
END;

