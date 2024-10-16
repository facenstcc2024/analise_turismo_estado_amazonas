CREATE OR REPLACE PROCEDURE `tccfacens2024-435322.TCC_Turismo.merge_d_saude`()
BEGIN


MERGE `tccfacens2024-435322.TCC_Turismo.d_saude` tgt
USING (SELECT NO_MUNICIPIO,
cast(LEITOS_QT as Integer) as LEITOS_QT
FROM `tccfacens2024-435322.TCC_Turismo_staging.d_saude` ) as src
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
    
DELETE FROM `tccfacens2024-435322.TCC_Turismo_staging.d_saude` WHERE 1 = 1;
END;