CREATE OR REPLACE PROCEDURE `tccfacens2024-435322.TCC_Turismo.merge_d_Localizacao`()
BEGIN


MERGE `tccfacens2024-435322.TCC_Turismo.d_Localizacao` tgt
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
  from `tccfacens2024-435322.TCC_Turismo_staging.d_Localizacao`
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
    
DELETE   from `tccfacens2024-435322.TCC_Turismo_staging.d_Localizacao` WHERE 1 = 1;
END;