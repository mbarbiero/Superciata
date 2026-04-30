-- ***************************************************************
-- SC_QUADRAS
-- SC_QUADRAS
-- SC_QUADRAS
-- ***************************************************************    
-- Remove dados antigos do município para evitar duplicidade
DELETE FROM SC_QUADRAS WHERE COD_MUNICIPIO = '${COD_MUNICIPIO}';

-- Atualizando as faces em SC_QUADRAS filtrando pelo município selecionado
INSERT INTO SC_QUADRAS (
    SC_ID_QUADRA,
    COD_MUNICIPIO,
    CI_ID_QUADRA,
    CN_ID_QUADRA,
    CENTROIDE
)
SELECT 
    cn.SC_ID_QUADRA,
    cn.COD_MUNICIPIO,
    ci.CI_ID_QUADRA,
    cn.ID_QUADRA AS CN_ID_QUADRA,
    cn.CENTROIDE
FROM CN_QUADRAS cn
INNER JOIN CI_QUADRAS ci 
    ON cn.SC_ID_QUADRA = ci.SC_ID_QUADRA 
    AND cn.COD_MUNICIPIO = ci.COD_MUNICIPIO
WHERE cn.COD_MUNICIPIO = '${COD_MUNICIPIO}'  -- Filtro crucial para isolar o processamento;
