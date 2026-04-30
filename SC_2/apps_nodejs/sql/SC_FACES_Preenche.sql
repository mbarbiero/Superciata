-- ***************************************************************
-- SC_FACES
-- SC_FACES
-- SC_FACES
-- ***************************************************************    
-- Remove dados antigos do município para evitar duplicidade
DELETE FROM SC_FACES WHERE COD_MUNICIPIO = '${COD_MUNICIPIO}';

-- Povoamento de SC_FACES com SC_QUADRAS e CN_FACES
INSERT INTO SC_FACES (
    SC_ID_FACE,
    COD_MUNICIPIO,
    SC_ID_QUADRA,
    SC_ID_LOGRADOURO,
    CN_ID_FACE,
    CENTROIDE,
    ORDEM_FACE
)
SELECT 
    -- Chave Primária: ID da Quadra concatenado ao ID do Logradouro
    CONCAT(Q.SC_ID_QUADRA, ';', CN.SC_ID_LOGRADOURO) AS SC_ID_FACE,
    Q.COD_MUNICIPIO,
    Q.SC_ID_QUADRA,
    CN.SC_ID_LOGRADOURO,
    CN.ID_FACE,
    CN.CENTROIDE,
    CN.NR_ORDEM
FROM CN_FACES CN
INNER JOIN SC_QUADRAS Q 
    ON CN.ID_QUADRA = Q.CN_ID_QUADRA 
    AND CN.COD_MUNICIPIO = Q.COD_MUNICIPIO
WHERE CN.COD_MUNICIPIO = '${COD_MUNICIPIO}';

-- Enriquecimento de SC_FACES com CI_FACES
UPDATE SC_FACES SC
INNER JOIN SC_QUADRAS Q 
	ON SC.SC_ID_QUADRA = Q.SC_ID_QUADRA
INNER JOIN CI_FACES CI 
	ON CI.COD_MUNICIPIO = SC.COD_MUNICIPIO 
	AND CI.SC_ID_LOGRADOURO = SC.SC_ID_LOGRADOURO
	AND CI.CI_ID_QUADRA = Q.CI_ID_QUADRA -- Crucial para garantir a face na quadra correta
SET 
    SC.DIM_FACE = CI.DIM_FACE,
    SC.QTD_LOTES = CI.QTD_LOTES,
    SC.CI_ID_FACE = CI.CI_ID_FACE
WHERE CI.COD_MUNICIPIO = '${COD_MUNICIPIO}';    

