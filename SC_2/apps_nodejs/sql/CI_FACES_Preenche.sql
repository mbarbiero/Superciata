-- ***************************************************************
-- CI_FACES
-- CI_FACES
-- CI_FACES
-- ***************************************************************    
-- Remove dados antigos do município para evitar duplicidade
DELETE FROM CI_FACES WHERE COD_MUNICIPIO = '${COD_MUNICIPIO}';

-- Atualizando as faces em CI_FACES filtrando pelo município selecionado
INSERT INTO CI_FACES (
    CI_ID_FACE, 
    COD_MUNICIPIO, 
    CI_ID_QUADRA, 
    SC_ID_LOGRADOURO, 
    QTD_LOTES, 
    DIM_FACE
) 
SELECT 
    CONCAT(LT.COD_MUNICIPIO, ";", LT.NUM_QUADRA, ";", LG.SC_ID_LOGRADOURO) AS CI_ID_FACE,
    LT.COD_MUNICIPIO, 
    LT.NUM_QUADRA AS CI_ID_QUADRA, 
    LG.SC_ID_LOGRADOURO,
    COUNT(*) AS QTD_LOTES, 
    SUM(LT.DIM_TESTADA) AS DIM_FACE
FROM CI_LOTES LT
JOIN CI_LOGRADOUROS LG 
    ON (LT.NOM_LOGRADOURO = LG.CI_NOM_LOGRADOURO)
    AND (LT.COD_MUNICIPIO = LG.COD_MUNICIPIO)
WHERE LT.COD_MUNICIPIO = '${COD_MUNICIPIO}'  -- Filtro crucial para isolar o processamento
GROUP BY 
    LT.COD_MUNICIPIO, 
    LT.NUM_QUADRA, 
    LG.SC_ID_LOGRADOURO;
    
    
-- ***************************************************************
-- CI_ADJACENTES
-- CI_ADJACENTES
-- CI_ADJACENTES
-- ***************************************************************    
-- Remove dados antigos do município para evitar duplicidade
DELETE FROM CI_ADJACENTES WHERE COD_MUNICIPIO = '${COD_MUNICIPIO}';

-- Povoamento de CI_ADJACENTES filtrando pelo município correto
INSERT INTO CI_ADJACENTES (
  CI_ID_FACE, 
  COD_MUNICIPIO, 
  CI_ID_QUADRA, 
  SC_ID_LOGRADOURO, 
  DIM_ADJACENTE
)
SELECT 
  CONCAT(LT.COD_MUNICIPIO, ";", LT.NUM_QUADRA, ";", LG.SC_ID_LOGRADOURO) AS CI_ID_FACE,
  LT.COD_MUNICIPIO, 
  LT.NUM_QUADRA AS ID_QUADRA, 
  LG.SC_ID_LOGRADOURO,
  SUM(LT.DIM_PROFUNDIDADE) AS DIM_ADJACENTE
FROM (
  SELECT  
    COD_MUNICIPIO, 
    NUM_QUADRA, 
    NOM_LOGRADOURO_ADJACENTE, 
    COD_UNICO_ENDERECO, 
    DIM_PROFUNDIDADE
  FROM CI_LOTES
  WHERE NOM_LOGRADOURO_ADJACENTE <> ""
    AND COD_MUNICIPIO = '${COD_MUNICIPIO}' -- Filtro na origem dos lotes
) LT
JOIN CI_LOGRADOUROS LG 
  ON (LT.NOM_LOGRADOURO_ADJACENTE = LG.CI_NOM_LOGRADOURO)
  AND (LT.COD_MUNICIPIO = LG.COD_MUNICIPIO)
GROUP BY 
  LT.COD_MUNICIPIO, 
  LT.NUM_QUADRA, 
  LG.SC_ID_LOGRADOURO;

-- Atualização de CI_FACES apenas para o município ativo
UPDATE CI_FACES F
INNER JOIN (
    -- Agrupamos os valores filtrando por município para otimizar a performance
    SELECT 
        CI_ID_FACE, 
        SUM(DIM_ADJACENTE) AS TOTAL_ADJACENTE
    FROM CI_ADJACENTES
    WHERE COD_MUNICIPIO = '${COD_MUNICIPIO}' -- Filtro para evitar processar a tabela toda
    GROUP BY CI_ID_FACE
) A ON F.CI_ID_FACE = A.CI_ID_FACE
SET F.DIM_FACE = F.DIM_FACE + A.TOTAL_ADJACENTE
WHERE F.COD_MUNICIPIO = '${COD_MUNICIPIO}'; -- Garante que apenas as faces deste projeto sejam alteradas