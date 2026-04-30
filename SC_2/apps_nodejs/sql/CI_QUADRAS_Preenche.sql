-- ***************************************************************
-- CI_QUADRAS
-- CI_QUADRAS
-- CI_QUADRAS
-- ***************************************************************    
-- Remove dados antigos do município para evitar duplicidade
DELETE FROM CI_QUADRAS WHERE COD_MUNICIPIO = '${COD_MUNICIPIO}';

-- Atualizando as faces em CI_FACES filtrando pelo município selecionado
INSERT INTO CI_QUADRAS (
  CI_ID_QUADRA, COD_MUNICIPIO, SC_ID_QUADRA, 
  QTD_LOTES, AREA
)
SELECT 
  CI_ID_QUADRA,
  COD_MUNICIPIO,
  CONCAT('[', 
      GROUP_CONCAT(SC_ID_LOGRADOURO 
        ORDER BY SC_ID_LOGRADOURO 
        SEPARATOR ','), 
  ']') AS SC_ID_QUADRA,
  SUM(QTD_LOTES) AS QTD_LOTES,
  0.00 AS AREA
FROM CI_FACES
WHERE COD_MUNICIPIO = '${COD_MUNICIPIO}'
GROUP BY 
  COD_MUNICIPIO, 
  CI_ID_QUADRA
    
    
