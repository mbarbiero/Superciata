-- teste de validade das coordenadas
SELECT 
    COUNT(*) AS total_registros,
    COUNT(CASE WHEN LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL THEN 1 END) AS com_coordenadas
FROM `smuu`.`cnefe`;

-- cálculo dos centroides para cada face/quadra
CREATE TEMPORARY TABLE temp_centroides AS
SELECT 
    CONCAT(COD_SETOR, NUM_QUADRA) AS ID_QUADRA,
    NUM_FACE,
    AVG(LATITUDE) AS latitude_centroide,
    AVG(LONGITUDE) AS longitude_centroide,
    COUNT(*) AS total_enderecos,
    ST_PointFromText(CONCAT('POINT(', AVG(LONGITUDE), ' ', AVG(LATITUDE), ')'), 4326) AS COORDS_CENTROIDE
FROM `smuu`.`cnefe`
WHERE 
    LATITUDE IS NOT NULL 
    AND LONGITUDE IS NOT NULL
GROUP BY 
    ID_QUADRA, 
    NUM_FACE;
    
-- mostra dados gerados
SELECT *
FROM
	temp_centroides
LIMIT
	20;
    
-- Atualizar os registros existentes
SET SQL_SAFE_UPDATES = 0;	-- Desativa o safe update mode apenas para esta sessão
UPDATE 
	CNEFE_FACES cf
JOIN 
	temp_centroides tc ON cf.NUM_QUADRA = tc.NUM_QUADRA AND cf.NUM_FACE = tc.NUM_FACE
SET 
    cf.LATITUDE_CENTROIDE = tc.latitude_centroide,
    cf.LONGITUDE_CENTROIDE = tc.longitude_centroide,
    cf.geom = tc.geom_centroide
WHERE
	cf.id > 16000
;
SET SQL_SAFE_UPDATES = 1;	-- Reativa o safe update mode

-- CENTROIDE DAS QUADRAS
-- cálculo dos centroides para cada quadra
CREATE TEMPORARY TABLE temp_centroides AS
SELECT 
    NUM_QUADRA,
    AVG(LATITUDE) AS latitude_centroide,
    AVG(LONGITUDE) AS longitude_centroide,
    COUNT(*) AS total_enderecos,
    ST_PointFromText(CONCAT('POINT(', AVG(LONGITUDE), ' ', AVG(LATITUDE), ')'), 4326) AS geom_centroide
FROM `smuu`.`cnefe`
WHERE 
    LATITUDE IS NOT NULL 
    AND LONGITUDE IS NOT NULL
GROUP BY 
    NUM_QUADRA
;
    
-- mostra dados gerados
SELECT 
	*
FROM
	temp_centroides
LIMIT
	20;
    
-- Atualizar os registros existentes
SET SQL_SAFE_UPDATES = 0;	-- Desativa o safe update mode apenas para esta sessão
UPDATE 
	CNEFE_QUADRAS cq
JOIN 
	temp_centroides tc ON cq.NUM_QUADRA = tc.NUM_QUADRA
SET 
    cq.LATITUDE_CENTROIDE = tc.latitude_centroide,
    cq.LONGITUDE_CENTROIDE = tc.longitude_centroide,
    cq.geom = tc.geom_centroide
;
SET SQL_SAFE_UPDATES = 1;	-- Reativa o safe update mode


-- Usa os pontos centrais das faces para gerar um poligono nas quadras
SET SQL_SAFE_UPDATES = 0;	-- Desativa o safe update mode apenas para esta sessão
UPDATE CNEFE_QUADRAS q
JOIN (
  SELECT 
    NUM_QUADRA,
    ST_ConvexHull(
      ST_Collect(
        ST_GeomFromText(
          CONCAT('POINT(', LONGITUDE_CENTROIDE, ' ', LATITUDE_CENTROIDE, ')')
        )
      )
    ) AS geom_poly
  FROM CNEFE_FACES
  WHERE LATITUDE_CENTROIDE IS NOT NULL AND LONGITUDE_CENTROIDE IS NOT NULL
  GROUP BY NUM_QUADRA
) f
ON q.NUM_QUADRA = f.NUM_QUADRA
SET q.geom = f.geom_poly;

SHOW GRANTS FOR 'smuu_add2';

