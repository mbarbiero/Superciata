
-- NOVA VERSÃO 
-- ***************************************************************
-- SC_QUADRAS
-- SC_QUADRAS
-- SC_QUADRAS
-- ***************************************************************
-- Criação da tabela SC_QUADRAS
CREATE TABLE IF NOT EXISTS SC_QUADRAS (
	SC_ID_QUADRA 		VARCHAR(250),
	COD_MUNICIPIO 	  	VARCHAR(7),
	CI_ID_QUADRA       	VARCHAR(50),
	CN_ID_QUADRA        VARCHAR(19),
    CENTROIDE           POINT
);

-- Povoamento de SC_QUADRAS
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
    ci.ID_QUADRA AS CI_ID_QUADRA,
    cn.ID_QUADRA AS CN_ID_QUADRA,
    cn.CENTROIDE
FROM CN_QUADRAS cn
INNER JOIN CI_QUADRAS ci 
    ON cn.SC_ID_QUADRA = ci.SC_ID_QUADRA 
    AND cn.COD_MUNICIPIO = ci.COD_MUNICIPIO;
    
-- ***************************************************************
-- ***   SC_FACES
-- ***   SC_FACES
-- ***   SC_FACES
-- ***************************************************************
-- Criação de SC_FACES
CREATE TABLE IF NOT EXISTS SC_FACES (
	SC_ID_FACE	 		VARCHAR(258), /* SC_ID_QUADRA + SC_ID_LOGRADOURO */
	COD_MUNICIPIO 	  	VARCHAR(7),
	SC_ID_QUADRA 		VARCHAR(250),
	SC_ID_LOGRADOURO	VARCHAR(8),
	CN_ID_FACE	        VARCHAR(22),
	CENTROIDE           POINT,
    ORDEM_FACE        	INT(11),
	CI_ID_FACE       	VARCHAR(65),
    DIM_FACE			FLOAT,
    QTD_LOTES			INT(11)
);

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
    AND CN.COD_MUNICIPIO = Q.COD_MUNICIPIO;
    
-- Enriquecimento de SC_FACES com CI_FACES
UPDATE SC_FACES SC
INNER JOIN SC_QUADRAS Q 
	ON SC.SC_ID_QUADRA = Q.SC_ID_QUADRA
INNER JOIN CI_FACES CI 
	ON CI.COD_MUNICIPIO = SC.COD_MUNICIPIO 
	AND CI.SC_ID_LOGRADOURO = SC.SC_ID_LOGRADOURO
	AND CI.ID_QUADRA = Q.CI_ID_QUADRA -- Crucial para garantir a face na quadra correta
SET 
    SC.DIM_FACE = CI.DIM_FACE,
    SC.QTD_LOTES = CI.QTD_LOTES,
    SC.CI_ID_FACE = CI.ID_FACE;
    
-- Verificar faces que ficaram sem dados de dimensão após o enriquecimento
SELECT 
    SC_ID_FACE, 
    SC_ID_LOGRADOURO, 
    SC_ID_QUADRA 
FROM SC_FACES 
WHERE DIM_FACE IS NULL OR DIM_FACE = 0;

-- ***************************************************************
-- SC_PARCELAS
-- SC_PARCELAS
-- SC_PARCELAS
-- ***************************************************************
-- Criação de SC_PARCELAS
CREATE TABLE IF NOT EXISTS SC_PARCELAS (
	SC_ID_PARCELA 		VARCHAR(33), 	-- COD_MINICIPIO + ";" + TRIM(CI_ID_LOTE)
	SC_ID_FACE	        VARCHAR(259),
	SC_ID_QUADRA 		VARCHAR(250),
	SC_ID_LOGRADOURO	VARCHAR(8),
	COD_MUNICIPIO 	  	VARCHAR(7),
    CI_ID_LOTE			VARCHAR(22),	-- TRIM(CI_LOTES.COD_UNICO_ENDERECO)
    CI_ID_FACE			VARCHAR(67),
    CI_ID_QUADRA		VARCHAR(20),
    NUM_ENDERECO		VARCHAR(10),	-- TRIM(CI_LOTES.COD_NUM_ENDERECO)	
	DIM_TESTADA         FLOAT,
	DIM_PROFUNDIDADE    FLOAT,
	COORD_PARCELA		POLYGON
);

-- Povoamento de SC_PARCELAS com CI_LOTES e CI_LOGRADOUROS
INSERT INTO SC_PARCELAS (
    SC_ID_PARCELA,
    COD_MUNICIPIO,
    CI_ID_FACE, 
    CI_ID_QUADRA,
    SC_ID_LOGRADOURO,
    NUM_ENDERECO,
    DIM_TESTADA,
    DIM_PROFUNDIDADE
)
SELECT 
    CONCAT(L.COD_MUNICIPIO, ';', TRIM(L.COD_UNICO_ENDERECO)) AS SC_ID_PARCELA,
    L.COD_MUNICIPIO,
    TRIM(L.COD_UNICO_ENDERECO) AS CI_ID_LOTE,
    TRIM(L.NUM_QUADRA),
    CL.SC_ID_LOGRADOURO,
    TRIM(L.NUM_ENDERECO) AS NUM_ENDERECO,
    L.DIM_TESTADA,
    L.DIM_PROFUNDIDADE
FROM CI_LOTES L
INNER JOIN CI_LOGRADOUROS CL 
    ON L.NOM_LOGRADOURO = CL.CI_NOM_LOGRADOURO 
    AND L.COD_MUNICIPIO = CL.COD_MUNICIPIO;
   
-- Enriquecimento de SC_PARCELAS com SC_QUADRAS
UPDATE SC_PARCELAS P
INNER JOIN SC_QUADRAS SQ 
    ON P.CI_ID_QUADRA = SQ.CI_ID_QUADRA 
    AND P.COD_MUNICIPIO = SQ.COD_MUNICIPIO
SET 
    P.SC_ID_QUADRA = SQ.SC_ID_QUADRA,
    P.CI_ID_FACE = CONCAT(P.COD_MUNICIPIO, ";", P.CI_ID_QUADRA, ";", P.SC_ID_LOGRADOURO),          
    P.SC_ID_FACE = CONCAT(SQ.SC_ID_QUADRA, ';', P.SC_ID_LOGRADOURO);
    
SET SESSION group_concat_max_len = 1000000;
SELECT 
    JSON_OBJECT(
        'sc_id_quadra', SC_ID_QUADRA,
        'faces', JSON_EXTRACT(
            CONCAT('[', GROUP_CONCAT(
                JSON_OBJECT(
                    'ordem_face', ORDEM_FACE,
                    'dim_face', DIM_FACE,
                    'sc_id_face', SC_ID_FACE,
                    'sc_id_logradouro', SC_ID_LOGRADOURO,
                    'centroide', JSON_OBJECT(
                        'lat', ST_Y(CENTROIDE),
                        'lng', ST_X(CENTROIDE)
                    )
                )
            ), ']'),
            '$'
        )
    ) AS quadra_json
FROM SC_FACES
WHERE SC_ID_QUADRA IN (
	'["0E9A43FC","37EBE4BB","5C81EDF2","EF2B51D7"]',
    '["1558A9D1","9003F37C","F0D334EC","F54DE0AF"]',
    '["6B8E7F2D","736565AB","9E705E10","E544EA7B"]')
GROUP BY SC_ID_QUADRA;

SELECT 
    JSON_OBJECT(
        'sc_id_quadra', SC_ID_QUADRA,
        'faces', JSON_EXTRACT(
            CONCAT('[', 
                GROUP_CONCAT(
                    JSON_OBJECT(
                        'ordem_face', ORDEM_FACE,
                        'dim_face', DIM_FACE,
                        'sc_id_face', SC_ID_FACE,
                        'sc_id_logradouro', SC_ID_LOGRADOURO,
                        'centroide', JSON_OBJECT(
                            'lat', ST_Y(CENTROIDE),
                            'lng', ST_X(CENTROIDE)
                        )
                    )
                ), 
            ']'),
            '$'
        )
    ) AS quadra_json
FROM SC_FACES
WHERE SC_ID_QUADRA IN (
    '["0E9A43FC","37EBE4BB","5C81EDF2","EF2B51D7"]',
    '["1558A9D1","9003F37C","F0D334EC","F54DE0AF"]',
    '["6B8E7F2D","736565AB","9E705E10","E544EA7B"]'
)
GROUP BY SC_ID_QUADRA;

-- GERA JSON DE SC_QUADRAS
SELECT 
    JSON_OBJECT(
        'sc_id_quadra', F.SC_ID_QUADRA,
        'faces', JSON_EXTRACT(
            CONCAT('[', 
                GROUP_CONCAT(
                    JSON_OBJECT(
                        'ordem_face', F.ORDEM_FACE,
                        'dim_face', F.DIM_FACE,
                        'sc_id_face', F.SC_ID_FACE,
                        'sc_id_logradouro', F.SC_ID_LOGRADOURO,
                        'nome_logradouro', L.NOM_LOGRADOURO, -- Nome vindo da tabela CN_LOGRADOURO
                        'centroide', JSON_OBJECT(
                            'lat', ST_Y(F.CENTROIDE),
                            'lng', ST_X(F.CENTROIDE)
                        )
                    )
                ), 
            ']'),
            '$'
        )
    ) AS quadra_json
FROM SC_FACES F
LEFT JOIN CN_LOGRADOUROS L ON F.SC_ID_LOGRADOURO = L.SC_ID_LOGRADOURO
WHERE F.SC_ID_QUADRA IN (
    '["0E9A43FC","37EBE4BB","5C81EDF2","EF2B51D7"]',
    '["1558A9D1","9003F37C","F0D334EC","F54DE0AF"]',
    '["6B8E7F2D","736565AB","9E705E10","E544EA7B"]'
)
GROUP BY F.SC_ID_QUADRA;
