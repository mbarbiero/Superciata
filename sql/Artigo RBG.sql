SELECT * FROM smuu.CI_LOTES;

      CREATE TABLE IF NOT EXISTS CI_FACES (
         ID_FACE                       VARCHAR(28),
         COD_MUNICIPIO                 VARCHAR(7),
         NUM_QUADRA                    VARCHAR(20),
         SC_ID_LOGRADOURO              VARCHAR(8),
         CI_NOM_LOGRADOURO             VARCHAR(250),
         QTD_LOTES                     INT,
         DIMENSAO                      FLOAT
    );
      
CREATE TABLE IF NOT EXISTS CI_QUADRAS (
		ID_QUADRA 			varchar(19),
		COD_MUNICIPIO 		varchar(7),
		SC_ID_QUADRA 		varchar(250),
		QTD_LOTES 			int(11)
	);

SELECT DISTINCT COD_MUNICIPIO, NUM_QUADRA, NOM_LOGRADOURO
FROM CI_LOTES LT
;