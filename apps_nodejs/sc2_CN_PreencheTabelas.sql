-- Remove dados antigos do município para evitar duplicidade
DELETE FROM sc2_CN_PONTOS WHERE COD_MUNICIPIO = '${MUNICIPIO}';

-- Executa a carga massiva via Stream
LOAD DATA LOCAL INFILE '${CAMINHO_CSV}'
INTO TABLE ${TABELA}
CHARACTER SET utf8
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

