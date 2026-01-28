// SQL para criar tabela CI_LOTES
const SQL_Cria_CI_LOTES = `
      CREATE TABLE IF NOT EXISTS CI_LOTES (
         COD_MUNICIPIO              VARCHAR(7),
         COD_UNICO_ENDERECO         VARCHAR(25),
         NUM_QUADRA                 VARCHAR(20),
         NOM_LOGRADOURO             VARCHAR(250),
         NUM_ENDERECO               VARCHAR(10),
         NOM_LOGRADOURO_ADJACENTE   VARCHAR(250),
         DIM_TESTADA                DECIMAL(6,2),
         DIM_PROFUNDIDADE           DECIMAL(6,2)
      );`;

const SQL_Cria_CI_LOGRADOUROS = `
      CREATE TABLE IF NOT EXISTS CI_LOGRADOUROS (
         SC_ID_LOGRADOURO              VARCHAR(8),
         COD_MUNICIPIO                 VARCHAR(7),
         CI_NOM_LOGRADOURO_NORM        VARCHAR(250),
         CI_NOM_LOGRADOURO             VARCHAR(250)
      );`;

const SQL_Cria_CI_FACES = `
      CREATE TABLE IF NOT EXISTS CI_FACES (
         ID_FACE                       VARCHAR(28),
         COD_MUNICIPIO                 VARCHAR(7),
         NUM_QUADRA                    VARCHAR(20),
         SC_ID_LOGRADOURO              VARCHAR(8),
         CI_NOM_LOGRADOURO             VARCHAR(250),
         QTD_LOTES                     INT,
         DIMENSAO                      DECIMAL(6,2)
      );`;

const SQL_Cria_CI_QUADRAS = `
      CREATE TABLE IF NOT EXISTS CI_QUADRAS (
         SC_ID_LOGRADOURO              VARCHAR(8),
         COD_MUNICIPIO                 VARCHAR(7),
      CI_NOM_LOGRADOURO_NORM        VARCHAR(250),
      CI_NOM_LOGRADOURO             VARCHAR(250),

      );`;

const SQL_Normaliza_CI_LOTES = `
      UPDATE CI_LOTES
         SET SC_NOM_LOGRADOURO = TrocaAbreviaturas(NormalizaString(SC_NOM_LOGRADOURO));`

module.exports = {
   SQL_Cria_CI_LOTES,
   SQL_Normaliza_CI_LOTES,
   SQL_Cria_CI_LOGRADOUROS,
   SQL_Cria_CI_LOTES,
   SQL_Cria_CI_QUADRAS
};
