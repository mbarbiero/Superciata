// SQL para criar tabela CI_LOTES
const SQL_CriaCI_LOTES = `
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

const SQL_CriaSC_LOGRADOUROS = `
      CREATE TABLE IF NOT EXISTS SC_LOGRADOUROS (
        SC_ID_LOGRADOURO     VARCHAR(8),   
        COD_MUNICIPIO        VARCHAR(7),
        SC_NOM_LOGRADOURO    VARCHAR(250),
        CI_NOM_LOGRADOURO    VARCHAR(250), 
        COORDS               TEXT
      );`

const SQL_NormalizaCI_LOTES = `
      UPDATE CI_LOTES
         SET SC_NOM_LOGRADOURO = TrocaAbreviaturas(NormalizaString(SC_NOM_LOGRADOURO));`

module.exports = {
   SQL_CriaCI_LOTES,
   SQL_NormalizaCI_LOTES,
   SQL_CriaSC_LOGRADOUROS
};
