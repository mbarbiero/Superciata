// SQL para criar tabela CI_LOTES
const SQL_CriaCI_LOTES = `
      CREATE TABLE IF NOT EXISTS CI_LOTES (
         COD_MUNICIPIO      VARCHAR(7),
         COD_UNICO_ENDERECO VARCHAR(25),
         COD_SETOR          VARCHAR(15),
         NUM_QUADRA         VARCHAR(20),
         LOTE               VARCHAR(30),
         NOM_LOGRADOURO     VARCHAR(250),
         NUM_ENDERECO       VARCHAR(10),
         QTD_TESTADAS       INT,
         DIM_TESTADA        DECIMAL(6,2),
         DIM_LATERAL        DECIMAL(6,2)
      );`;

module.exports = {
   SQL_CriaCI_LOTES
};
