// db.js
const mysql = require('mysql2/promise');
const fs = require('fs');
const CI = require('./ciata.js');

const dbConfig = {
  host: 'mysql.smuu.com.br',
  user: 'smuu_add1',
  password: 'SmuuBd1',
  database: 'smuu',
  port: 3306,
  // Configuração necessária para LOCAL INFILE no mysql2 v2.0+
  authPlugins: {
    mysql_clear_password: () => () => Buffer.from(`${dbConfig.password}\0`)
  },
  // IMPORTANTE: Configurar para usar LOCAL INFILE
  flags: ['+LOCAL_FILES', '+PLUGIN_AUTH'],
  connectionLimit: 10,
  queueLimit: 0,
  // Habilitar LOCAL INFILE
  enableKeepAlive: true,
  keepAliveInitialDelay: 0
};

const pool = mysql.createPool(dbConfig);

// Função para testar conexão
async function testaConexao() {
  const connection = await pool.getConnection();
  try {
    await connection.ping();
    console.log('Conexão MySQL estabelecida com sucesso!');
    return true;
  } finally {
    connection.release();
  }
}

// Função para verificar se tabela existe
async function tabelaExiste(nomeTabela = 'CN_PONTOS') {
  try {
    const query = `
      SELECT COUNT(*) as existe 
      FROM information_schema.tables 
      WHERE table_schema = ? AND table_name = ?
    `;
    const resultado = await executaQuery(query, [dbConfig.database, nomeTabela]);
    return resultado[0].existe > 0;
  } catch (error) {
    console.error('Erro ao verificar tabela:', error);
    return false;
  }
}

// Função para criar tabela CN_PONTOS
async function cria_CN_PONTOS(tabela = 'CN_PONTOS') {

  console.log('Criando tabela CN_PONTOS...');

  const createTableCN_PONTOS = `
    CREATE TABLE IF NOT EXISTS CN_PONTOS (
      id INT AUTO_INCREMENT PRIMARY KEY,
      COD_UNICO_ENDERECO VARCHAR(20),
      COD_UF VARCHAR(2),
      COD_MUNICIPIO VARCHAR(7),
      COD_DISTRITO VARCHAR(9),
      COD_SUBDISTRITO VARCHAR(11),
      COD_SETOR VARCHAR(16),
      NUM_QUADRA VARCHAR(3),
      NUM_FACE VARCHAR(3),
      CEP VARCHAR(8),
      DSC_LOCALIDADE VARCHAR(50),
      NOM_TIPO_SEGLOGR VARCHAR(50),
      NOM_TITULO_SEGLOGR VARCHAR(50),
      NOM_SEGLOGR VARCHAR(50),
      NUM_ENDERECO VARCHAR(10),
      DSC_MODIFICADOR VARCHAR(20),
      NOM_COMP_ELEM1 VARCHAR(20),
      VAL_COMP_ELEM1 VARCHAR(20),
      NOM_COMP_ELEM2 VARCHAR(20),
      VAL_COMP_ELEM2 VARCHAR(20),
      NOM_COMP_ELEM3 VARCHAR(20),
      VAL_COMP_ELEM3 VARCHAR(20),
      NOM_COMP_ELEM4 VARCHAR(20),
      VAL_COMP_ELEM4 VARCHAR(20),
      NOM_COMP_ELEM5 VARCHAR(20),
      VAL_COMP_ELEM5 VARCHAR(20),
      LATITUDE DECIMAL(10,8),
      LONGITUDE DECIMAL(10,8),
      NV_GEO_COORD VARCHAR(10),
      COD_ESPECIE VARCHAR(10),
      DSC_ESTABELECIMENTO VARCHAR(100),
      COD_INDICADOR_ESTAB_ENDERECO VARCHAR(10),
      COD_INDICADOR_CONST_ENDERECO VARCHAR(10),
      COD_INDICADOR_FINALIDADE_CONST VARCHAR(10),
      COD_TIPO_ESPECI VARCHAR(10),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `;

  try {
    await executaQuery(createTableCN_PONTOS);
    console.log('Tabela CN_PONTOS criada com sucesso');
    return true;
  } catch (error) {
    console.error('Erro ao criar tabela:', error);
    return false;
  }
}

// Função para executar queries
async function executaQuery(sql, params = []) {
  const connection = await pool.getConnection();
  try {
    console.log(`Executando query: ${sql.substring(0, 100)}...`);
    const [resultado] = await connection.execute(sql, params);
    return resultado;
  }
  catch (error) {
    console.error('Erro ao criar tabela:', error);
    return false;
  }
  finally {
    connection.release();
  }
}

// Função para LOAD DATA LOCAL INFILE
async function preenche_CN_PONTOS(arquivoPath) {

  // Criar uma conexão especial para LOCAL INFILE
  const connection = await mysql.createConnection({
    ...dbConfig,
    // Configuração crucial para mysql2 v2.0+
    streamFactory: (path) => fs.createReadStream(path),
    infileStreamFactory: (path) => fs.createReadStream(path)
  });

  try {
    // Verificar se arquivo existe
    if (!fs.existsSync(arquivoPath)) {
      throw new Error(`Arquivo não encontrado: ${arquivoPath}`);
    }

    const caminhoUnix = arquivoPath.replace(/\\/g, '/');

    console.log(`Preparando LOAD DATA LOCAL INFILE: ${caminhoUnix}`);

    // Query com LOCAL INFILE
    const query = `
      LOAD DATA LOCAL INFILE '${caminhoUnix}'
      INTO TABLE CN_PONTOS
      CHARACTER SET utf8
      FIELDS TERMINATED BY ';'
      LINES TERMINATED BY '\\n'
      IGNORE 1 ROWS
      (
        COD_UNICO_ENDERECO, COD_UF, COD_MUNICIPIO, COD_DISTRITO, COD_SUBDISTRITO,
        COD_SETOR, NUM_QUADRA, NUM_FACE, CEP, DSC_LOCALIDADE, NOM_TIPO_SEGLOGR,
        NOM_TITULO_SEGLOGR, NOM_SEGLOGR, NUM_ENDERECO, DSC_MODIFICADOR,
        NOM_COMP_ELEM1, VAL_COMP_ELEM1, NOM_COMP_ELEM2, VAL_COMP_ELEM2,
        NOM_COMP_ELEM3, VAL_COMP_ELEM3, NOM_COMP_ELEM4, VAL_COMP_ELEM4,
        NOM_COMP_ELEM5, VAL_COMP_ELEM5, LATITUDE, LONGITUDE, NV_GEO_COORD,
        COD_ESPECIE, DSC_ESTABELECIMENTO, COD_INDICADOR_ESTAB_ENDERECO,
        COD_INDICADOR_CONST_ENDERECO, COD_INDICADOR_FINALIDADE_CONST, COD_TIPO_ESPECI
      )
    `;

    console.log('Executando carga de dados...');

    const [resultado] = await connection.query(query);

    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resultado;

  } catch (error) {
    console.error('Erro no LOAD DATA LOCAL INFILE:', error);
    throw error;
  }
}

// Função para criar CN_PONTOS_UNICOS
async function cria_CN_PONTOS_UNICOS() {
  console.log(`Criando CN_PONTOS_UNICOS`);
  const query = `
      CREATE TABLE IF NOT EXISTS CN_PONTOS_UNICOS (
        id INT AUTO_INCREMENT PRIMARY KEY,
        COD_MUNICIPIO        VARCHAR(7),
        COD_UNICO_ENDERECO   VARCHAR(20),
        ID_QUADRA            VARCHAR(19),
        ID_FACE              VARCHAR(22),
        NOM_LOGRADOURO       VARCHAR(250),
        NUM_ENDERECO         VARCHAR(10),
        LATITUDE             DECIMAL(10,8),
        LONGITUDE            DECIMAL(11,8),
        COORDS               POINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`
  try {
    await executaQuery(query);

    const resultado = {
      "sucesso": true,
      "mensagem": "Tabela CN_PONTOS_UNICOS criada com sucesso."
    }
    return resultado;
  } catch (error) {
    throw error;
  }
}

// Função para preencher CN_PONTOS_UNICOS
async function preenche_CN_PONTOS_UNICOS() {

  console.log(`Preparando CN_PONTOS_UNICOS`);
  // Query com LOCAL INFILE
  const query = `
    INSERT INTO CN_PONTOS_UNICOS (
      COD_MUNICIPIO, COD_UNICO_ENDERECO, ID_QUADRA, ID_FACE,
      NOM_LOGRADOURO, NUM_ENDERECO, LATITUDE, LONGITUDE, COORDS
    )
    SELECT 
      COD_MUNICIPIO,
      COD_UNICO_ENDERECO,
      CONCAT(COD_SETOR, LPAD(NUM_QUADRA, 3, '0')) AS ID_QUADRA,
      CONCAT(COD_SETOR, LPAD(NUM_QUADRA, 3, '0'), LPAD(NUM_FACE, 3, '0')) AS ID_FACE,
      REPLACE(
        CONCAT(
          TRIM(COALESCE(NOM_TIPO_SEGLOGR,'')), ' ',
          TRIM(COALESCE(NOM_TITULO_SEGLOGR,'')), ' ',
          TRIM(COALESCE(NOM_SEGLOGR,''))
        ),
        '  ', ' '
      ) AS NOM_LOGRADOURO,
      NUM_ENDERECO,
      LATITUDE,
      LONGITUDE,
      ST_GeomFromText(CONCAT('POINT(', LONGITUDE, ' ', LATITUDE, ')'))
    FROM CN_PONTOS
    WHERE NUM_QUADRA > 0
      AND NV_GEO_COORD < '4'
    ORDER BY ID_QUADRA;
  `;

  console.log('Executando carga de dados...');

  try {
    const resultado = await executaQuery(query);

    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resultado;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CN_PONTOS_UNICOS:', error);
    throw error;
  }
}

// Funções de inspeção
async function descreveTabela(nomeTabela = 'CN_PONTOS') {
  try {
    const resultado = await executaQuery(`DESCRIBE ${nomeTabela}`);
    return resultado;
  } catch (error) {
    console.error('Erro ao descrever tabela:', error);
    return null;
  }
}

async function amostraDados(nomeTabela = 'CN_PONTOS', limite = 5) {
  try {
    const resultado = await executaQuery(`SELECT * FROM ${nomeTabela} LIMIT ?`, [limite]);
    return resultado;
  } catch (error) {
    console.error('Erro ao obter amostra:', error);
    return null;
  }
}

async function contaRegistros(nomeTabela = 'CN_PONTOS') {
  try {
    const resultado = await executaQuery(`SELECT COUNT(*) as total FROM ${nomeTabela}`);
    return resultado[0].total;
  } catch (error) {
    console.error('Erro ao contar registros:', error);
    return 0;
  }
}

// 🔹🔹🔹 CN_QUADRAS 🔹🔹🔹
// Função para criar CN_QUADRAS()
async function cria_CN_QUADRAS() {
  console.log(`Criando CN_QUADRAS`);
  const query = `
    CREATE TABLE IF NOT EXISTS CN_QUADRAS (
      id INT AUTO_INCREMENT PRIMARY KEY,
      ID_QUADRA            VARCHAR(19),
      COD_MUNICIPIO VARCHAR(7),
      QTD_PONTOS INT,
      CENTROIDE POINT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`
  try {
    await executaQuery(query);

    const resultado = {
      "sucesso": true,
      "mensagem": "Tabela CN_QUADRAS criada com sucesso."
    }
    return resultado;
  } catch (error) {
    throw error;
  }
}

// Função para preencher CN_QUADRAS
async function preenche_CN_QUADRAS() {

  console.log(`Preparando CN_QUADRAS`);

  const query = `
    INSERT INTO CN_QUADRAS (ID_QUADRA, COD_MUNICIPIO, QTD_PONTOS,
        CENTROIDE)
    SELECT
        ID_QUADRA,
        COD_MUNICIPIO,
        COUNT(*) AS QTD_PONTOS,
        POINT(AVG(LONGITUDE), AVG(LATITUDE)) AS CENTROIDE
    FROM CN_PONTOS_UNICOS
    GROUP BY ID_QUADRA;
  `;

  console.log('Executando carga de dados...');

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "Tabela CN_QUADRAS criada com sucesso.",
      "linhas incluídas": resultado.affectedRows 
    }

    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resp;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CN_QUADRAS:', error);
    throw error;
  }
}

// 🔹🔹🔹 CN_FACES 🔹🔹🔹
// Função para criar CN_FACES()
async function cria_CN_FACES() {
  console.log(`Criando CN_FACES`);
  const query = `
    CREATE TABLE IF NOT EXISTS CN_FACES (
      ID_FACE VARCHAR(22),
      COD_MUNICIPIO VARCHAR(7),
      ID_QUADRA VARCHAR(19),
      NOM_LOGRADOURO VARCHAR(250),
      QTD_PONTOS INT,
      CENTROIDE POINT
    );`
  try {
    await executaQuery(query);

    const resultado = {
      "sucesso": true,
      "mensagem": "Tabela CN_FACES criada com sucesso."
    }
    return resultado;
  } catch (error) {
    throw error;
  }
}

// Função para preencher CN_FACES
async function preenche_CN_FACES() {

  console.log(`Preparando CN_FACES`);

  const query = `
    INSERT INTO CN_FACES (ID_FACE, COD_MUNICIPIO, ID_QUADRA,
        NOM_LOGRADOURO, QTD_PONTOS, CENTROIDE)
    SELECT
        ID_FACE,
        COD_MUNICIPIO,
        ID_QUADRA,
        NOM_LOGRADOURO,
        COUNT(*) AS QTD_PONTOS,
        POINT(AVG(LONGITUDE), AVG(LATITUDE)) AS CENTROIDE
    FROM CN_PONTOS_UNICOS
    GROUP BY ID_FACE;
  `;

  console.log('Executando carga de dados...');

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "Tabela CN_FACES criada com sucesso.",
      "linhas incluídas": resultado.affectedRows 
    }

    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resp;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CN_FACES:', error);
    throw error;
  }
}

// 🔹🔹🔹 CI_LOTES 🔹🔹🔹
// Função para criar CI_LOTES
async function cria_CI_LOTES() {
  console.log(`Criando CI_LOTES`);
  try {
    await executaQuery(CI.SQL_CriaCI_LOTES);

    const resultado = {
      "sucesso": true,
      "mensagem": "Tabela CI_LOTES criada com sucesso."
    }
    return resultado;
  } catch (error) {
    throw error;
  }
}

// Função para LOAD DATA LOCAL INFILE
async function preenche_CI_LOTES(arquivoPath) {

  // Criar uma conexão especial para LOCAL INFILE
  const connection = await mysql.createConnection({
    ...dbConfig,
    // Configuração crucial para mysql2 v2.0+
    streamFactory: (path) => fs.createReadStream(path),
    infileStreamFactory: (path) => fs.createReadStream(path)
  });

  try {
    // Verificar se arquivo existe
    if (!fs.existsSync(arquivoPath)) {
      throw new Error(`Arquivo não encontrado: ${arquivoPath}`);
    }

    const caminhoUnix = arquivoPath.replace(/\\/g, '/');

    console.log(`Preparando LOAD DATA LOCAL INFILE: ${caminhoUnix}`);

    // Query com LOCAL INFILE
    const query = `
      LOAD DATA LOCAL INFILE '${caminhoUnix}'
      INTO TABLE CN_PONTOS
      CHARACTER SET utf8
      FIELDS TERMINATED BY ';'
      LINES TERMINATED BY '\\n'
      IGNORE 1 ROWS
      (
        COD_UNICO_ENDERECO, COD_UF, COD_MUNICIPIO, COD_DISTRITO, COD_SUBDISTRITO,
        COD_SETOR, NUM_QUADRA, NUM_FACE, CEP, DSC_LOCALIDADE, NOM_TIPO_SEGLOGR,
        NOM_TITULO_SEGLOGR, NOM_SEGLOGR, NUM_ENDERECO, DSC_MODIFICADOR,
        NOM_COMP_ELEM1, VAL_COMP_ELEM1, NOM_COMP_ELEM2, VAL_COMP_ELEM2,
        NOM_COMP_ELEM3, VAL_COMP_ELEM3, NOM_COMP_ELEM4, VAL_COMP_ELEM4,
        NOM_COMP_ELEM5, VAL_COMP_ELEM5, LATITUDE, LONGITUDE, NV_GEO_COORD,
        COD_ESPECIE, DSC_ESTABELECIMENTO, COD_INDICADOR_ESTAB_ENDERECO,
        COD_INDICADOR_CONST_ENDERECO, COD_INDICADOR_FINALIDADE_CONST, COD_TIPO_ESPECI
      )
    `;

    console.log('Executando carga de dados...');

    const [resultado] = await connection.query(query);

    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resultado;

  } catch (error) {
    console.error('Erro no LOAD DATA LOCAL INFILE:', error);
    throw error;
  }
}


module.exports = {
  testaConexao,
  executaQuery,
  tabelaExiste,
  descreveTabela,
  amostraDados,
  contaRegistros,
  cria_CN_PONTOS,
  preenche_CN_PONTOS,
  cria_CN_PONTOS_UNICOS,
  preenche_CN_PONTOS_UNICOS,
  cria_CN_QUADRAS,
  preenche_CN_QUADRAS,
  cria_CN_FACES,
  preenche_CN_FACES,
  cria_CI_LOTES
};