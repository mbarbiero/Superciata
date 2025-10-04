// db.js
const mysql = require('mysql2/promise');
const fs = require('fs');

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

// Função para criar tabela
async function criaTabelaSeNecessario(tabela = 'CN_PONTOS') {
  const tabelaJaExiste = await tabelaExiste(tabela);

  if (tabelaJaExiste) {
    console.log(`Tabela ${tabela} já existe, pulando criação`);
    return true;
  }

  console.log('Criando tabela CN_PONTOS...');

  const createTableCN_PONTOS = `
    CREATE TABLE CN_PONTOS (
      id INT AUTO_INCREMENT PRIMARY KEY,
      COD_UNICO_ENDERECO VARCHAR(50),
      COD_UF VARCHAR(2),
      COD_MUNICIPIO VARCHAR(10),
      COD_DISTRITO VARCHAR(10),
      COD_SUBDISTRITO VARCHAR(10),
      COD_SETOR VARCHAR(20),
      NUM_QUADRA VARCHAR(20),
      NUM_FACE VARCHAR(20),
      CEP VARCHAR(8),
      DSC_LOCALIDADE VARCHAR(100),
      NOM_TIPO_SEGLOGR VARCHAR(50),
      NOM_TITULO_SEGLOGR VARCHAR(50),
      NOM_SEGLOGR VARCHAR(200),
      NUM_ENDERECO VARCHAR(20),
      DSC_MODIFICADOR VARCHAR(50),
      NOM_COMP_ELEM1 VARCHAR(50),
      VAL_COMP_ELEM1 VARCHAR(100),
      NOM_COMP_ELEM2 VARCHAR(50),
      VAL_COMP_ELEM2 VARCHAR(100),
      NOM_COMP_ELEM3 VARCHAR(50),
      VAL_COMP_ELEM3 VARCHAR(100),
      NOM_COMP_ELEM4 VARCHAR(50),
      VAL_COMP_ELEM4 VARCHAR(100),
      NOM_COMP_ELEM5 VARCHAR(50),
      VAL_COMP_ELEM5 VARCHAR(100),
      LATITUDE DECIMAL(10,8),
      LONGITUDE DECIMAL(11,8),
      NV_GEO_COORD VARCHAR(10),
      COD_ESPECIE VARCHAR(10),
      DSC_ESTABELECIMENTO VARCHAR(200),
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
async function carregaCSV(arquivoPath, tabela = 'CN_PONTOS') {
  await criaTabelaSeNecessario(tabela);

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
      INTO TABLE ${tabela}
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
        COD_MUNICIPIO        VARCHAR(10),
        COD_UNICO_ENDERECO   VARCHAR(50),
        ID_QUADRA            VARCHAR(20),
        ID_FACE              VARCHAR(20),
        NOM_LOGRADOURO       VARCHAR(255),
        NUM_ENDERECO         VARCHAR(20),
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
      CONCAT(COD_SETOR, LPAD(NUM_QUADRA, 3, '0'), LPAD(NUM_FACE, 2, '0')) AS ID_FACE,
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

module.exports = {
  testaConexao,
  executaQuery,
  carregaCSV,
  tabelaExiste,
  criaTabelaSeNecessario,
  descreveTabela,
  amostraDados,
  contaRegistros,
  cria_CN_PONTOS_UNICOS,
  preenche_CN_PONTOS_UNICOS
};