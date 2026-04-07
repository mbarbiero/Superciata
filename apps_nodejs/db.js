// db.js
const mysql = require('mysql2/promise');
const fs = require('fs');
const CI = require('./ciata.js');
const util = require('./util.js');

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
  const dropTableCN_PONTOS = 'DROP TABLE IF EXISTS CN_PONTOS;';
  try {
    await executaQuery(dropTableCN_PONTOS);
    console.log('Tabela CN_PONTOS excluída com sucesso');
  } catch (error) {
    console.error('Erro ao excluir tabela:', error);
    return false;
  }

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
    //console.log(`Executando query: ${sql.substring(0, 100)}...`);
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
      );
    `;
    console.log(query);
    const [resultado] = await connection.query(query);
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
        COD_MUNICIPIO        VARCHAR(7),
        COD_UNICO_ENDERECO   VARCHAR(20),
        ID_QUADRA            VARCHAR(19),
        ID_FACE              VARCHAR(22),
        NOM_LOGRADOURO       VARCHAR(250),
        NUM_ENDERECO         VARCHAR(10),
        LATITUDE             DECIMAL(10,8),
        LONGITUDE            DECIMAL(11,8),
        COORDS               POINT
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
  let resultado = '';
  try {
    let query = `
    DROP TABLE IF EXISTS TMP_ENDERECOS_BASE;
    `;
    resultado = await executaQuery(query);

    query = `
    DROP TABLE IF EXISTS TMP_ENDERECOS_UNICOS;
    `;
    resultado = await executaQuery(query);

    query = `  
      CREATE TABLE TMP_ENDERECOS_BASE AS
      SELECT 
          COD_MUNICIPIO,
          COD_UNICO_ENDERECO,
          -- No MySQL use CONCAT(), no Postgres use ||
          CONCAT(COD_SETOR, LPAD(NUM_QUADRA, 3, '0')) AS ID_QUADRA,
          CONCAT(COD_SETOR, LPAD(NUM_QUADRA, 3, '0'), LPAD(NUM_FACE, 3, '0')) AS ID_FACE,
          TRIM(
            CONCAT_WS(' ',
              NULLIF(TRIM(NOM_TIPO_SEGLOGR), ''),
              NULLIF(TRIM(NOM_TITULO_SEGLOGR), ''),
              NULLIF(TRIM(NOM_SEGLOGR), '')
            )
          ) AS NOM_LOGRADOURO,
          NUM_ENDERECO,
          LATITUDE,
          LONGITUDE
      FROM CN_PONTOS
      WHERE 
        NV_GEO_COORD < '4'
          AND 
        NUM_QUADRA > 0;
    `;
    // somente quadras urbanas?      --WHERE NUM_QUADRA > 0 AND NV_GEO_COORD < '4';
    resultado = await executaQuery(query);
    console.log(resultado);

    query = `
      -- Cria índice para acelerar o agrupamento
      CREATE INDEX idx_endereco_base ON TMP_ENDERECOS_BASE (COD_MUNICIPIO, NOM_LOGRADOURO, NUM_ENDERECO);
    `;
    resultado = await executaQuery(query);
    console.log(resultado);

    query = `
      -- Segunda etapa: Agrupamento para eliminação de duplicidades e cálculo de centroides
      -- MySQL 10.2
      CREATE TABLE TMP_ENDERECOS_UNICOS AS
      SELECT 
          COD_MUNICIPIO,
          NOM_LOGRADOURO,
          NUM_ENDERECO,
          AVG(LATITUDE) AS LATITUDE_MEDIA,
          AVG(LONGITUDE) AS LONGITUDE_MEDIA,
          COUNT(*) AS QTD_PONTOS,
          -- Identificação da Quadra e Face predominantes (Moda Estatística)
          SUBSTRING_INDEX(
              GROUP_CONCAT(ID_QUADRA ORDER BY ID_QUADRA SEPARATOR ','), 
              ',', 1
          ) AS ID_QUADRA_MODA,
          SUBSTRING_INDEX(
              GROUP_CONCAT(ID_FACE ORDER BY ID_FACE SEPARATOR ','), 
              ',', 1
          ) AS ID_FACE_MODA,
          MIN(COD_UNICO_ENDERECO) AS COD_UNICO_ENDERECO_REF
      FROM TMP_ENDERECOS_BASE
      GROUP BY 
          COD_MUNICIPIO, 
          NOM_LOGRADOURO, 
          NUM_ENDERECO;
    `;
    resultado = await executaQuery(query);
    console.log(resultado);

  } catch (error) {
    console.error('Erro ao criar tabelas temporárias:', error);
    throw error;
  }


  query = `
    -- Insere os endereços únicos na tabela final
    -- MySQL 10.2
    INSERT INTO CN_PONTOS_UNICOS (
      COD_MUNICIPIO, 
      COD_UNICO_ENDERECO, 
      ID_QUADRA, 
      ID_FACE, 
      NOM_LOGRADOURO, 
      NUM_ENDERECO, 
      LATITUDE, 
      LONGITUDE, 
      COORDS
    )
    SELECT 
        COD_MUNICIPIO,
        COD_UNICO_ENDERECO_REF,
        ID_QUADRA_MODA,
        ID_FACE_MODA,
        NOM_LOGRADOURO,
        NUM_ENDERECO,
        LATITUDE_MEDIA,
        LONGITUDE_MEDIA,
        POINT(LONGITUDE_MEDIA, LATITUDE_MEDIA)
    FROM TMP_ENDERECOS_UNICOS;
    `;
  /*
    query = `
        -- Insere os endereços únicos na tabela final
        -- MySQL 10.2
        INSERT INTO CN_PONTOS_UNICOS (
          COD_MUNICIPIO,
          COD_UNICO_ENDERECO,
          ID_QUADRA,
          ID_FACE,
          NOM_LOGRADOURO,
          NUM_ENDERECO,
          LATITUDE,
          LONGITUDE,
          COORDS
        )
        SELECT 
          COD_MUNICIPIO,
          COD_UNICO_ENDERECO_REF AS COD_UNICO_ENDERECO,
          ID_QUADRA_MODA AS ID_QUADRA,
          ID_FACE_MODA AS ID_FACE,
          NOM_LOGRADOURO,
          NUM_ENDERECO,
          LATITUDE_MEDIA AS LATITUDE,
          LONGITUDE_MEDIA AS LONGITUDE,
          ST_GeomFromText(CONCAT('POINT(', LONGITUDE_MEDIA, ' ', LATITUDE_MEDIA, ')'))
        FROM TMP_ENDERECOS_UNICOS;
      `;
      */

  console.log('Executando carga de dados...');
  try {
    resultado = await executaQuery(query);
    resp = {
      "sucesso": true,
      "mensagem": "Tabela CN_PONTOS_UNICOS preenchida com sucesso.",
      "linhas incluídas": resultado.affectedRows
    }

    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resp;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CN_PONTOS_UNICOS:', error);
    throw error;
  }
}

async function cria_CN_LOGRADOUROS() {
  console.log(`Criando CN_LOGRADOUROS`);
  const query = `
      CREATE TABLE IF NOT EXISTS CN_LOGRADOUROS (
        COD_MUNICIPIO        VARCHAR(7),
        NOM_LOGRADOURO       VARCHAR(250),
        SC_ID_LOGRADOURO     VARCHAR(8), 
        COORDS               TEXT
      );`
  try {
    await executaQuery(query);

    const resultado = {
      "sucesso": true,
      "mensagem": "Tabela CN_LOGRADOUROS criada com sucesso."
    }
    return resultado;
  } catch (error) {
    throw error;
  }
}

// Função para preencher CN_LOGRADOUROS
async function preenche_CN_LOGRADOUROS(cod_municipio) {

  console.log(`Preparando preenche_CN_LOGRADOUROS`);

  const query = `
    INSERT INTO CN_LOGRADOUROS (
        COD_MUNICIPIO, 
        NOM_LOGRADOURO, 
        SC_ID_LOGRADOURO, 
        COORDS
        )
        SELECT 
            COD_MUNICIPIO,
            NOM_LOGRADOURO,
            LPAD(HEX(CRC32(CONCAT(COD_MUNICIPIO, NOM_LOGRADOURO))), 8, '0') AS SC_ID_LOGRADOURO,
            ""
        FROM CN_PONTOS_UNICOS
        WHERE COD_MUNICIPIO = ${cod_municipio}  
        GROUP BY COD_MUNICIPIO, NOM_LOGRADOURO;
  `;

  console.log('Executando carga de dados...');

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "Tabela CN_LOGRADOUROS preenchida com sucesso.",
      "linhas incluídas": resultado.affectedRows
    }

    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resp;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CN_LOGRADOUROS:', error);
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
      SC_ID_QUADRA        VARCHAR(250),
      COD_MUNICIPIO       VARCHAR(7),
      ID_QUADRA           VARCHAR(19),
      QTD_PONTOS          INT,
      CENTROIDE           POINT
    );
  `;
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
    INSERT INTO CN_QUADRAS (
        SC_ID_QUADRA, 
        COD_MUNICIPIO, 
        ID_QUADRA, 
        QTD_PONTOS, 
        CENTROIDE)
    SELECT 
        -- SC_ID_QUADRA
        CONCAT('["', 
          GROUP_CONCAT(COALESCE(f.SC_ID_LOGRADOURO, '')
            ORDER BY f.SC_ID_LOGRADOURO 
            SEPARATOR '","'), 
        '"]') AS SC_ID_QUADRA,
        f.COD_MUNICIPIO,
        f.ID_QUADRA,
        SUM(f.QTD_PONTOS) AS QTD_PONTOS,
            -- Calcular centroide manualmente (média das coordenadas)
        ST_GEOMFROMTEXT(
            CONCAT(
              'POINT(',
                  AVG(ST_X(f.CENTROIDE)),
                  ' ',
                  AVG(ST_Y(f.CENTROIDE)),
              ')'
            )
        ) AS CENTROIDE
    FROM 
        CN_FACES f
    WHERE 
        f.SC_ID_LOGRADOURO IS NOT NULL
        AND f.SC_ID_LOGRADOURO != ''
    GROUP BY 
        f.ID_QUADRA, 
        f.COD_MUNICIPIO
    ORDER BY 
        f.COD_MUNICIPIO, 
        f.ID_QUADRA;
    `;

  console.log('Executando carga de dados...');

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "Tabela CN_QUADRAS preenchida com sucesso.",
      "linhas incluídas": resultado.affectedRows
    }
    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resp;
  } catch (error) {
    console.error('Erro ao preencher dados na tabela CN_QUADRAS:', error);
    throw error;
  }
}

// Função para preencher SC_ID_QUADRA em CN_QUADRAS
async function complementa_CN_QUADRAS() {

  console.log(`Preenchendo SC_ID_QUADRA em CN_QUADRAS`);

  const query = `
    UPDATE CN_QUADRAS q
    JOIN (
      SELECT 
        f.ID_QUADRA,
        -- Constrói o array usando aspas duplas literais
        CONCAT('["', 
          GROUP_CONCAT(SC_ID_LOGRADOURO ORDER BY SC_ID_LOGRADOURO SEPARATOR '","'), 
        '"]') AS SC_ID_QUADRA
        FROM CN_FACES f
        INNER JOIN CN_LOGRADOUROS l
          ON l.COD_MUNICIPIO = f.COD_MUNICIPIO
          AND l.NOM_LOGRADOURO = f.NOM_LOGRADOURO
        GROUP BY f.ID_QUADRA
      ) AS x 
    ON x.ID_QUADRA = q.ID_QUADRA
    SET q.SC_ID_QUADRA = x.SC_ID_QUADRA;
  `;

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "A chave SC_ID_QUADRA foi preenchida com sucesso.",
      "linhas atualizadas": resultado.affectedRows
    }
    console.log(`Atualização concluída: ${resultado.affectedRows} linhas afetadas`);
    return resp;
  } catch (error) {
    console.error('Erro ao atualizar SC_ID_QUADRAS na tabela CN_QUADRAS:', error);
    throw error;
  }
}

// 🔹🔹🔹 CN_FACES 🔹🔹🔹
// Função para criar CN_FACES()
async function cria_CN_FACES() {
  console.log(`Criando CN_FACES`);
  const query = `
    CREATE TABLE IF NOT EXISTS CN_FACES (
      ID_FACE           VARCHAR(22),
      COD_MUNICIPIO     VARCHAR(7),
      ID_QUADRA         VARCHAR(19),
      NOM_LOGRADOURO    VARCHAR(250),
      SC_ID_LOGRADOURO  VARCHAR(8),
      NR_ORDEM          INT,
      QTD_PONTOS        INT,
      CENTROIDE         POINT
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
    INSERT INTO CN_FACES (
        ID_FACE, 
        COD_MUNICIPIO, 
        ID_QUADRA,
        NOM_LOGRADOURO, 
        SC_ID_LOGRADOURO, 
        NR_ORDEM, 
        QTD_PONTOS, 
        CENTROIDE
    )
    SELECT
        ID_FACE,
        COD_MUNICIPIO,
        ID_QUADRA,
        NOM_LOGRADOURO,
        "",
        CAST(SUBSTRING(ID_FACE FROM LENGTH(ID_FACE) - 2) AS INTEGER),
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
      "mensagem": "Tabela CN_FACES preenchida com sucesso.",
      "linhas incluídas": resultado.affectedRows
    }

    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resp;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CN_FACES:', error);
    throw error;
  }
}

// Função para complementar CN_FACES
async function Complementa_CN_FACES(atributo) {

  console.log(`Complementando CN_FACES com ${atributo}`);

  const query = `
      UPDATE CN_FACES AS f
      JOIN CN_LOGRADOUROS AS l
          ON f.COD_MUNICIPIO = l.COD_MUNICIPIO
              AND TRIM(UPPER(f.NOM_LOGRADOURO)) = TRIM(UPPER(l.NOM_LOGRADOURO))
      SET f.SC_ID_LOGRADOURO = l.SC_ID_LOGRADOURO
      WHERE f.SC_ID_LOGRADOURO IS NULL 
          OR f.SC_ID_LOGRADOURO = '';
  `;

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "Tabela CN_FACES complementada com sucesso.",
      "linhas complementadas": resultado.affectedRows
    }

    console.log(`Complementação concluída: ${resultado.affectedRows} linhas afetadas`);
    return resp;

  } catch (error) {
    console.error('Erro ao complementar dados na tabela CN_FACES:', error);
    throw error;
  }
}


// 🔹🔹🔹 CI_LOTES 🔹🔹🔹
// 🔹 Função para criar CI_LOTES
async function cria_CI_LOTES() {
  console.log(`Criando CI_LOTES`);
  try {
    await executaQuery(CI.SQL_Cria_CI_LOTES);

    const resultado = {
      "sucesso": true,
      "mensagem": "Tabela CI_LOTES criada com sucesso."
    }
    return resultado;
  } catch (error) {
    throw error;
  }
}

// 🔹 Função para LOAD DATA LOCAL INFILE em CI_LOTES com tratamento de decimais
async function preenche_CI_LOTES(arquivoPath) {
  const connection = await mysql.createConnection({
    ...dbConfig,
    streamFactory: (path) => fs.createReadStream(path),
    infileStreamFactory: (path) => fs.createReadStream(path),
  });

  try {
    // Verificar se o arquivo existe
    if (!fs.existsSync(arquivoPath)) {
      throw new Error(`Arquivo não encontrado: ${arquivoPath}`);
    }

    const caminhoUnix = arquivoPath.replace(/\\/g, "/");
    console.log(`Preparando LOAD DATA LOCAL INFILE para CI_LOTES: ${caminhoUnix}`);

    // Query de importação com tratamento de decimais
    const query = `
      LOAD DATA LOCAL INFILE '${caminhoUnix}'
      INTO TABLE CI_LOTES
      CHARACTER SET utf8
      FIELDS TERMINATED BY ';'
      LINES TERMINATED BY '\\n'
      IGNORE 1 ROWS
      (
         COD_MUNICIPIO,
         COD_UNICO_ENDERECO,
         NUM_QUADRA,
         NOM_LOGRADOURO,
         NUM_ENDERECO,
         NOM_LOGRADOURO_ADJACENTE,
         @DIM_TESTADA,
         @DIM_PROFUNDIDADE
      )
      SET
         DIM_TESTADA = REPLACE(@DIM_TESTADA, ',', '.'),
         DIM_PROFUNDIDADE = REPLACE(@DIM_PROFUNDIDADE, ',', '.');
    `;

    const [resultado] = await connection.query(query);
    console.log(`✅ Importação concluída: ${resultado.affectedRows} registros inseridos.`);
    return resultado;

  } catch (error) {
    console.error("❌ Erro no LOAD DATA LOCAL INFILE (CI_LOTES):", error);
    throw error;

  } finally {
    await connection.end();
  }
}

// 🔹🔹🔹 CI_LOGRADOUROS 🔹🔹🔹
// 🔹 Função para criar CI_LOGRADOUROS
async function cria_CI_LOGRADOUROS() {
  console.log(`Criando CI_LOGRADOUROS`);
  try {
    const resultado = await executaQuery(CI.SQL_Cria_CI_LOGRADOUROS);
    return resultado;
  } catch (error) {
    throw error;
  }
}

// 🔹 Função para preencher CI_LOGRADOUROS
async function preenche_CI_LOGRADOUROS() {
  console.log('Preenchendo CI_LOGRADOUROS');

  var resultado = '';
  const query = `
    INSERT INTO CI_LOGRADOUROS (
        COD_MUNICIPIO, 
        CI_NOM_LOGRADOURO_NORM,
        CI_NOM_LOGRADOURO,
        SC_ID_LOGRADOURO
    )
    SELECT DISTINCT
        LT.COD_MUNICIPIO,
        REGEXP_REPLACE(TRIM(UPPER(LT.NOM_LOGRADOURO)), '[[:space:]]{2,}', ' ') AS CI_NOM_LOGRADOURO_NORM,
        LT.NOM_LOGRADOURO AS CI_NOM_LOGRADOURO,
        NULL AS SC_ID_LOGRADOURO
    FROM CI_LOTES LT;
  `;

  console.log('Executando carga de dados...');

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "Tabela CI_LOGRADOUROS preenchida com sucesso.",
      "linhas incluídas": resultado.affectedRows
    }

    console.log(`Carga concluída: ${resultado.affectedRows} linhas afetadas`);
    return resp;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CI_LOGRADOUROS:', error);
    throw error;
  }
}

// 🔹 Função para atualizar CI_LOGRADOUROS
async function atualiza_CI_LOGRADOUROS(cod_municipio) {
  let resp = {
    sucesso: true,
    municipio_codigo: cod_municipio,
    detalhes: {}
  };
  let linhasAfetadasTotal = 0;

  console.log('Atualizando CI_LOGRADOUROS');
  // Nomes de logradouros coincidentes em CI e CN
  try {
    const query = `
              UPDATE smuu.CI_LOGRADOUROS AS CI
                JOIN smuu.CN_LOGRADOUROS AS CN
                  ON 
                    CI.CI_NOM_LOGRADOURO_NORM = CN.NOM_LOGRADOURO 
                  AND 
                    CI.COD_MUNICIPIO = CN.COD_MUNICIPIO
                SET CI.SC_ID_LOGRADOURO = CN.SC_ID_LOGRADOURO
              WHERE 
                (CI.COD_MUNICIPIO = '${cod_municipio}')
                  AND 
                (CI.SC_ID_LOGRADOURO IS NULL);`
    const resultado = await executaQuery(query);
    linhasAfetadasTotal += resultado.affectedRows;

    resp.detalhes.etapa_coincidentes = {
      descricao: "Nomes de logradouros coincidentes (Exatos)",
      linhas_afetadas: resultado.affectedRows
    };
  } catch (error) {
    resp.sucesso = false;
    resp.mensagem = "Erro na execução da Etapa de Coincidência Exata.";
    resp.erro_detalhes = {
      etapa: "etapa_coincidentes",
      mensagem_original: error.message || String(error)
    };
    throw error;
  }

  // Nomes de logradouros SOUNDEX em CI e CN
  try {
    const query = `
              UPDATE smuu.CI_LOGRADOUROS AS CI
              JOIN smuu.CN_LOGRADOUROS AS CN
                ON 
                  SOUNDEX(CI.CI_NOM_LOGRADOURO_NORM) = SOUNDEX(CN.NOM_LOGRADOURO)
                AND 
                  CI.COD_MUNICIPIO = CN.COD_MUNICIPIO
              SET 
                CI.SC_ID_LOGRADOURO = CN.SC_ID_LOGRADOURO
              WHERE 
                (CI.COD_MUNICIPIO = '${cod_municipio}')
                  AND 
                (CI.SC_ID_LOGRADOURO IS NULL);`
    const resultado = await executaQuery(query);
    linhasAfetadasTotal += resultado.affectedRows;

    resp.detalhes.etapa_soundex = {
      descricao: "Nomes de logradouros coincidentes por SOUNDEX",
      linhas_afetadas: resultado.affectedRows
    };
  } catch (error) {
    resp.sucesso = false;
    resp.mensagem = "Erro na execução da Etapa SOUNDEX. Verifique a sintaxe ou dados.";
    resp.erro_detalhes = {
      etapa: "etapa_soundex",
      mensagem_original: error.message || String(error)
    };
    throw error;
  }

  let lev = await buscarECompararLogradouros(cod_municipio);

  // Finalização
  resp.mensagem = resp.sucesso ? "Atualização da tabela CI_LOGRADOUROS concluída com sucesso." : resp.mensagem;
  resp.total_linhas_afetadas = linhasAfetadasTotal;

  return resp;

}

// 🔹 Função para normalizar CI_LOGRADOUROS
async function normaliza_CI_LOGRADOUROS(cod_municipio) {
  console.log(`Normalizando CI_LOGRADOUROS`);
  let linhasModificadas = 0; // Variável para acumular o total de linhas afetadas
  let resp = {};

  const selectQuery = `
    SELECT 
      CI_NOM_LOGRADOURO 
    FROM CI_LOGRADOUROS
    WHERE 
      COD_MUNICIPIO = '${cod_municipio}';
  `;
  try {
    const linhas = await executaQuery(selectQuery);

    if (linhas.length === 0) {
      console.log(`Nenhuma linha encontrada para o município ${cod_municipio}.`);
      return { "sucesso": true, "mensagem": "Nenhuma linha para normalizar." };
    }

    for (var linha of linhas) {
      const nomLogradouroOriginal = linha.CI_NOM_LOGRADOURO;
      const normalizado = util.TrocaAbreviaturas(util.NormalizaString(nomLogradouroOriginal));

      const updateQuery = `
        UPDATE CI_LOGRADOUROS
        SET CI_NOM_LOGRADOURO_NORM = '${normalizado}'
        WHERE 
            CI_NOM_LOGRADOURO = '${nomLogradouroOriginal}' 
            AND COD_MUNICIPIO = '${cod_municipio}';
      `;
      const resultadoUpdate = await executaQuery(updateQuery);
      linhasModificadas += resultadoUpdate.changedRows;
    }

    resp = {
      "sucesso": true,
      "mensagem": "Tabela CI_LOGRADOUROS normalizada com sucesso.",
      "linhas modificadas": linhasModificadas
    }
    return resp;
  } catch (error) {
    console.log('Erro ao normalizar CI_LOGRADOUROS', error);
    throw error;
  }
}

/**
* Busca e compara logradouros de CI (apenas os já identificados) com CN, usando Levenshtein.
* * @param {string} cod_municipio - Código do município para filtrar as buscas.
* @returns {Array<Object>} Array com o CI_NOM, CN_NOM e a Distância Levenshtein.
*/
async function buscarECompararLogradouros(cod_municipio) {
  console.log(`Buscando logradouros identificados (${cod_municipio}) para comparação Levenshtein...`);

  // 1. Busca dos dados CI (COM FILTRO)
  const query_CI = `
        SELECT CI_NOM_LOGRADOURO_NORM
        FROM smuu.CI_LOGRADOUROS
        WHERE 
            COD_MUNICIPIO = '${cod_municipio}' AND
            SC_ID_LOGRADOURO IS NULL; -- AQUI ESTÁ O SEU FILTRO
    `;

  // 2. Busca dos dados CN (TODOS)
  const query_CN = `
        SELECT NOM_LOGRADOURO
        FROM smuu.CN_LOGRADOUROS
        WHERE 
            COD_MUNICIPIO = '${cod_municipio}';
    `;

  let ciLogradouros;
  let cnLogradouros;

  try {
    // Assume que executaQuery retorna um array de objetos [{ campo: valor }]
    ciLogradouros = await executaQuery(query_CI);
    cnLogradouros = await executaQuery(query_CN);

  } catch (error) {
    console.error('Erro ao buscar dados do MariaDB:', error);
    throw new Error('Falha ao acessar o banco de dados para comparação.');
  }

  if (ciLogradouros.length === 0 || cnLogradouros.length === 0) {
    console.log('Uma das bases de dados está vazia após o filtro. Nenhuma comparação realizada.');
    return [];
  }

  console.log(`Bases carregadas. CI: ${ciLogradouros.length} / CN: ${cnLogradouros.length}. Iniciando comparação N*M.`);

  const resultados = [];

  // 3. Comparação N*M (Produto Cartesiano)
  for (const itemCI of ciLogradouros) {
    const nomeCI = itemCI.CI_NOM_LOGRADOURO_NORM;

    for (const itemCN of cnLogradouros) {
      const nomeCN = itemCN.NOM_LOGRADOURO;

      // 4. Cálculo da Distância Levenshtein
      const distancia = util.LevenshteinDistance(nomeCI, nomeCN);

      resultados.push({
        CI_NOM_LOGRADOURO_NORM: nomeCI,
        NOM_LOGRADOURO: nomeCN,
        DISTANCIA_LEVENSHTEIN: distancia
      });
    }
  }

  // 5. Classifica os resultados (melhores matches primeiro)
  resultados.sort((a, b) => a.DISTANCIA_LEVENSHTEIN - b.DISTANCIA_LEVENSHTEIN);

  console.log('Comparação concluída.');
  //console.log(resultados);
  return (resultados);
}

// 🔹🔹🔹 CI_FACES 🔹🔹🔹
// Função para criar CI_FACES()
async function cria_CI_FACES() {
  console.log(`Criando CI_FACES`);
  const query = `
    CREATE TABLE IF NOT EXISTS CI_FACES (
      ID_FACE           VARCHAR(67),
      COD_MUNICIPIO     VARCHAR(7),
      ID_QUADRA         VARCHAR(50),
      SC_ID_LOGRADOURO  VARCHAR(8),
      QTD_LOTES         INT(11),
      DIM_FACE          DECIMAL(6,2)                                                                                                   
    );`
  try {
    await executaQuery(query);

    const resultado = {
      "sucesso": true,
      "mensagem": "Tabela CI_FACES criada com sucesso."
    }
    return resultado;
  } catch (error) {
    throw error;
  }
}

// 🔹 Função para preencher CI_FACES
async function preenche_CI_FACES() {
  console.log('Preenchendo CI_FACES');

  var resultado = '';
  const query = `
      INSERT INTO CI_FACES (
          ID_FACE, 
          COD_MUNICIPIO, 
          ID_QUADRA, 
          SC_ID_LOGRADOURO, 
          QTD_LOTES, 
          DIM_FACE
      )
      SELECT 
          CONCAT(LT.COD_MUNICIPIO, ";", TRIM(LT.NUM_QUADRA), ";", LG.SC_ID_LOGRADOURO) AS ID_FACE,
          LT.COD_MUNICIPIO, 
          LT.NUM_QUADRA AS ID_QUADRA, 
          LG.SC_ID_LOGRADOURO,
          COUNT(*) AS QTD_LOTES,
          SUM(LT.DIM_TESTADA) AS DIM_FACE
      FROM (
          /* Subconsulta para eliminar duplicatas de COD_UNICO_ENDERECO */
          SELECT DISTINCT 
              COD_MUNICIPIO, 
              NUM_QUADRA, 
              NOM_LOGRADOURO, 
              COD_UNICO_ENDERECO, 
              DIM_TESTADA
          FROM CI_LOTES
      ) LT
      JOIN CI_LOGRADOUROS LG 
          ON (LT.NOM_LOGRADOURO = LG.CI_NOM_LOGRADOURO)
          AND (LT.COD_MUNICIPIO = LG.COD_MUNICIPIO)
      GROUP BY 
          LT.COD_MUNICIPIO, 
          LT.NUM_QUADRA, 
          LG.SC_ID_LOGRADOURO;
  `;
  console.log('Executando carga de dados...');

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "Tabela CI_FACES preenchida com sucesso.",
      "linhas incluídas": resultado.affectedRows
    }
    console.log(`Carga concluída: ${resultado.affectedRows}`);
    return resp;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CI_FACES:', error);
    throw error;
  }
}

// 🔹🔹🔹 CI_ADJACENTES 🔹🔹🔹
// Função para criar CI_ADJACENTES()
async function cria_CI_ADJACENTES() {
  console.log(`Criando CI_ADJACENTES`);
  const query = `
    CREATE TABLE IF NOT EXISTS CI_ADJACENTES (
      ID_FACE           VARCHAR(67),
      COD_MUNICIPIO     VARCHAR(7),
      ID_QUADRA         VARCHAR(50),
      SC_ID_LOGRADOURO  VARCHAR(8),
      DIM_ADJACENTE     DECIMAL(6,2)                                                                                                   
    );
    `
  try {
    await executaQuery(query);

    const resultado = {
      "sucesso": true,
      "mensagem": "Tabela CI_ADJACENTES criada com sucesso."
    }
    return resultado;
  } catch (error) {
    throw error;
  }
}

// 🔹 Função para preencher CI_ADJACENTES
async function preenche_CI_ADJACENTES() {
  console.log('Preenchendo CI_ADJACENTES');

  var resultado = '';
  const query = `
--  Atualizando as faces em ADJACENTES
    INSERT INTO CI_ADJACENTES (
      ID_FACE, 
      COD_MUNICIPIO, 
      ID_QUADRA, 
      SC_ID_LOGRADOURO, 
      DIM_ADJACENTE
    )
    SELECT 
      CONCAT(
          LT.COD_MUNICIPIO, “;”, LT.NUM_QUADRA, “;”, LG.SC_ID_LOGRADOURO
        ) AS ID_FACE,
      LT.COD_MUNICIPIO, 
      LT.NUM_QUADRA AS ID_QUADRA, 
      LG.SC_ID_LOGRADOURO,
      SUM(LT.DIM_PROFUNDIDADE) AS DIM_ADJACENTE
    FROM (
      /* Subconsulta para eliminar duplicatas de COD_UNICO_ENDERECO */
      SELECT DISTINCT 
        COD_MUNICIPIO, 
        NUM_QUADRA, 
        NOM_LOGRADOURO_ADJACENTE, 
        COD_UNICO_ENDERECO, 
        DIM_PROFUNDIDADE
      FROM CI_LOTES
      WHERE 
        NOM_LOGRADOURO_ADJACENTE <> ""
    ) LT
    JOIN CI_LOGRADOUROS LG 
      ON (LT.NOM_LOGRADOURO_ADJACENTE = LG.CI_NOM_LOGRADOURO)
      AND (LT.COD_MUNICIPIO = LG.COD_MUNICIPIO)
    GROUP BY 
      LT.COD_MUNICIPIO, 
      LT.NUM_QUADRA, 
      LG.SC_ID_LOGRADOURO;
   `;
  console.log('Executando carga de dados...');

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "Tabela CI_ADJACENTES preenchida com sucesso.",
      "linhas incluídas": resultado.affectedRows
    }
    console.log(`Carga concluída: ${resultado.affectedRows}`);
    return resp;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CI_ADJACENTES:', error);
    throw error;
  }
}

// 🔹🔹🔹 CI_QUADRAS 🔹🔹🔹
// Função para criar CI_QUADRAS()
async function cria_CI_QUADRAS() {
  console.log(`Criando CI_QUADRAS`);
  const query = `
    CREATE TABLE IF NOT EXISTS CI_QUADRAS (
      ID_QUADRA         VARCHAR(50),
      COD_MUNICIPIO 	  VARCHAR(7),
      SC_ID_QUADRA 		  VARCHAR(250),
      QTD_PONTOS 		    INT,
      AREA 				      DECIMAL(10,2)
    );
    `;
  try {
    await executaQuery(query);

    const resultado = {
      "sucesso": true,
      "mensagem": "Tabela CI_QUADRAS criada com sucesso."
    }
    return resultado;
  } catch (error) {
    throw error;
  }
}

// 🔹 Função para preencher CI_QUADRAS
async function preenche_CI_QUADRAS() {
  console.log('Preenchendo CI_QUADRAS');

  var resultado = '';
  const query = `
      INSERT INTO CI_QUADRAS (
          ID_QUADRA, 
          COD_MUNICIPIO, 
          SC_ID_QUADRA, 
          QTD_PONTOS, 
          AREA
      )
      SELECT 
          ID_QUADRA,
          COD_MUNICIPIO,
          -- Constrói o array usando aspas duplas literais
          CONCAT('["', 
              GROUP_CONCAT(SC_ID_LOGRADOURO ORDER BY SC_ID_LOGRADOURO SEPARATOR '","'), 
          '"]') AS SC_ID_QUADRA,
          SUM(QTD_LOTES) AS QTD_PONTOS,
          0.00 AS AREA
      FROM CI_FACES
      GROUP BY 
          COD_MUNICIPIO, 
          ID_QUADRA;  
      `;
  console.log('Executando carga de dados...');

  try {
    const resultado = await executaQuery(query);

    const resp = {
      "sucesso": true,
      "mensagem": "Tabela CI_QUADRAS preenchida com sucesso.",
      "linhas incluídas": resultado.affectedRows
    }
    console.log(`Carga concluída: ${resultado.affectedRows}`);
    return resp;

  } catch (error) {
    console.error('Erro ao preencher dados na tabela CI_QUADRAS:', error);
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
  cria_CN_LOGRADOUROS,
  preenche_CN_LOGRADOUROS,
  cria_CN_FACES,
  preenche_CN_FACES,
  Complementa_CN_FACES,
  cria_CN_QUADRAS,
  preenche_CN_QUADRAS,
  complementa_CN_QUADRAS,
  cria_CI_LOTES,
  preenche_CI_LOTES,
  cria_CI_LOGRADOUROS,
  preenche_CI_LOGRADOUROS,
  normaliza_CI_LOGRADOUROS,
  atualiza_CI_LOGRADOUROS,
  cria_CI_FACES,
  preenche_CI_FACES,
  cria_CI_QUADRAS,
  preenche_CI_QUADRAS
};



