
const cors = require('cors');
const db = require('./db');
const fs = require('fs');
const path = require('path');


/*
#######################################################
MYSQL
#######################################################*/
// const pastaCSV = path.join(__dirname, pastaFinal);
const mysql = require('mysql2');
const mysql_p = require('mysql2/promise');
const dbConfig = {
  host: 'mysql.smuu.com.br',
  user: 'smuu_add1',
  password: 'SmuuBd1',
  database: 'smuu',
  port: 3306
};


/*
#######################################################
EXPRESS
#######################################################*/
// Criar uma aplicação Express
const express = require('express');
const app = express();
const PORT = 21067; // Porta do servidor

const server = app.listen(21067, () => {
  console.log('Server started on port 21067');
  server.timeout = 30 * 60 * 1000; // 30 minutos
});



/*
#######################################################
MULTER
#######################################################*/
const multer = require('multer');
const upload = multer({
  dest: 'temp/',
  limits: {
    fileSize: 100 * 1024 * 1024, // 100MB
    fieldSize: 100 * 1024 * 1024 // 100MB
  }
});

// Rota principal que retorna "Hello World!"
app.get('/superciata/', (req, res) => {
  res.send('Hello World!');
});

app.get('/superciata/testar-conexao', (req, res) => {
  const connection = mysql.createConnection(dbConfig);

  connection.connect((err) => {
    console.log('Testando conexão');
    if (err) {
      console.error('Erro na conexão:', err.message);
      return res.status(500).json({ sucesso: false, erro: err.message });
    }

    console.log('Conexão MySQL bem-sucedida!');
    connection.end();
    res.json({ sucesso: true, mensagem: 'Conexão MySQL OK!' });
  });
});

app.get('/superciata/importa-cnefe', (req, res) => {
  const connection = mysql.createConnection(dbConfig);

  // Consulta SQL (ajuste o nome da tabela conforme necessário)
  const sql = `LOAD DATA INFILE '/home/smuu/apps_nodejs/temp/4301800_BARRACAO.csv'
INTO TABLE PONTOS_CNEFE
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;`; // Limita a 100 registros para evitar sobrecarga

  connection.query(sql, (err, results) => {
    connection.end(); // Fecha a conexão após a consulta

    if (err) {
      console.error('Erro na consulta:', err.message);
      return res.status(500).json({ sucesso: false, erro: err.message });
    }

    console.log(`Dados da cnefe retornados: ${results.length} registros`);
    res.json({ dados: results });
    //res.json({ sucesso: true, dados: results });
  });
});

app.get('/superciata/cnefe', (req, res) => {
  const connection = mysql.createConnection(dbConfig);

  // Consulta SQL (ajuste o nome da tabela conforme necessário)
  const sql = 'SELECT * FROM PONTOS_CNEFE LIMIT 100'; // Limita a 100 registros para evitar sobrecarga

  connection.query(sql, (err, results) => {
    connection.end(); // Fecha a conexão após a consulta

    if (err) {
      console.error('Erro na consulta:', err.message);
      return res.status(500).json({ sucesso: false, erro: err.message });
    }

    console.log(`Dados da cnefe retornados: ${results.length} registros`);
    res.json({ dados: results });
    //res.json({ sucesso: true, dados: results });
  });
});

/*
#######################################################
CentroideFacesGeojson
#######################################################*/
app.get('/superciata/retCentroideFaces', (req, res) => {
  const connection = mysql.createConnection(dbConfig);
  var qtd = 0;
  const codMunicipio = req.query.COD_MUNICIPIO;

  // A tabela cnefe valores nas colunas de latitude/longitude
  const sql = `
    SELECT DISTINCT
      ID_QUADRA,
      NUM_FACE,
      NOM_LOGRADOURO,
      LATITUDE_CENTROIDE,
      LONGITUDE_CENTROIDE      
    FROM 
      CN_CENTROIDES_FACES
    WHERE 
      COD_MUNICIPIO = '${codMunicipio}' 
        AND
      LATITUDE_CENTROIDE IS NOT NULL AND LONGITUDE_CENTROIDE IS NOT NULL
    ORDER BY
      ID_QUADRA, 
      NUM_FACE
    `;

  connection.query(sql, (err, results) => {
    connection.end();

    if (err) {
      return res.status(500).json({ erro: err.message });
    }

    // Converter resultados para GeoJSON
    const geojson = {
      type: "FeatureCollection",
      features: results.map(item => ({
        type: "Feature",
        properties: {
          num_quadra: item.ID_QUADRA,
          num_face: item.NUM_FACE,
          nom_logradouro: item.NOM_LOGRADOURO,
          coords: [parseFloat(item.LONGITUDE_CENTROIDE), parseFloat(item.LATITUDE_CENTROIDE)]
        },
        geometry: {
          type: "Point",
          coordinates: [parseFloat(item.LONGITUDE_CENTROIDE), parseFloat(item.LATITUDE_CENTROIDE)]
        }
      }))
    };

    res.json(geojson);
  });
});


/*
#######################################################
retCentroideQuadras
#######################################################*/
app.get('/superciata/retCentroideQuadras', (req, res) => {
  const connection = mysql.createConnection(dbConfig);
  var qtd = 0;
  const codMunicipio = req.query.COD_MUNICIPIO;

  // A tabela cnefe valores nas colunas de latitude/longitude
  const sql = `
    SELECT DISTINCT
      COD_MUNICIPIO,
      ID_QUADRA,
      LATITUDE_CENTROIDE,
      LONGITUDE_CENTROIDE      
    FROM 
      CN_CENTROIDES_QUADRAS
    WHERE 
      COD_MUNICIPIO = '${codMunicipio}' 
        AND
      LATITUDE_CENTROIDE IS NOT NULL AND LONGITUDE_CENTROIDE IS NOT NULL
    ORDER BY
      ID_QUADRA
    `;

  console.log(sql);

  connection.query(sql, (err, results) => {
    connection.end();

    if (err) {
      return res.status(500).json({ erro: err.message });
    }

    // Converter resultados para GeoJSON
    const geojson = {
      type: "FeatureCollection",
      features: results.map(item => ({
        type: "Feature",
        properties: {
          num_quadra: item.ID_QUADRA
        },
        geometry: {
          type: "Point",
          coordinates: [parseFloat(item.LONGITUDE_CENTROIDE), parseFloat(item.LATITUDE_CENTROIDE)]
        }
      }))
    };

    res.json(geojson);
  });
});

/*
#######################################################
carrega-cnefe
#######################################################*/
const Busboy = require('busboy');

// Criar diretório temp se não existir
const tempDir = path.join(__dirname, 'temp');
if (!fs.existsSync(tempDir)) {
  fs.mkdirSync(tempDir, { recursive: true });
}

app.post("/carrega-cnefe", (req, res) => {
  console.log('=== NOVA REQUISIÇÃO ===');
  console.log('Method:', req.method);
  console.log('URL:', req.url);
  console.log('Content-Type:', req.headers['content-type']);
  console.log('Content-Length:', req.headers['content-length']);
  console.log('User-Agent:', req.headers['user-agent']);

  const bb = Busboy({
    headers: req.headers,
    limits: {
      fileSize: 1024 * 1024 * 1024, // 1GB
      files: 5,
      fields: 10
    }
  });

  let fileName = '';
  let fileSize = 0;
  let writeStream = null;
  let startTime = Date.now();

  bb.on("file", (name, file, info) => {
    console.log('--- FILE EVENT ---');
    console.log('Field name:', name);
    console.log('File info:', info);

    fileName = info.filename;
    const saveTo = path.join(tempDir, `${Date.now()}_${fileName}`);

    console.log('Save to:', saveTo);

    writeStream = fs.createWriteStream(saveTo);

    writeStream.on('open', () => {
      console.log('Write stream opened');
    });

    writeStream.on('close', () => {
      console.log('Write stream closed');
    });

    writeStream.on('error', (err) => {
      console.error('Write stream error:', err);
    });

    file.on("data", (chunk) => {
      fileSize += chunk.length;
      const elapsed = (Date.now() - startTime) / 1000;
      const speed = (fileSize / 1024 / 1024 / elapsed).toFixed(2);

      console.log(`Chunk: ${chunk.length} bytes | Total: ${fileSize} bytes | Speed: ${speed} MB/s`);
    });

    file.on("end", () => {
      console.log('File stream ended - Total size:', fileSize);
    });

    file.on("error", (err) => {
      console.error('File stream error:', err);
    });

    // Pipe para o writeStream
    file.pipe(writeStream);
  });

  bb.on("field", (name, value) => {
    console.log(`Field [${name}]: ${value}`);
  });

  bb.on("finish", () => {
    console.log('Busboy finish event');
  });

  bb.on("error", (err) => {
    console.error('--- BUSBOY ERROR ---');
    console.error(err);
    if (writeStream) {
      writeStream.destroy();
    }
    res.status(500).json({
      error: 'Erro no processamento do upload',
      message: err.message
    });
  });

  bb.on("close", () => {
    console.log('--- BUSBOY CLOSE ---');
    console.log('Final file size:', fileSize);
    console.log('Time taken:', (Date.now() - startTime) / 1000, 'seconds');

    if (writeStream) {
      writeStream.end();
    }

    res.json({
      ok: true,
      fileName: fileName,
      fileSize: fileSize,
      savedPath: path.join(tempDir, fileName),
      uploadTime: (Date.now() - startTime) / 1000
    });
  });

  // Log do pipe
  console.log('Starting pipe...');
  req.pipe(bb);

  // Monitorar a requisição original
  let reqBytes = 0;
  req.on('data', (chunk) => {
    reqBytes += chunk.length;
    console.log('Req data:', chunk.length, 'bytes - Total req:', reqBytes);
  });

  req.on('end', () => {
    console.log('Req end - Total request bytes:', reqBytes);
  });
});

