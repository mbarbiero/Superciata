const cors = require('cors');
const path = require('path');
const fs = require('fs');
const fs_p = require('fs/promises');
const db = require('./db');
//const { importaPontosCnefe } = require('./Importador');
const csv = require('csv-parser');
const { performance } = require('perf_hooks');
const multer = require('multer');

/*
#######################################################
EXPRESS
#######################################################*/
const express = require('express');
const app = express();
const PORT = 21067;
app.use(cors()); // Permitir chamadas do navegador

/*
#######################################################
MYSQL
#######################################################*/
const pastaCSV = path.join(__dirname, pastaFinal);
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
MULTER
#######################################################*/
const upload = multer({
  dest: 'uploads/cnefe/', // Pasta onde os arquivos serão salvos
  limits: {
    fileSize: 100 * 1024 * 1024 // Limite de 100MB
  }
});

/*
#######################################################
CriaTabPontosCnefe
#######################################################*/
app.get('/ciata/CriaTabPontosCnefe', async (req, res) => {
  res.json(await db.CriaTabPontosCnefe());
});

/*
#######################################################
CriaPontosUnicosCnefe
#######################################################*/
app.get('/ciata/CriaPontosUnicosCnefe', async (req, res) => {
  res.json(await db.CriaPontosUnicosCnefe());
});

/*
#######################################################
CriaCentroidesFacesCnefe
#######################################################*/
app.get('/ciata/CriaCentroidesFacesCnefe', async (req, res) => {
  res.json(await db.CriaCentroidesFacesCnefe());
});

/*
#######################################################
CriaCentroidesQuadrasCnefe
#######################################################*/
app.get('/ciata/CriaCentroidesQuadrasCnefe', async (req, res) => {
  res.json(await db.CriaCentroidesQuadrasCnefe());
});

/*
#######################################################
ImportaCSV
#######################################################*/
app.get('/ciata/ImportaCSV', async (req, res) => {
  try {
    const arquivos = await fs_p.readdir(pastaCSV);
    const arquivosCsv = arquivos.filter(arquivo => arquivo.endsWith('.csv'));

    if (arquivosCsv.length === 0) {
      return res.status(404).send('Nenhum arquivo CSV encontrado na pasta.');
    }

    const connection = await mysql_p.createConnection(dbConfig);
    let resultados = [];
    let erros = [];

    for (const nomeArquivo of arquivosCsv) {
      const caminhoCompleto = path.join(pastaCSV, nomeArquivo);
      let dados = [];

      await new Promise((resolve, reject) => {
        fs.createReadStream(caminhoCompleto)
          .pipe(csv({ separator: ';' }))
          .on('data', (linha) => dados.push(linha))
          .on('end', resolve)
          .on('error', reject);
      });

      const insertQuery = `
        INSERT INTO PONTOS_CNEFE (
          COD_UNICO_ENDERECO, COD_UF, COD_MUNICIPIO, COD_DISTRITO, COD_SUBDISTRITO,
          COD_SETOR, NUM_QUADRA, NUM_FACE, CEP, DSC_LOCALIDADE,
          NOM_TIPO_SEGLOGR, NOM_TITULO_SEGLOGR, NOM_SEGLOGR, NUM_ENDERECO, DSC_MODIFICADOR,
          NOM_COMP_ELEM1, VAL_COMP_ELEM1, NOM_COMP_ELEM2, VAL_COMP_ELEM2, NOM_COMP_ELEM3,
          VAL_COMP_ELEM3, NOM_COMP_ELEM4, VAL_COMP_ELEM4, NOM_COMP_ELEM5, VAL_COMP_ELEM5,
          LATITUDE, LONGITUDE, NV_GEO_COORD, COD_ESPECIE, DSC_ESTABELECIMENTO,
          COD_INDICADOR_ESTAB_ENDERECO, COD_INDICADOR_CONST_ENDERECO, COD_INDICADOR_FINALIDADE_CONST,
          COD_TIPO_ESPECI, COORDS_PONTO
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_PointFromText(?))
      `;

      for (const linha of dados) {

        const point = `POINT(${linha.LONGITUDE} ${linha.LATITUDE})`;

        const values = [
          linha.COD_UNICO_ENDERECO, linha.COD_UF, linha.COD_MUNICIPIO, linha.COD_DISTRITO, linha.COD_SUBDISTRITO,
          linha.COD_SETOR, linha.NUM_QUADRA, linha.NUM_FACE, linha.CEP, linha.DSC_LOCALIDADE,
          linha.NOM_TIPO_SEGLOGR, linha.NOM_TITULO_SEGLOGR, linha.NOM_SEGLOGR, linha.NUM_ENDERECO, linha.DSC_MODIFICADOR,
          linha.NOM_COMP_ELEM1, linha.VAL_COMP_ELEM1, linha.NOM_COMP_ELEM2, linha.VAL_COMP_ELEM2, linha.NOM_COMP_ELEM3,
          linha.VAL_COMP_ELEM3, linha.NOM_COMP_ELEM4, linha.VAL_COMP_ELEM4, linha.NOM_COMP_ELEM5, linha.VAL_COMP_ELEM5,
          parseFloat(linha.LATITUDE), parseFloat(linha.LONGITUDE), linha.NV_GEO_COORD, linha.COD_ESPECIE, linha.DSC_ESTABELECIMENTO,
          linha.COD_INDICADOR_ESTAB_ENDERECO, linha.COD_INDICADOR_CONST_ENDERECO, linha.COD_INDICADOR_FINALIDADE_CONST,
          linha.COD_TIPO_ESPECI, point
        ];
        console.log(values);

        try {
          await connection.execute(insertQuery, values);
        } catch (erroInsercao) {
          console.error(`Erro ao inserir linha com COD_UNICO_ENDERECO=${linha.COD_UNICO_ENDERECO}:`, erroInsercao.code);
          erros.push({
            codigo: linha.COD_UNICO_ENDERECO,
            erro: erroInsercao.code,
            mensagem: erroInsercao.message
          });
          continue; // continua para próxima linha
        }
      }

      await fs_p.unlink(caminhoCompleto); // Apaga o CSV
      resultados.push(`Arquivo ${nomeArquivo} importado e removido.`);
    }

    await connection.end();

    if (erros.length > 0) {
      res.status(207).send({
        mensagem: 'Importação concluída com alguns erros.',
        resultados,
        erros
      });
    } else {
      res.send(resultados.join('\n'));
    }
  } catch (err) {
    console.error('Erro na importação:', err);
    res.status(500).send('Erro ao importar os arquivos CSV.');
  }
});

app.get('/ciata/testar-conexao', (req, res) => {
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

app.get('/ciata/cnefe', (req, res) => {
  const connection = mysql.createConnection(dbConfig);

  // Consulta SQL (ajuste o nome da tabela conforme necessário)
  const sql = 'SELECT * FROM cnefe LIMIT 100'; // Limita a 100 registros para evitar sobrecarga

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

app.get('/ciata/retPontosUnicosCnefe', (req, res) => {
  const connection = mysql.createConnection(dbConfig);
  var qtd = 0;

  const codMunicipio = req.query.COD_MUNICIPIO;

  // A tabela cnefe valores nas colunas de latitude/longitude
  const sql = `
    SELECT 
      ID_QUADRA,
      NUM_FACE,
      LATITUDE,
      LONGITUDE,
      NOM_LOGRADOURO,
      NUM_ENDERECO
    FROM 
      PONTOS_UNICOS_CNEFE
    WHERE 
      COD_MUNICIPIO = '${codMunicipio}' 
        AND
      LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
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
          num_endereco: item.NUM_ENDERECO
        },
        geometry: {
          type: "Point",
          coordinates: [parseFloat(item.LONGITUDE), parseFloat(item.LATITUDE)]
        }
      }))
    };

    res.json(geojson);
  });
});

app.get('/ciata/quadras-cnefe-geojson', (req, res) => {
  const connection = mysql.createConnection(dbConfig);
  var qtd = 0;

  const codMunicipio = req.query.COD_MUNICIPIO;

  // A tabela cnefe valores nas colunas de latitude/longitude
  const sql = `
    SELECT DISTINCT
      NUM_QUADRA,
      NUM_FACE,
      latitude,
      longitude,
      NOM_LOGRADOURO,
      NUM_ENDERECO
    FROM 
      cnefe
    WHERE 
      COD_MUNICIPIO = '${codMunicipio}' 
        AND
      latitude IS NOT NULL AND longitude IS NOT NULL
    ORDER BY
      NUM_QUADRA, NUM_FACE
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
          num_quadra: item.NUM_QUADRA,
          num_face: item.NUM_FACE,
          nom_logradouro: item.NOM_LOGRADOURO,
          num_endereco: item.NUM_ENDERECO
        },
        geometry: {
          type: "Point",
          coordinates: [parseFloat(item.longitude), parseFloat(item.latitude)]
        }
      }))
    };

    res.json(geojson);
  });
});

/*
#######################################################
CentroideFacesGeojson
#######################################################*/
app.get('/ciata/retCentroideFaces', (req, res) => {
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
      CENTROIDES_FACES_CNEFE
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
app.get('/ciata/retCentroideQuadras', (req, res) => {
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
      CENTROIDES_QUADRAS_CNEFE
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
retPoligonosQuadras
#######################################################*/
app.get('/ciata/retPoligonosQuadras', (req, res) => {
  const connection = mysql.createConnection(dbConfig);
  var qtd = 0;

  console.log(req.query);

  const codMunicipio = req.query.cod_municipio;

  // A tabela cnefe valores nas colunas de latitude/longitude
  const sql = `
      SELECT 
        NUM_QUADRA,
        ST_AsGeoJSON(geom) AS geojson
      FROM 
        CNEFE_QUADRAS
      WHERE 
        COD_MUNICIPIO = '${codMunicipio}' 
        AND geom IS NOT NULL
      ORDER BY 
        NUM_QUADRA;
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
          num_quadra: item.NUM_QUADRA
        },
        geometry: JSON.parse(item.geojson) // 'geojson' é string JSON da geometria
      }))
    };

    res.json(geojson);
  });
});

app.get('/ciata/cnefe-minmax', (req, res) => {
  const connection = mysql.createConnection(dbConfig);
  var qtd = 0;

  const codMunicipio = req.query.COD_MUNICIPIO;
  const codQuadra = req.query.COD_QUADRA;

  // A tabela cnefe valores nas colunas de latitude/longitude
  const sql = `
    SELECT 
      min(latitude) as minLat,
      min(longitude) as minLon,
      max(latitude) as maxLat,
      max(longitude) as maxLon
    FROM 
      cnefe
    WHERE 
      COD_MUNICIPIO = '${codMunicipio}' 
        AND
      NUM_QUADRA = '${codQuadra}'
        AND
      latitude IS NOT NULL AND longitude IS NOT NULL
    `;

  connection.query(sql, (err, results) => {
    connection.end();

    if (err) {
      return res.status(500).json({ erro: err.message });
    }


    // Converter resultados para GeoJSON
    const geojson = {
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          properties: {
            num_quadra: codQuadra
          },
          geometry: {
            type: "Polygon",
            coordinates: [[
              [parseFloat(results[0].minLon), parseFloat(results[0].minLat)],
              [parseFloat(results[0].maxLon), parseFloat(results[0].minLat)],
              [parseFloat(results[0].maxLon), parseFloat(results[0].maxLat)],
              [parseFloat(results[0].minLon), parseFloat(results[0].maxLat)],
              [parseFloat(results[0].minLon), parseFloat(results[0].minLat)]
            ]]
          }
        }
      ]
    };
    res.json(geojson);
  });
});

/*
#######################################################
carrega-cnefe
#######################################################*/
app.post("/upload-cnefe", upload.single("upload-cnefe"), (req, res) => {
  
  // Verifica se o arquivo foi recebido
  if (!req.file) {
    return res.status(400).json({
      success: false,
      message: "Nenhum arquivo foi enviado"
    });
  }

  // Retorna informações básicas do arquivo
  res.json({
    success: true,
    message: "Arquivo recebido com sucesso!",
    file: {
      originalName: req.file.originalname,
      size: req.file.size,
      filename: req.file.filename,
      path: req.file.path
    }
  });
});

app.listen(PORT, () => {
  console.log('Começando');
  console.log(`Backend rodando em http://localhost:${PORT}`);
});


