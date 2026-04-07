// index.js
const express = require("express");
const https = require("https");
const http = require('http');
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const multer = require("multer");

// UNZIPPER
const unzipper = require("unzipper");
// Pasta temporária para armazenar os arquivos
const CsvDir = path.join(__dirname, "../arquivos/CSV");
if (!fs.existsSync(CsvDir)) {
  fs.mkdirSync(CsvDir);
}

const db = require("./db"); // importa o módulo mysql
const arquivos = require("./arquivos");
const { Console } = require("console");

const app = express();
const PORT = 21079;

// Pasta temporária para armazenar os arquivos
const tempDir = path.join(__dirname, "../arquivos/temp");
if (!fs.existsSync(tempDir)) {
  fs.mkdirSync(tempDir);
}

// Configuração do multer para salvar o arquivo ZIP em tempDir
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, tempDir),
  filename: (req, file, cb) => cb(null, file.originalname),
});
const upload = multer({ storage });

// Middlewares
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Certificados HTTPS
const options = {
  key: fs.readFileSync("smuu.com.br.key"),
  cert: fs.readFileSync("smuu.com.br.crt")
};

// Servidor em HTTPS
https.createServer(options, app).listen(PORT, () => {
    console.log(`Servidor HTTPS rodando em https://localhost:${PORT}`);
});
/*
https.createServer(options, app).listen(PORT, () => {
  console.log(`Servidor HTTPS rodando em https://localhost:${PORT}`);
});
*/
// 🔹 Rota GET para testar conexão MySQL
app.get("/", async (req, res) => {
    res.json({ sucesso: true, mensagem: `Porta ${PORT} funcionando` });
});

// 🔹 Rota GET para testar conexão MySQL
app.get("/superciata/testa_conexao", async (req, res) => {
  try {
    await db.testaConexao();
    console.log("Conexão MySQL bem-sucedida!");
    res.json({ sucesso: true, mensagem: "Conexão MySQL OK!" });
  } catch (err) {
    console.error("Erro na conexão:", err.message);
    res.json({ sucesso: false, erro: err.message });
  }
});

// 🔹 Rota GET para listar arquivos CSV disponíveis
app.get("/superciata/lista_arquivos", async (req, res) => {
  try {
    const lstArquivos = arquivos.listaArquivos(`/home/smuu/arquivos/temp`);

    res.json({
      lstArquivos
    });

  } catch (err) {
    console.error("Erro ao listar arquivos:", err);
    res.json({
      sucesso: false,
      erro: err.message
    });
  }
});


// 🔹 Rota GET para buscar faces por código de município
app.get('/superciata/retorna_CN_FACES', async (req, res) => {

  const { cod_municipio } = req.query;

  if (!cod_municipio) {
    return res.status(400).json({
      sucesso: false,
      mensagem: 'Parâmetro cod_municipio é obrigatório.'
    });
  }

  try {
    const sql = `
      SELECT 
        ID_FACE,
        ID_QUADRA,
        NOM_LOGRADOURO,
        QTD_PONTOS,
        ST_AsText(CENTROIDE) AS CENTROIDE
      FROM CN_FACES
      WHERE COD_MUNICIPIO = ?
    `;

    const resultados = await db.executaQuery(sql, [cod_municipio]);

    if (resultados.length === 0) {
      return res.status(404).json({
        sucesso: true,
        mensagem: `Nenhuma face encontrada para o município ${cod_municipio}.`,
        dados: []
      });
    }

    res.json(resultados);

  } catch (error) {
    console.error(`Erro ao buscar faces para ${cod_municipio}:`, error.message);
    res.json({
      sucesso: false,
      mensagem: 'Erro interno ao consultar o banco de dados.'
    });
  }
});

// 🔹 Rota POST para upload do ZIP
app.post("/superciata/carrega_arqZip", upload.single("arqZip"), async (req, res) => {
  try {
    const zipPath = req.file.path;

    await fs.createReadStream(zipPath)
      .pipe(unzipper.Extract({ path: CsvDir }))
      .promise();

    arquivos.deletaArquivo(zipPath);

    res.json({
      sucesso: true,
      mensagem: `ZIP recebido e descompactado em ${CsvDir}`,
    });
  } catch (err) {
    res.json({
      sucesso: false,
      mensagem: err.message,
    });
  }
});

// 🔹🔹🔹 CN_LOGRADOUROS 🔹🔹🔹
// 🔹 Rota GET para criar a tabela CN_LOGRADOUROS 
app.get('/superciata/cria_CN_LOGRADOUROS', async (req, res) => {
  try {
    const resultado = await db.cria_CN_LOGRADOUROS();
    res.json({
      sucesso: true,
      detalhes: "Tabela CN_LOGRADOUROS OK"
    });
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar o arquivo CN_LOGRADOUROS"
    });
  }
});

// 🔹 Rota GET para preencher CN_LOGRADOUROS
app.get('/superciata/preenche_CN_LOGRADOUROS', async (req, res) => {
  const { cod_municipio } = req.query;

  try {
    const resultado = await db.executaQuery(`delete from CN_LOGRADOUROS where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros
    console.log(resultado);
  } catch (err) {
    console.log(err);
  }

  try {
    const resultado = await db.preenche_CN_LOGRADOUROS(cod_municipio);
    res.json(resultado);
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao preencher o arquivo CN_LOGRADOUROS"
    });
  }
});

// 🔹🔹🔹 CN_FACES 🔹🔹🔹
// 🔹 Rota GET para criar a tabela CN_FACES com o centroide das faces
app.get('/superciata/cria_CN_FACES', async (req, res) => {
  try {
    const resultado = await db.cria_CN_FACES();
    res.json({
      sucesso: true,
      detalhes: "Tabela CN_FACES OK"
    });
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar o arquivo CN_FACES"
    });
  }
});

// 🔹 Rota GET para preencher CN_FACES
app.get('/superciata/preenche_CN_FACES', async (req, res) => {
  const { cod_municipio } = req.query;

  try {
    const resultado = await db.executaQuery(`delete from CN_FACES where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros
    console.log(resultado);
  } catch (err) {
    console.log(err);
  }

  try {
    const resultado = await db.preenche_CN_FACES();
    res.json(resultado);
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao preencher o arquivo CN_FACES"
    });
  }
});

// 🔹 Rota GET para complementar CN_FACES
app.get('/superciata/complementa_CN_FACES', async (req, res) => {
  const { cod_municipio} = req.query;

  try {
    const resultado = await db.Complementa_CN_FACES(`SC_ID_LOGRADOURO`);
    res.json(resultado);
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao complementar o arquivo CN_FACES"
    });
  }
});



// 🔹🔹🔹 CN_QUADRAS 🔹🔹🔹
// 🔹 Rota GET para criar a tabela CN_QUADRAS com o centroide das quadras
app.get('/superciata/cria_CN_QUADRAS', async (req, res) => {
  try {
    const resultado = await db.cria_CN_QUADRAS();
    res.json({
      sucesso: true,
      detalhes: "Tabela CN_QUADRAS OK"
    });
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar o arquivo CN_QUADRAS"
    });
  }
});

// 🔹 Rota GET para preencher CN_QUADRAS
app.get('/superciata/preenche_CN_QUADRAS', async (req, res) => {
  const { cod_municipio } = req.query;

  try {
    const resultado = await db.executaQuery(`delete from CN_QUADRAS where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros
    console.log(resultado);
  } catch (err) {
    console.log(err);
  }

  try {
    const resultado = await db.preenche_CN_QUADRAS();
    res.json(resultado);
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao preencher o arquivo CN_QUADRAS"
    });
  }
});


// 🔹 Rota GET para complementar CN_QUADRAS com a chave SC_ID_QUADRA
app.get('/superciata/complementa_CN_QUADRAS', async (req, res) => {
  const { cod_municipio } = req.query;

  try {
    const resultado = await db.complementa_CN_QUADRAS();
    res.json(resultado);
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao preencher CN_QUADRAS com SC_ID_QUADRA"
    });
  }
});

// 🔹 Rota GET para buscar quadras por código de município
app.get('/superciata/retorna_CN_QUADRAS', async (req, res) => {
  const { cod_municipio } = req.query;
  if (!cod_municipio) {
    return res.status(400).json({
      sucesso: false,
      mensagem: 'Parâmetro cod_municipio é obrigatório.'
    });
  }

  try {
    const sql = `
      SELECT 
        COD_MUNICIPIO,
        ID_QUADRA,
        QTD_PONTOS,
        ST_AsText(CENTROIDE) AS CENTROIDE
      FROM CN_QUADRAS
      WHERE COD_MUNICIPIO = ?
    `;

    const resultados = await db.executaQuery(sql, [cod_municipio]);

    if (resultados.length === 0) {
      return res.status(404).json({
        sucesso: true,
        mensagem: `Nenhuma quadra encontrada para o município ${cod_municipio}.`,
        dados: []
      });
    }

    res.json(resultados);

  } catch (error) {
    console.error(`Erro ao buscar quadras para ${cod_municipio}:`, error.message);
    res.json({
      sucesso: false,
      mensagem: 'Erro interno ao consultar o banco de dados.'
    });
  }
});


// 🔹🔹🔹 CN_PONTOS_UNICOS 🔹🔹🔹
// 🔹 Rota GET para criar a tabela CN_PONTOS_UNICOS
app.get("/superciata/cria_CN_PONTOS_UNICOS", async (req, res) => {
  try {
    const resultado = db.cria_CN_PONTOS_UNICOS();
    res.json({
      sucesso: true,
      detalhes: "Tabela CN_PONTOS_UNICOS OK"
    });
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar o arquivo CN_PONTOS_UNICOS"
    });
  }
});

// 🔹 Rota GET para povoar a tabela CN_PONTOS_UNICOS
app.get("/superciata/preenche_CN_PONTOS_UNICOS", async (req, res) => {
  const { cod_municipio } = req.query;

  try {
    const resultado = await db.executaQuery(`delete from CN_PONTOS_UNICOS where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros
    console.log(resultado);
  } catch (err) {
    console.log(err);
  }

  try {
    const resultado = await db.preenche_CN_PONTOS_UNICOS();
    res.json(resultado);
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao preencher o arquivo CN_PONTOS_UNICOS"
    });
  }
});

// 🔹 Rota GET para buscar pontos geográficos por código de município
app.get('/superciata/retorna_CN_PONTOS_UNICOS', async (req, res) => {

  const { cod_municipio } = req.query;

  if (!cod_municipio) {
    return res.json({
      sucesso: false,
      mensagem: 'Parâmetro cod_municipio é obrigatório.'
    });
  }

  try {
    // MySQL tem a função AS TEXT() ou ST_AsText() para converter o POINT em string.
    const sql = `
            SELECT 
                COD_MUNICIPIO,
                COD_UNICO_ENDERECO,
                NOM_LOGRADOURO,
                NUM_ENDERECO,
                ST_AsText(COORDS) AS COORDS 
            FROM CN_PONTOS_UNICOS
            WHERE COD_MUNICIPIO = ?;
        `;

    const resultados = await db.executaQuery(sql, [cod_municipio]);

    if (resultados.length === 0) {
      return res.status(404).json({
        sucesso: true,
        mensagem: `Nenhum ponto encontrado para o município ${cod_municipio}.`,
        dados: []
      });
    }
    res.json(resultados); // O frontend espera o array de objetos diretamente

  } catch (error) {
    console.error(`Erro ao buscar pontos para ${cod_municipio}:`, error.message);
    res.json({
      sucesso: false,
      mensagem: 'Erro interno ao consultar o banco de dados.'
    });
  }
});

// 🔹🔹🔹 CN_PONTOS 🔹🔹🔹
// 🔹 Rota GET para criar a tabela CN_PONTOS
app.get("/superciata/cria_CN_PONTOS", async (req, res) => {
  try {
    const resultado = db.cria_CN_PONTOS();
    res.json({
      sucesso: true,
      detalhes: "Tabela CN_PONTOS OK"
    });
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar o arquivo CN_PONTOS"
    });
  }
});

// 🔹 Rota GET para carregar CSV
app.get("/superciata/preenche_CN_PONTOS", async (req, res) => {
  try {
    const { nmArquivo, cod_municipio } = req.query;
    if (!nmArquivo) {
      return res.json({
        sucesso: false,
        erro: "Parâmetro 'arquivo' é obrigatório"
      });
    }
    const arquivoPath = path.join(CsvDir, nmArquivo);
    if (!fs.existsSync(arquivoPath)) {
      return res.json({
        sucesso: false,
        erro: `Arquivo não encontrado: ${arquivoPath}`
      });
    }

    if (db.tabelaExiste('CN_PONTOS')) {
      await db.executaQuery(`delete from CN_PONTOS where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros 
    }
    console.log('🔄 Tentando carregar CSV...');
    const resultado = await db.preenche_CN_PONTOS(arquivoPath);

    arquivos.deletaArquivo(arquivoPath);

    res.json({
      sucesso: true,
      mensagem: `Arquivo ${nmArquivo} carregado com sucesso!`,
      linhas_afetadas: resultado.affectedRows
    });

  } catch (err) {
    console.error("Erro ao carregar arquivo CSV:", err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Verifique se o arquivo existe e tem formato CSV válido"
    });
  }
});

// 🔹🔹🔹 CI_LOTES 🔹🔹🔹
// 🔹 Rota GET para criar a tabela CI_LOTES
app.get("/superciata/cria_CI_LOTES", async (req, res) => {
  try {
    const resultado = db.cria_CI_LOTES();
    res.json({
      sucesso: true,
      detalhes: "Tabela CI_LOTES OK"
    });
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar o arquivo CI_LOTES"
    });
  }
});

// 🔹 Rota GET para carregar CSV em CI_LOTES
app.get("/superciata/preenche_CI_LOTES", async (req, res) => {
  try {
    const { nmArquivo, cod_municipio } = req.query;
    console.log('preenche_CI_LOTES.cod_municipio', cod_municipio);

    if (!nmArquivo) {
      return res.json({
        sucesso: false,
        erro: "Parâmetro 'arquivo' é obrigatório"
      });
    }
    const arquivoPath = path.join(CsvDir, nmArquivo);
    if (!fs.existsSync(arquivoPath)) {
      return res.json({
        sucesso: false,
        erro: `Arquivo não encontrado: ${arquivoPath}`
      });
    }

    if (db.tabelaExiste('CI_LOTES')) {
      await db.executaQuery(`delete from CI_LOTES where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros 
    }
    console.log('🔄 Tentando carregar CSV...');
    const resultado = await db.preenche_CI_LOTES(arquivoPath);

    arquivos.deletaArquivo(arquivoPath);

    res.json({
      sucesso: true,
      mensagem: `Arquivo ${nmArquivo} carregado com sucesso!`,
      linhas_afetadas: resultado.affectedRows
    });

  } catch (err) {
    console.error("Erro ao carregar arquivo CSV:", err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Verifique se o arquivo existe e tem formato CSV válido"
    });
  }
});

// 🔹 Rota GET para criar a tabela CI_LOGRADOUROS
app.get("/superciata/cria_CI_LOGRADOUROS", async (req, res) => {
  try {
    const resultado = await db.cria_CI_LOGRADOUROS();
    res.json({
      sucesso: true,
      detalhes: "Tabela CI_LOGRADOUROS OK"
    });
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar a tabela CI_LOGRADOUROS"
    });
  }
});

// 🔹 Rota GET para carregar dados em CI_LOGRADOUROS
app.get("/superciata/preenche_CI_LOGRADOUROS", async (req, res) => {
  try {
    const { cod_municipio } = req.query;

    if (db.tabelaExiste('CI_LOGRADOUROS')) {
      console.log("cod_municipio", cod_municipio);
      await db.executaQuery(`delete from CI_LOGRADOUROS where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros 
    }
    console.log('🔄 Tentando carregar CSV...');
    const resultado = await db.preenche_CI_LOGRADOUROS();

    res.json({
      sucesso: true,
      mensagem: `CI_LOGRADOUROS carregado com sucesso!`
    });

  } catch (err) {
    console.error("Erro ao preencher CI_LOGRADOUROS:", err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao carregar CI_LOGRADOUROS"
    });
  }
});



// 🔹 Rota GET para carregar dados em CI_LOGRADOUROS
app.get("/superciata/normaliza_CI_LOGRADOUROS", async (req, res) => {
  try {
    const { cod_municipio } = req.query;

    const resultado = await db.normaliza_CI_LOGRADOUROS(cod_municipio);

    res.json(resultado);

  } catch (err) {
    console.error("Erro ao normalizar CI_LOGRADOUROS:", err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao carregar CI_LOGRADOUROS"
    });
  }
});

// 🔹 Rota GET para atualizar dados de CI_LOGRADOUROS
app.get("/superciata/atualiza_CI_LOGRADOUROS", async (req, res) => {
  try {
    const { cod_municipio } = req.query;

    const resultado = await db.atualiza_CI_LOGRADOUROS(cod_municipio);

    res.json(resultado);

  } catch (err) {
    console.error("Erro ao atualizar CI_LOGRADOUROS:", err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao atualizar CI_LOGRADOUROS"
    });
  }
});

// 🔹 Rota GET para criar a tabela CI_FACES
app.get("/superciata/cria_CI_FACES", async (req, res) => {
  try {
    const resultado = await db.cria_CI_FACES();
    res.json({
      sucesso: true,
      detalhes: "Tabela CI_FACES OK"
    });
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar a tabela CI_FACES"
    });
  }
});

// 🔹 Rota GET para carregar dados em CI_FACES
app.get("/superciata/preenche_CI_FACES", async (req, res) => {
  try {
    const { cod_municipio } = req.query;

    if (db.tabelaExiste('CI_FACES')) {
      await db.executaQuery(`delete from CI_FACES where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros 
    }
    const resultado = await db.preenche_CI_FACES();
    res.json(resultado);
  } catch (err) {
    console.error("Erro ao preencher CI_FACES:", err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao carregar CI_FACES"
    });
  }
});

// 🔹 Rota GET para criar a tabela CI_QUADRAS
app.get("/superciata/cria_CI_QUADRAS", async (req, res) => {
  try {
    const resultado = await db.cria_CI_QUADRAS();
    res.json({
      sucesso: true,
      detalhes: "Tabela CI_QUADRAS OK"
    });
  } catch (err) {
    console.log(err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar a tabela CI_QUADRAS"
    });
  }
});

// 🔹 Rota GET para carregar dados em CI_QUADRAS
app.get("/superciata/preenche_CI_QUADRAS", async (req, res) => {
  try {
    const { cod_municipio } = req.query;

    if (db.tabelaExiste('CI_QUADRAS')) {
      await db.executaQuery(`delete from CI_QUADRAS where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros 
    }
    const resultado = await db.preenche_CI_QUADRAS();
    res.json(resultado);
  } catch (err) {
    console.error("Erro ao preencher CI_QUADRAS:", err);
    res.json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao carregar CI_QUADRAS"
    });
  }
});

// 🔹 Rota GET para buscar os dois pontos mais próximos de um logradouro
app.get('/superciata/pontos_por_logradouro', async (req, res) => {
  const { sc_id, lat, lng } = req.query;

  try {
    // A query DEVE filtrar pelo sc_id recebido
    const sql = `
      SELECT 
        SC_ID_LOGRADOURO,
        NOM_LOGRADOURO,
        ST_AsText(GEOMETRIA) AS WKT
      FROM OSM_LOGRADOUROS
      WHERE SC_ID_LOGRADOURO = "${sc_id}" 
      LIMIT 1
    `;

    const resultados = await db.executaQuery(sql);

    if (resultados.length === 0) {
      return res.status(404).json({ sucesso: false, mensagem: 'Logradouro não encontrado no banco.' });
    }

    const logradouro = resultados[0];
    // A função abaixo fatia a linha e pega o segmento perto da lat/lng informada
    const segmento = extrairDoisPontosMaisProximos(logradouro.WKT, parseFloat(lat), parseFloat(lng));

    res.json({
      sucesso: true,
      sc_id: logradouro.SC_ID_LOGRADOURO,
      nome: logradouro.NOM_LOGRADOURO,
      segmento_proximo: segmento
    });

  } catch (error) {
    res.status(500).json({ sucesso: false, mensagem: error.message });
  }
});

/**
 * Auxiliar: Processa a string LINESTRING e encontra o segmento (2 pontos) 
 * cuja média de distância é a menor em relação ao ponto alvo.
 */
function extrairDoisPontosMaisProximos(wkt, targetLat, targetLng) {
  // Limpa a string: "LINESTRING(-53.1 -25.2, -53.2 -25.3)" -> "-53.1 -25.2, -53.2 -25.3"
  const coordsStr = wkt.replace('LINESTRING(', '').replace(')', '');
  const pares = coordsStr.split(',').map(p => {
    const [lon, lat] = p.trim().split(' ');
    return [parseFloat(lat), parseFloat(lon)];
  });

  let melhorSegmento = [];
  let menorDistancia = Infinity;

  // Itera sobre os segmentos (pares de pontos consecutivos)
  for (let i = 0; i < pares.length - 1; i++) {
    const p1 = pares[i];
    const p2 = pares[i+1];

    // Calcula a distância do ponto médio do segmento ao alvo (simplificado)
    const midLat = (p1[0] + p2[0]) / 2;
    const midLng = (p1[1] + p2[1]) / 2;
    const dist = Math.sqrt(Math.pow(targetLat - midLat, 2) + Math.pow(targetLng - midLng, 2));

    if (dist < menorDistancia) {
      menorDistancia = dist;
      melhorSegmento = [p1, p2];
    }
  }

  return melhorSegmento;
}