// server.js
const express = require("express");
const https = require("https");
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

// 🔹 Rota GET para testar conexão MySQL
app.get("/superciata/testa_conexao", async (req, res) => {
  try {
    await db.testaConexao();
    console.log("Conexão MySQL bem-sucedida!");
    res.json({ sucesso: true, mensagem: "Conexão MySQL OK!" });
  } catch (err) {
    console.error("Erro na conexão:", err.message);
    res.status(500).json({ sucesso: false, erro: err.message });
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
    res.status(500).json({
      sucesso: false,
      erro: err.message
    });
  }
});

// 🔹 Rota POST para upload do ZIP
app.post("/carrega_CNEFE", upload.single("arqCnefe"), async (req, res) => {
  try {
    const zipPath = req.file.path;

    await fs.createReadStream(zipPath)
      .pipe(unzipper.Extract({ path: CsvDir }))
      .promise();

    arquivos.deletaArquivo(zipPath);

    res.json({
      status: "ok",
      mensagem: `ZIP recebido e descompactado em ${CsvDir}`,
      pasta: CsvDir,
    });
  } catch (err) {
    console.error("Erro ao processar ZIP:", err);
    res.status(500).json({ status: "erro", mensagem: err.message });
  }
});

// Certificados HTTPS
const options = {
  key: fs.readFileSync("smuu.com.br.pem"),
  cert: fs.readFileSync("smuu.com.br.pem"),
};

// Start do servidor
https.createServer(options, app).listen(PORT, () => {
  console.log(`Servidor HTTPS rodando em https://localhost:${PORT}`);
});

// 🔹 Rota GET para carregar CSV
app.get("/superciata/preenche_CN_PONTOS", async (req, res) => {
  try {
    const { arquivo } = req.query;
    const cod_municipio = arquivo.substring(0, 7); // Código do município do IBGE com 7 algarismos

    console.log(`arquivo em Rota GET para carregar CSV: ${arquivo}`);

    if (!arquivo) {
      return res.status(400).json({
        sucesso: false,
        erro: "Parâmetro 'arquivo' é obrigatório",
        exemplo: "https://smuu.com.br:21079/superciata/carrega_CN_PONTOS?arquivo=4301800_BARRACAO.csv"
      });
    }

    const arquivoPath = path.join(CsvDir, arquivo);

    if (!fs.existsSync(arquivoPath)) {
      // Listar arquivos disponíveis para ajudar o usuário
      const arquivosDisponiveis = fs.readdirSync(CsvDir)
        .filter(f => f.endsWith('.csv'))
        .slice(0, 10); // Limitar a 10 arquivos

      return res.status(404).json({
        sucesso: false,
        erro: `Arquivo não encontrado: ${arquivoPath}`,
        arquivos_disponiveis: arquivosDisponiveis
      });
    }

    await db.executaQuery(`delete from CN_PONTOS where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros 

    console.log('🔄 Tentando carregar CSV...');
    const resultado = await db.carregaCSV(arquivoPath);

    arquivos.deletaArquivo(arquivoPath);

    res.json({
      sucesso: true,
      mensagem: `Arquivo ${arquivo} carregado com sucesso!`,
      caminho_arquivo: arquivoPath,
      linhas_afetadas: resultado.affectedRows,
      arquivo: arquivo
    });

  } catch (err) {
    console.error("Erro ao carregar arquivo CSV:", err);
    res.status(500).json({
      sucesso: false,
      erro: err.message,
      detalhes: "Verifique se o arquivo existe e tem formato CSV válido"
    });
  }
});

// 🔹 Rota GET para buscar pontos geográficos por código de município
app.get('/superciata/retorna_CN_PONTOS_UNICOS', async (req, res) => {

  const { cod_municipio } = req.query;

  if (!cod_municipio) {
    return res.status(400).json({
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
            WHERE COD_MUNICIPIO = ?
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
    res.status(500).json({
      sucesso: false,
      mensagem: 'Erro interno ao consultar o banco de dados.'
    });
  }
});

// Inicia o servidor na porta 21075, conforme o URL no código frontend
app.listen(PORT, () => {
  console.log(`Servidor de dados geográficos rodando em: https://smuu.com.br:${PORT}`);
});

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
    res.status(500).json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao criar o arquivo CN_PONTOS_UNICOS"
    });
  }
});

// 🔹 Rota GET para povoar a tabela CN_PONTOS_UNICOS
app.get("/superciata/preenche_CN_PONTOS_UNICOS", async (req, res) => {
  const { cod_municipio } = req.query;
  console.log(`cod_municipio = ${cod_municipio}`);

  try {
    const resultado = await db.executaQuery(`delete from CN_PONTOS_UNICOS where COD_MUNICIPIO = "${cod_municipio}";`); // Exclui registros do município antes de carregar os novos registros
    console.log(resultado);
  } catch (err) {
    console.log(err);
  }

  try {
    const resultado = db.preenche_CN_PONTOS_UNICOS();
    res.json({
      sucesso: true,
      detalhes: "Tabela CN_PONTOS_UNICOS OK",
      "resultado": JSON.stringify(resultado)
    });
  } catch (err) {
    console.log(err);
    res.status(500).json({
      sucesso: false,
      erro: err.message,
      detalhes: "Erro ao preencher o arquivo CN_PONTOS_UNICOS"
    });
  }
});
