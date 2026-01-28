const express = require("express");
const https = require("https");
const fs = require("fs");

const app = express();
const PORT = 21044;

app.use(express.json());

app.get("/", (req, res) => {
  res.json({
    sucesso: true,
    mensagem: `Servidor Express HTTPS rodando na porta ${PORT}`
  });
});

app.post("/teste", (req, res) => {
  res.json({
    sucesso: true,
    recebido: req.body || null
  });
});

const options = {
  key: fs.readFileSync("./smuu.com.br.key"),
  cert: fs.readFileSync("./smuu.com.br.crt")
};

https.createServer(options, app).listen(PORT, "0.0.0.0", () => {
  console.log(`🔒 Express HTTPS ativo na porta ${PORT}`);
});
