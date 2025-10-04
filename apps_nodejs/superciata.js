const fs = require('fs');
const express = require("express");
const https = require('https');
const app = express();
const PORT = 21041; 

// Carregar os arquivos do certificado
const options = {
  key: fs.readFileSync('smuu.com.br.pem'),
  cert: fs.readFileSync('smuu.com.br.pem')
};

https.createServer(options, app).listen(PORT, () => {
  console.log(`Servidor HTTPS rodando na porta ${PORT}`);
});

app.get("/", (req, res) => {
  res.send(`SuperCIATA.js - ${PORT}`);
});

/*
/carrega-cnefe
*/
app.get('/carrega-cnefe', (req, res) => {
  res.send("carrega-cnefe");
});