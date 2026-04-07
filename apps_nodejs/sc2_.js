const express = require('express');
const mysql = require('mysql2/promise');
const { executarProcedimento } = require('./dbExecutor');

const app = express();
const port = 21134; // Porta de acesso que você definiu

app.use(express.json());
app.use(express.text({ type: 'application/sql' }));

// Configuração do BD smuu.com.br
const dbConfig = {
  host: 'mysql.smuu.com.br',
  user: 'smuu_add1',
  password: 'SmuuBd1',
  database: 'smuu',
  port: 3306,
};

// --- ROTAS DE TESTE E DIAGNÓSTICO ---

// Hello World
app.get('/sc2_2/hello', (req, res) => {
  res.json({
    projeto: "SuperCIATA 2.0",
    status: "online",
    acesso: "smuu.com.br/sc_2",
    timestamp: new Date().toLocaleString('pt-BR')
  });
});

// Teste de Conexão com o Banco
app.get('/sc2_/test_db', async (req, res) => {
  let connection;
  try {
    connection = await mysql.createConnection(dbConfig);
    const [rows] = await connection.query('SELECT VERSION() as versao');
    res.json({
      sucesso: true,
      banco: "MariaDB (mysql.smuu.com.br)",
      versao: rows[0].versao
    });
  } catch (err) {
    res.status(500).json({ sucesso: false, erro: err.message });
  } finally {
    if (connection) await connection.end();
  }
});

// --- ROTA EXECUTORA (MODULAR) ---

app.post('/sc2_/executar_lote', async (req, res) => {
  const sqlBatch = req.body;
  const nomeProc = req.headers['x-procedimento'] || 'Lote_SuperCIATA';

  if (!sqlBatch) {
    return res.status(400).json({ sucesso: false, erro: "Corpo SQL vazio." });
  }

  try {
    // Chama o executor modular que criamos (dbExecutor.js)
    await executarProcedimento(dbConfig, sqlBatch, nomeProc);
    
    res.json({
      sucesso: true,
      procedimento: nomeProc,
      msg: "Execução atômica concluída."
    });
  } catch (err) {
    // Nota: O dbExecutor usa process.exit(1) por padrão em erros críticos.
    // Para ambiente web, você pode alterar o dbExecutor para apenas dar 'throw err'.
    res.status(500).json({ sucesso: false, erro: err.message });
  }
});

app.listen(port, () => {
  console.log(`[SuperCIATA 2.0] Rodando em http://smuu.com.br:${port}/sc_2`);
});