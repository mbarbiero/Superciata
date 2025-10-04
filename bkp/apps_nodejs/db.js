// db.js
const mysql = require('mysql2/promise');

const dbConfig = {
  host: 'mysql.smuu.com.br',
  user: 'smuu_add1',
  password: 'SmuuBd1',
  database: 'smuu',
  port: 3306
};

// Função para testar conexão
async function testaConexao() {
  const connection = await mysql.createConnection(dbConfig);
  await connection.ping(); // envia ping ao servidor
  await connection.end();
  return true;
}

// Função para criar conexões
async function getConnection() {
  return mysql.createConnection(dbConfig);
}

// Ou, melhor ainda: usar pool de conexões
const pool = mysql.createPool(dbConfig);

module.exports = {
  testaConexao,
  getConnection,
  pool
};
