// config.js
const fs = require('fs');
const path = require('path');

// Defina as credenciais antes para evitar erros de referência circular
const DB_PASSWORD = 'SmuuBd1';

const sc2_Config = {
   db: {
      host: 'mysql.smuu.com.br',
      user: 'smuu_add1',
      password: DB_PASSWORD, // Usa a constante definida acima
      database: 'smuu',
      port: 3306,
      connectionLimit: 10,
      
      // Correção do Plugin de Autenticação
      authPlugins: {
         mysql_clear_password: () => () => Buffer.from(DB_PASSWORD + '\0')
      },
      
      // Flags para LOCAL INFILE e plugins
      flags: ['+LOCAL_FILES', '+PLUGIN_AUTH'],
      
      queueLimit: 0,
      enableKeepAlive: true,
      keepAliveInitialDelay: 0
   },
   server: {
      port: 21079,
      // Geralmente em configs de backend, o host é o IP (0.0.0.0) ou 'localhost'
      // O domínio público fica para o CORS ou referências de redirecionamento
      host: 'https://superciata.smuu.com.br' 
   },
   paths: {
      sqlFolder: path.join(__dirname, 'sql'),
      logFile: path.join(__dirname, 'logs', 'execucao.log'),
      logDir: path.join(__dirname, 'logs'),
      csvDir: path.join('/home/smuu/arquivos/CSV')
   }
};

// Auto-Setup de diretórios (Mantido seu fluxo original)
[sc2_Config.paths.sqlFolder, sc2_Config.paths.logDir].forEach(dir => {
   if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
   }
});

module.exports = sc2_Config;
