// config.js
const fs = require('fs');
const path = require('path');

const config = {
   db: {
      host: 'mysql.smuu.com.br',
      user: 'smuu_add1',
      password: 'SmuuBd1',
      database: 'smuu',
      port: 3306,
      connectionLimit: 10,
      // Configuração necessária para LOCAL INFILE no mysql2 v2.0+
      authPlugins: {
         mysql_clear_password: () => () => Buffer.from(`${dbConfig.password}\0`)
      },
      // IMPORTANTE: Configurar para usar LOCAL INFILE
      flags: ['+LOCAL_FILES', '+PLUGIN_AUTH'],
      queueLimit: 0,
      // Habilitar LOCAL INFILE
      enableKeepAlive: true,
      keepAliveInitialDelay: 0

   },
   server: {
      port: 21079,
      host: 'https://superciata.smuu.com.br'
   },
   paths: {
      sqlFolder: path.join(__dirname, 'sql'),
      logFile: path.join(__dirname, 'logs', 'execucao.log'),
      logDir: path.join(__dirname, 'logs')
   }
};

// Auto-Setup de diretórios
[config.paths.sqlFolder, config.paths.logDir].forEach(dir => {
   if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
   }
});
module.exports = config;