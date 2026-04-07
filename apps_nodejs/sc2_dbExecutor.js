const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const config = require('./sc2_config');

/**
 * Split inteligente: ignora ';' dentro de aspas simples ou duplas.
 * Essencial para comandos como FIELDS TERMINATED BY ';'.
 */
function splitComandosSQL(sql) {
    const regex = /;(?=(?:(?:[^"']*["']){2})*[^"']*$)/g;
    return sql.split(regex).map(c => c.trim()).filter(c => c.length > 0);
}

/**
 * Identifica a Operação e a Tabela alvo para o log analítico.
 */
function parseSQL(sql) {
    const partes = sql.trim().split(/\s+/);
    const operacao = partes[0] ? partes[0].toUpperCase() : 'DESCONHECIDO';
    let tabela = 'N/A';

    if (['SELECT', 'DELETE'].includes(operacao)) {
        const fromIndex = partes.indexOf('FROM');
        if (fromIndex !== -1) tabela = partes[fromIndex + 1];

    } else if (['INSERT', 'REPLACE', 'LOAD'].includes(operacao)) {
        if (operacao === 'LOAD') {
            const tableIndex = partes.indexOf('TABLE');
            tabela = (tableIndex !== -1) ? partes[tableIndex + 1] : 'DATA_FILE';
        } else {
            const intoIndex = partes.indexOf('INTO');
            tabela = (intoIndex !== -1) ? partes[intoIndex + 1] : partes[1];
        }

    } else if (operacao === 'UPDATE') {
        tabela = (partes[1] === 'IGNORE') ? partes[2] : partes[1];

    } else if (operacao === 'CREATE' || operacao === 'DROP') {
        let targetIndex = 1;
        if (partes.includes('TABLE')) targetIndex = partes.indexOf('TABLE') + 1;
        else if (partes.includes('VIEW')) targetIndex = partes.indexOf('VIEW') + 1;

        if (partes[targetIndex] === 'IF') {
            targetIndex += (partes[targetIndex + 1] === 'NOT') ? 3 : 2;
        }
        tabela = partes[targetIndex];
    }

    if (tabela) tabela = tabela.replace(/[`()]/g, '').split('.')[0];
    return { operacao, tabela };
}

/**
 * Função principal de execução.
 */
async function executa_sql(input, nomeProcedimento = 'SC2_Processamento') {
    let connection;
    let sqlContent = '';

    // Verifica se o input é um arquivo na pasta /sql ou SQL puro
    const isFilePath = typeof input === 'string' &&
        (input.endsWith('.sql') || fs.existsSync(path.join(config.paths.sqlFolder, input)));

    if (isFilePath) {
        const fullPath = path.isAbsolute(input) ? input : path.join(config.paths.sqlFolder, input);
        sqlContent = fs.readFileSync(fullPath, 'utf8');
    } else {
        sqlContent = input;
    }

    try {
        // Configuração da conexão com suporte a LOAD DATA LOCAL INFILE (v2.0)
        connection = await mysql.createConnection({
            ...config.db,
            multipleStatements: false,
            flags: ['+LOCAL_FILES'],
            infileStreamFactory: (filePath) => {
                console.log(`[STREAM] Lendo arquivo para o banco: ${filePath}`);
                return fs.createReadStream(filePath);
            }
        });

        await connection.beginTransaction();

        const comandos = splitComandosSQL(sqlContent);
        const logEntradas = [];
        const resumoFinal = [];

        for (let i = 0; i < comandos.length; i++) {
            const sql = comandos[i];
            const info = parseSQL(sql);
            const start = Date.now();

            const [result] = await connection.query(sql);

            // Adicione isso logo após a query do LOAD:
            if (info.operacao === 'LOAD') {
                console.log("⚠️ LOAD");
                const [warnings] = await connection.query("SHOW WARNINGS");
                if (warnings.length > 0) {
                    console.log("⚠️ Warnings do MariaDB:", warnings);
                }
            }

            const duration = Date.now() - start;

            let registros = result.affectedRows || (Array.isArray(result) ? result.length : 0);

            const logLine = `Passo ${i + 1} | OP: ${info.operacao} | TAB: ${info.tabela} | REG: ${registros} | ${duration}ms`;

            console.log(` ✅ ${logLine}`);
            logEntradas.push(`${new Date().toLocaleString('pt-BR')} - ${logLine}`);
            resumoFinal.push({ passo: i + 1, ...info, registros, duration });
        }

        await connection.commit();
        fs.appendFileSync(config.paths.logFile, logEntradas.join('\n') + '\n');

        return { sucesso: true, total_comandos: comandos.length, detalhes: resumoFinal };

    } catch (err) {
        if (connection) await connection.rollback();
        const erroMsg = `${new Date().toLocaleString('pt-BR')} [ERRO] ${err.message}`;
        fs.appendFileSync(config.paths.logFile, erroMsg + '\n');
        throw err;
    } finally {
        if (connection) await connection.end();
    }
}

module.exports = { executa_sql };