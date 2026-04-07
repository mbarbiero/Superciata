const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const config = require('./sc2_config');

// Função auxiliar para extrair Operação e Tabela
function parseSQL(sql) {
    const limpo = sql.trim().toUpperCase();
    const partes = limpo.split(/\s+/);
    const operacao = partes[0] || 'DESCONHECIDO';
    
    let tabela = 'N/A';
    
    // Lógica simples para identificar a tabela alvo
    if (['SELECT', 'DELETE'].includes(operacao)) {
        const fromIndex = partes.indexOf('FROM');
        if (fromIndex !== -1) tabela = partes[fromIndex + 1];
    } else if (['INSERT', 'REPLACE'].includes(operacao)) {
        const intoIndex = partes.indexOf('INTO');
        tabela = (intoIndex !== -1) ? partes[intoIndex + 1] : partes[1];
    } else if (operacao === 'UPDATE') {
        tabela = partes[1];
    } else if (operacao === 'CREATE' || operacao === 'DROP') {
        tabela = partes[2]; // Ex: CREATE TABLE [tabela]
    }

    // Remove caracteres especiais como parênteses ou crases do nome da tabela
    tabela = tabela.replace(/[`()]/g, '').split('.')[0]; 

    return { operacao, tabela };
}

async function executa_sql(input, nomeProcedimento = 'SC2_Lote_Automatico') {
    let connection;
    let sqlContent = '';
    
    // Identificação de fonte (Arquivo vs Texto)
    const isFilePath = typeof input === 'string' && 
                       (input.endsWith('.sql') || fs.existsSync(path.join(config.paths.sqlFolder, input)));

    if (isFilePath) {
        const fullPath = path.isAbsolute(input) ? input : path.join(config.paths.sqlFolder, input);
        sqlContent = fs.readFileSync(fullPath, 'utf8');
    } else {
        sqlContent = input;
    }

    try {
        connection = await mysql.createConnection(config.db);
        await connection.beginTransaction();

        const comandos = sqlContent.split(';').map(c => c.trim()).filter(c => c.length > 0);
        const logEntradas = [];
        const resumoFinal = [];

        console.log(`\n[INICIANDO LOTE: ${nomeProcedimento}]`);

        for (let i = 0; i < comandos.length; i++) {
            const sql = comandos[i];
            const info = parseSQL(sql);
            const start = Date.now();
            
            const [result] = await connection.query(sql);
            const duration = Date.now() - start;

            // Define a contagem de registros baseada no tipo de resultado
            let registros = 0;
            if (result.affectedRows !== undefined) registros = result.affectedRows;
            else if (Array.isArray(result)) registros = result.length;

            const logLine = `Passo ${i + 1} | OP: ${info.operacao} | TABELA: ${info.tabela} | REGISTROS: ${registros} | TEMPO: ${duration}ms`;
            
            console.log(` ✅ ${logLine}`);
            logEntradas.push(`${new Date().toLocaleString('pt-BR')} - ${logLine}`);
            resumoFinal.push({ passo: i + 1, ...info, registros, duration });
        }

        await connection.commit();
        
        // Escrita persistente no arquivo de log
        fs.appendFileSync(config.paths.logFile, logEntradas.join('\n') + '\n');
        
        return { 
            sucesso: true, 
            procedimento: nomeProcedimento,
            total_comandos: comandos.length,
            detalhes: resumoFinal 
        };

    } catch (err) {
        if (connection) await connection.rollback();
        const erroMsg = `${new Date().toLocaleString('pt-BR')} [ERRO] ${nomeProcedimento}: ${err.message}`;
        fs.appendFileSync(config.paths.logFile, erroMsg + '\n');
        throw err;
    } finally {
        if (connection) await connection.end();
    }
}

module.exports = { executa_sql };