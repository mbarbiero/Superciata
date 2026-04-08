const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const config = require('./sc2_config');

function splitComandosSQL(sql) {
    // LOG DE DEPURAÇÃO 1
    //console.log("--- DEBUG SPLIT ---");
    //console.log("Tipo recebido:", typeof sql);
    //console.log("Conteúdo recebido:", sql);

    if (typeof sql !== 'string') {
        console.error("❌ ERRO CRÍTICO: splitComandosSQL recebeu algo que não é string!");
        // Tenta converter para string para não travar, mas avisa o erro
        sql = String(sql || ''); 
    }

    const regex = /;(?=(?:(?:[^"']*["']){2})*[^"']*$)/g;
    return sql.split(regex).map(c => c.trim()).filter(c => c.length > 0);
}

function aplicarTemplate(sql, valores = {}) {
    if (typeof sql !== 'string') return sql;
    return sql.replace(/\${(\w+)}/g, (match, key) => {
        return valores[key] !== undefined ? valores[key] : match;
    });
}

function parseSQL(sql) {
    const strSql = String(sql || '');
    const partes = strSql.trim().split(/\s+/);
    const operacao = partes[0] ? partes[0].toUpperCase() : 'DESCONHECIDO';
    let tabela = 'N/A';

    if (['SELECT', 'DELETE', 'TRUNCATE'].includes(operacao)) {
        const fromIndex = partes.indexOf('FROM');
        tabela = (fromIndex !== -1) ? partes[fromIndex + 1] : partes[1];
    } else if (['INSERT', 'REPLACE', 'LOAD'].includes(operacao)) {
        const target = (operacao === 'LOAD') ? partes[partes.indexOf('TABLE') + 1] : partes[partes.indexOf('INTO') + 1];
        tabela = target || 'N/A';
    }

    if (tabela) tabela = tabela.replace(/[`()]/g, '').split('.')[0];
    return { operacao, tabela };
}

async function executa_sql(sqlContent, nomeProcedimento = 'SC2_Direto') {
    //console.log(`[LOG] Iniciando executa_sql (${nomeProcedimento})`);
    
    // LOG DE DEPURAÇÃO 2
    //console.log("DEBUG: Conteúdo original em executa_sql:", typeof sqlContent);

    // Se o Express enviou o objeto body inteiro em vez de só o texto
    let sqlFinal = sqlContent;
    if (sqlContent && typeof sqlContent === 'object' && sqlContent.sql) {
        console.log("⚠️ Aviso: Recebido objeto em vez de string. Extraindo propriedade .sql");
        sqlFinal = sqlContent.sql;
    }

    return await processar_execucao(sqlFinal, nomeProcedimento);
}

async function executa_arquivo_sql(nomeArquivo, valores = {}, nomeProcedimento = 'SC2_Arquivo') {
    console.log(`Arquivo em dbExecutor: ${nomeArquivo}`);
    const fullPath = path.isAbsolute(nomeArquivo) ? nomeArquivo : path.join(config.paths.sqlFolder, nomeArquivo);

    //console.log(`[LOG] Lendo arquivo: ${fullPath}`);
    if (!fs.existsSync(fullPath)) throw new Error(`Arquivo não encontrado: ${fullPath}`);

    let sqlContent = fs.readFileSync(fullPath, 'utf8');
    sqlContent = aplicarTemplate(sqlContent, valores);

    return await processar_execucao(sqlContent, nomeProcedimento);
}

async function processar_execucao(sqlContent, nomeProcedimento) {
    let connection;
    try {
        // LOG DE DEPURAÇÃO 3
        //console.log("DEBUG: processar_execucao recebeu:", typeof sqlContent);

        connection = await mysql.createConnection({
            ...config.db,
            multipleStatements: false,
            flags: ['+LOCAL_FILES'],
            infileStreamFactory: (filePath) => fs.createReadStream(filePath)
        });

        //await connection.beginTransaction();

        // O erro acontece aqui se sqlContent não for string
        const comandos = splitComandosSQL(sqlContent);
        
        const resumoFinal = [];
        for (let i = 0; i < comandos.length; i++) {
            const sql = comandos[i];
            console.log(sql);
            console.log('-------------------------');           
            const info = parseSQL(sql);
            const start = Date.now();
            const [result] = await connection.query(sql);
            const duration = Date.now() - start;
            let registros = result.affectedRows || (Array.isArray(result) ? result.length : 0);
            resumoFinal.push({ passo: i + 1, ...info, registros, duration });
        }

        //await connection.commit();
        return { sucesso: true, detalhes: resumoFinal };
    } catch (err) {
        //if (connection) await connection.rollback();
        console.error("❌ Erro em processar_execucao:", err.message);
        throw err;
    } finally {
        //if (connection) await connection.end();
    }
}

module.exports = { executa_sql, executa_arquivo_sql };