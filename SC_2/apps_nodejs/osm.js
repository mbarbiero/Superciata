const mysql = require('mysql2/promise');
const axios = require('axios');
const config = require('./sc2_config');

let ocupado = false;
let logMensagens = ["Servidor pronto para iniciar coleta."];

const addLog = (msg) => {
    const time = new Date().toLocaleTimeString();
    logMensagens.unshift(`[${time}] ${msg}`);
    if (logMensagens.length > 50) logMensagens.pop();
    console.log(`[OSM_SERVICE] ${msg}`);
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function processarLote(codigoMunicipio, quantidade = 10) {
    if (ocupado) {
        addLog("⚠️ Já existe um processo em execução. Aguarde.");
        return;
    }

    ocupado = true;
    let connection;

    try {
        connection = await mysql.createConnection(config.db);
        addLog(`🚀 Iniciando lote de ${quantidade} para o município ${codigoMunicipio}...`);

        for (let i = 1; i <= quantidade; i++) {
            // Busca a próxima face pendente
            const [faces] = await connection.query(`
                SELECT f.SC_ID_LOGRADOURO, f.NOM_LOGRADOURO as CN_NOM_LOGRADOURO, f.COD_MUNICIPIO, 
                       ST_X(f.CENTROIDE) as lon, ST_Y(f.CENTROIDE) as lat
                FROM CN_FACES f
                LEFT JOIN OSM_CONSULTAS o ON f.SC_ID_LOGRADOURO = o.SC_ID_LOGRADOURO
                WHERE o.SC_ID_LOGRADOURO IS NULL AND f.COD_MUNICIPIO = ?
                ORDER BY f.QTD_PONTOS DESC LIMIT 1`, [codigoMunicipio]);

            if (faces.length === 0) {
                addLog("🏁 Fim dos registros: Tudo processado!");
                break;
            }

            const face = faces[0];
            addLog(`ITEM ${i}/${quantidade}: ${face.CN_NOM_LOGRADOURO}`);

            try {
                // FASE 1: Overpass API (Nome)
                const q1 = `[out:json];way(around:10,${face.lat},${face.lon})["highway"];out tags;`;
                const r1 = await axios.get(`https://overpass-api.de/api/interpreter?data=${encodeURIComponent(q1)}`, { timeout: 30000 });
                
                const osmNome = r1.data.elements?.find(e => e.tags && e.tags.name)?.tags.name;

                if (osmNome) {
                    // FASE 2: Overpass API (Geometria completa)
                    const q2 = `[out:json];area["br:ibge:code"="${face.COD_MUNICIPIO}"]->.a;(way["name"="${osmNome}"](area.a););(._;>;);out body;`;
                    const r2 = await axios.get(`https://overpass-api.de/api/interpreter?data=${encodeURIComponent(q2)}`, { timeout: 60000 });

                    await connection.query(`
                        INSERT INTO OSM_CONSULTAS (SC_ID_LOGRADOURO, CN_NOM_LOGRADOURO, OSM_NOM_LOGRADOURO, RESPOSTA_OSM)
                        VALUES (?, ?, ?, ?)`, 
                        [face.SC_ID_LOGRADOURO, face.CN_NOM_LOGRADOURO, osmNome, JSON.stringify(r2.data)]
                    );
                    addLog(`✅ Salvo: ${osmNome}`);
                } else {
                    addLog(`❌ Nome não encontrado no raio de 10m.`);
                }
            } catch (err) {
                addLog(`⚠️ Erro no item: ${err.message}`);
                if (err.response?.status === 429) {
                    addLog("🚫 Bloqueio Overpass (429). Aguardando 2 minutos...");
                    await sleep(120000);
                }
            }

            // A PAUSA CRÍTICA: 30 a 60 segundos entre cada item do lote
            if (i < quantidade) {
                const tempoEspera = Math.floor(Math.random() * (60000 - 30000) + 30000);
                addLog(`😴 Aguardando ${tempoEspera/1000}s para evitar bloqueio...`);
                await sleep(tempoEspera);
            }
        }
    } catch (globalErr) {
        addLog(`🔥 Erro crítico no serviço: ${globalErr.message}`);
    } finally {
        ocupado = false;
        if (connection) await connection.end();
        addLog("🏁 Lote finalizado. Servidor liberado.");
    }
}

module.exports = { 
    processarLote, 
    isOcupado: () => ocupado, 
    getLogs: () => logMensagens 
};