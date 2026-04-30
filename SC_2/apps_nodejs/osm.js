const mysql = require('mysql2/promise');
const axios = require('axios');

class MotorOSM {
    constructor(dbConfig) {
        this.dbConfig = dbConfig;
        this.ativo = false;
        this.COD_MUN = "4104501"; // Capanema
        this.logs = [];
        
        // Servidores Overpass para rodízio
        this.servidores = [
            'https://overpass-api.de/api/interpreter',
            'https://overpass.kumi.systems/api/interpreter',
            'https://overpass.openstreetmap.ru/cgi/interpreter'
        ];
        this.serverIndex = 0;
    }

    async sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

    async log(msg, tipo = 'INFO') {
        const entrada = `[${new Date().toLocaleTimeString()}] [${tipo}] ${msg}`;
        console.log(entrada);
        this.logs.unshift(entrada);
        if (this.logs.length > 50) this.logs.pop();
    }

    async executar() {
        if (this.ativo) return;
        this.ativo = true;
        let connection;

        try {
            connection = await mysql.createConnection(this.dbConfig);
            this.log("🚀 Motor SuperCIATA Online | Modo: Rodízio de Servidores | Pausa: 30s", "SISTEMA");

            while (this.ativo) {
                /* SQL OTIMIZADO:
                   1. Filtra por ID_FACE para nunca repetir a mesma geometria.
                   2. Filtra por SC_ID_LOGRADOURO para pular faces de ruas já encontradas.
                */
                const sqlBusca = `
                    SELECT 
                        f.ID_FACE, 
                        f.SC_ID_LOGRADOURO, 
                        f.NOM_LOGRADOURO, 
                        ROUND(ST_X(f.CENTROIDE), 8) as lon, 
                        ROUND(ST_Y(f.CENTROIDE), 8) as lat 
                    FROM CN_FACES f
                    LEFT JOIN OSM_CONSULTAS o_face ON f.ID_FACE = o_face.CN_ID_FACE
                    WHERE f.COD_MUNICIPIO = ?
                    AND o_face.CN_ID_FACE IS NULL 
                    AND f.SC_ID_LOGRADOURO NOT IN (
                        SELECT DISTINCT SC_ID_LOGRADOURO 
                        FROM OSM_CONSULTAS 
                        WHERE COD_MUNICIPIO = ? 
                        AND OSM_NOM_LOGRADOURO <> 'NÃO ENCONTRADO'
                    )
                    ORDER BY f.QTD_PONTOS DESC LIMIT 1`;

                const [rows] = await connection.execute(sqlBusca, [this.COD_MUN, this.COD_MUN]);

                if (rows.length === 0) {
                    this.log("🏁 Fim da coleta: Todas as faces processadas ou logradouros mapeados.", "FIM");
                    this.ativo = false;
                    break;
                }

                const face = rows[0];
                
                // Seleção e rotação do servidor
                const baseUrl = this.servidores[this.serverIndex];
                this.serverIndex = (this.serverIndex + 1) % this.servidores.length;

                this.log(`🔎 Analisando: "${face.NOM_LOGRADOURO}" | Face: ${face.ID_FACE} | Server: ${new URL(baseUrl).hostname}`, "BUSCA");

                try {
                    // Query Overpass com timeout de 25s
                    const query = `[out:json][timeout:45];way(around:20,${face.lat},${face.lon})["highway"];out tags;`;
                    const url = `${baseUrl}?data=${encodeURIComponent(query)}`;
                    
                    const response = await axios.get(url, { 
                        timeout: 30000, 
                        headers: { 'User-Agent': 'SuperCIATA-Research/3.0 (Master-UFSC)' }
                    });

                    const via = response.data.elements?.find(e => e.tags && e.tags.name);
                    const nomeOSM = via ? via.tags.name : "NÃO ENCONTRADO";
                    const idOSM = via ? via.id : null;

                    // Gravação na nova estrutura de tabela
                    await connection.execute(`
                        INSERT INTO OSM_CONSULTAS 
                        (CN_ID_FACE, SC_ID_LOGRADOURO, CN_NOM_LOGRADOURO, OSM_NOM_LOGRADOURO, COD_MUNICIPIO, RESPOSTA_OSM_JSON)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE 
                            OSM_NOM_LOGRADOURO = VALUES(OSM_NOM_LOGRADOURO),
                            RESPOSTA_OSM_JSON = VALUES(RESPOSTA_OSM_JSON),
                            DATA_CONSULTA = CURRENT_TIMESTAMP`, 
                        [
                            face.ID_FACE, 
                            face.SC_ID_LOGRADOURO, 
                            face.NOM_LOGRADOURO, 
                            nomeOSM, 
                            this.COD_MUN, 
                            idOSM ? String(idOSM) : null
                        ]
                    );

                    if (nomeOSM !== "NÃO ENCONTRADO") {
                        this.log(`✅ SUCESSO: "${face.NOM_LOGRADOURO}" -> "${nomeOSM}"`, "RESULTADO");
                    } else {
                        this.log(`⚪ VAZIO: "${face.NOM_LOGRADOURO}" sem dados no OSM`, "RESULTADO");
                    }

                    // Pausa fixa de 30 segundos
                    await this.sleep(30000);

                } catch (error) {
                    const status = error.response ? error.response.status : null;
                    if (status === 429 || status === 504 || status === 529) {
                        this.log(`⚠️ Servidor ${new URL(baseUrl).hostname} instável (${status}). Pausa de 1 min...`, "AVISO");
                        await this.sleep(60000);
                    } else if (error.code === 'ECONNABORTED') {
                        this.log(`⏱️ Timeout no servidor ${new URL(baseUrl).hostname}. Tentando próximo em 10s...`, "AVISO");
                        await this.sleep(10000);
                    } else {
                        this.log(`❌ Erro: ${error.message}. Próxima tentativa em 30s...`, "ERRO");
                        await this.sleep(30000);
                    }
                }
            }
        } catch (err) {
            this.log(`🔥 Erro Fatal de Conexão: ${err.message}`, "CRÍTICO");
        } finally {
            if (connection) await connection.end();
            this.ativo = false;
            this.log("Motor desligado.", "SISTEMA");
        }
    }

    parar() { 
        this.ativo = false; 
        this.log("Comando de parada recebido.", "SISTEMA");
    }

    getStatus() { 
        return { ativo: this.ativo, historico: this.logs }; 
    }
}

module.exports = MotorOSM;