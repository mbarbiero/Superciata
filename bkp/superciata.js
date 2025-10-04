const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise'); // Usando a versão com Promises para código mais limpo
const Busboy = require('busboy');

// --- CONFIGURAÇÃO ---
const PORT = 21067;
const dbConfig = {
    host: 'mysql.smuu.com.br',
    user: 'smuu_add1',
    password: 'SmuuBd1',
    database: 'smuu',
    port: 3306,
    waitForConnections: true,
    connectionLimit: 10, // Limite de conexões no pool
    queueLimit: 0
};

// --- INICIALIZAÇÃO ---
const app = express();
app.use(cors());
app.use(express.json()); // Middleware para parsear JSON bodies

// OTIMIZAÇÃO: Criar um pool de conexões em vez de conexões individuais
const pool = mysql.createPool(dbConfig);
console.log('Pool de conexões MySQL criado.');

// Criar diretório temp se não existir
const tempDir = path.join(__dirname, 'temp');
if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true });
}


// --- ROTAS ---

app.get('/', (req, res) => {
    res.send('Superciata!');
});

app.get('/superciata/testar-conexao', async (req, res) => {
    try {
        const connection = await pool.getConnection();
        console.log('Conexão MySQL bem-sucedida do pool!');
        connection.release(); // Libera a conexão de volta para o pool
        res.json({ sucesso: true, mensagem: 'Conexão MySQL OK!' });
    } catch (err) {
        console.error('Erro ao obter conexão do pool:', err.message);
        res.status(500).json({ sucesso: false, erro: err.message });
    }
});

app.get('/superciata/cnefe', async (req, res, next) => {
    try {
        const sql = 'SELECT * FROM PONTOS_CNEFE LIMIT 100';
        const [results] = await pool.query(sql); // Usando o pool diretamente
        console.log(`Dados da cnefe retornados: ${results.length} registros`);
        res.json({ dados: results });
    } catch (err) {
        console.error('Erro na consulta:', err.message);
        next(err); // Passa o erro para o tratador global
    }
});

/*
#######################################################
GeoJSON com Segurança e Otimização
#######################################################*/
const createGeoJSONRoute = (tableName, fields) => {
    return async (req, res, next) => {
        const { COD_MUNICIPIO } = req.query;
        if (!COD_MUNICIPIO) {
            return res.status(400).json({ erro: 'O parâmetro COD_MUNICIPIO é obrigatório.' });
        }

        // SEGURANÇA: Usando parameterized queries para prevenir SQL Injection
        const sql = `
            SELECT ${fields.join(', ')}
            FROM ${tableName}
            WHERE COD_MUNICIPIO = ?
              AND LATITUDE_CENTROIDE IS NOT NULL AND LONGITUDE_CENTROIDE IS NOT NULL
            ORDER BY ID_QUADRA, NUM_FACE`;

        try {
            const [results] = await pool.query(sql, [COD_MUNICIPIO]);

            const geojson = {
                type: "FeatureCollection",
                features: results.map(item => ({
                    type: "Feature",
                    properties: {
                        num_quadra: item.ID_QUADRA,
                        num_face: item.NUM_FACE,
                        nom_logradouro: item.NOM_LOGRADOURO,
                    },
                    geometry: {
                        type: "Point",
                        coordinates: [parseFloat(item.LONGITUDE_CENTROIDE), parseFloat(item.LATITUDE_CENTROIDE)]
                    }
                }))
            };
            res.json(geojson);
        } catch (err) {
            console.error(`Erro ao buscar dados da tabela ${tableName}:`, err.message);
            next(err);
        }
    };
};

app.get('/superciata/retCentroideFaces', createGeoJSONRoute('CN_CENTROIDES_FACES', [
    'ID_QUADRA', 'NUM_FACE', 'NOM_LOGRADOURO', 'LATITUDE_CENTROIDE', 'LONGITUDE_CENTROIDE'
]));

app.get('/superciata/retCentroideQuadras', createGeoJSONRoute('CN_CENTROIDES_QUADRAS', [
    'ID_QUADRA', 'NUM_FACE', 'LATITUDE_CENTROIDE', 'LONGITUDE_CENTROIDE'
]));


/*
#######################################################
ROTA DE UPLOAD CORRIGIDA
#######################################################*/
app.post("/carrega-cnefe", (req, res, next) => { // Adicionado 'next' para passar erros
    const bb = Busboy({
        headers: req.headers,
        limits: { fileSize: 100 * 1024 * 1024 } // 100MB
    });

    let savedFilePath = '';
    let originalFileName = '';

    bb.on("file", (name, file, info) => {
        originalFileName = info.filename;
        const saveTo = path.join(tempDir, `${Date.now()}_${originalFileName}`);
        savedFilePath = saveTo;
        
        console.log(`Recebendo arquivo: ${originalFileName}. Salvando em: ${saveTo}`);
        
        const writeStream = fs.createWriteStream(saveTo);
        file.pipe(writeStream);
        
        // Importante: Tratar erros no stream de escrita
        writeStream.on('error', (err) => {
            console.error('Erro ao escrever arquivo no disco:', err);
            // Passa o erro para o tratador de erros do busboy/express
            bb.emit('error', err); 
        });
    });

    bb.on("error", (err) => {
        console.error('Erro no Busboy:', err.message);
        // Limpar arquivo parcial se houver erro
        if (fs.existsSync(savedFilePath)) {
            fs.unlinkSync(savedFilePath);
        }
        next(err); // Passa o erro para o tratador global
    });

    bb.on("finish", () => {
        console.log(`Upload de ${originalFileName} concluído.`);
        res.json({
            sucesso: true,
            mensagem: 'Arquivo recebido com sucesso!',
            arquivo: originalFileName,
            caminho: savedFilePath
        });
    });
    
    // CORREÇÃO: Apenas o pipe. Remover os listeners 'req.on("data")'
    req.pipe(bb);
});


/*
#######################################################
TRATADOR DE ERROS GLOBAL (ESSENCIAL)
#######################################################*/
// ESTABILIDADE: Middleware para capturar todos os erros não tratados
app.use((error, req, res, next) => {
    console.error("ERRO GLOBAL CAPTURADO:", error);
    res.status(500).send({
        sucesso: false,
        erro: 'Ocorreu um erro inesperado no servidor.',
        detalhes: error.message
    });
});


// --- INICIAR SERVIDOR ---
const server = app.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`);
    server.timeout = 30 * 60 * 1000; // 30 minutos
});

