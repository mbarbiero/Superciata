const fs = require("fs");
const path = require("path");
const multer = require("multer");
const unzipper = require("unzipper");

// Pasta temporária em ../arquivos/temp
const tempDir = path.join(__dirname, "../arquivos/temp");
if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true });
}

// Configuração do multer para salvar arquivos zip na pasta temp
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, tempDir);
    },
    filename: (req, file, cb) => {
        cb(null, Date.now() + "-" + file.originalname);
    }
});

const upload = multer({ storage });

// Função para descompactar um arquivo ZIP
async function unzipArquivo(filePath, destDir) {
    
    // 1. GARANTE que o diretório de destino exista
    // A opção { recursive: true } cria subdiretórios, se necessário.
    try {
        await mkdir(destDir, { recursive: true });
    } catch (error) {
        // Se a criação falhar por permissão ou outro motivo (não por já existir), rejeita
        throw new Error(`Falha ao criar o diretório de destino "${destDir}": ${error.message}`);
    }

    return new Promise((resolve, reject) => {
        const readStream = fs.createReadStream(filePath);
        const extractStream = unzipper.Extract({ path: destDir });

        // 2. TRATAMENTO DE ERRO na LEITURA (fonte do stream)
        // Captura erros como "arquivo não existe" ou "permissão negada".
        readStream.on("error", (err) => {
            // Rejeita a Promise e fecha o stream, se necessário
            reject(new Error(`Erro de Leitura do arquivo ZIP (${filePath}): ${err.message}`));
        });
        
        // 3. TRATAMENTO DE ERRO na ESCRITA/DESCOMPACTAÇÃO (destino do stream)
        // Captura erros durante o processo de descompactação ou escrita no disco.
        extractStream.on("error", (err) => {
             // Rejeita a Promise
             reject(new Error(`Erro de Descompactação/Escrita no destino: ${err.message}`));
        });

        // 4. Conecta e Inicia o Processo
        readStream
            .pipe(extractStream)
            .on("close", () => {
                // Sucesso: resolve a Promise com o caminho de destino
                resolve(destDir);
            });
    });
}

// Função para listar arquivos em um diretório
function listaArquivos(dirPath) {
    if (!fs.existsSync(dirPath)) {
        console.error(`ERRO: Diretório não encontrado: ${dirPath}`);
        return {
            sucesso: false,
            diretorio: dirPath,
            arquivos: [],
            erro: "Diretório não encontrado"
        };
    }

    try {
        // 1. Lista todos os nomes de arquivos (síncrono)
        const nomesDeArquivos = fs.readdirSync(dirPath);
        
        const arquivosDetalhes = [];

        // 2. Itera sobre os arquivos para obter o tamanho (síncrono)
        for (const nome of nomesDeArquivos) {
            const filePath = path.join(dirPath, nome);
            
            // fs.statSync obtém metadados do arquivo
            const stats = fs.statSync(filePath);
            
            // Ignora se for um diretório
            if (stats.isDirectory()) {
                continue;
            }

            arquivosDetalhes.push({
                nome: nome,
                tamanho: stats.size, // Tamanho em bytes
                dataModificacao: stats.mtime
            });
        }

        return {
            sucesso: true,
            diretorio: dirPath,
            arquivos: arquivosDetalhes
        };

    } catch (error) {
        console.error(`ERRO de leitura em "${dirPath}":`, error.message);
        return {
            sucesso: false,
            diretorio: dirPath,
            arquivos: [],
            erro: "Erro de permissão ou I/O na leitura do diretório"
        };
    }
}

// Função para remover um arquivo
function deletaArquivo(filePath) {
    if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
    }
}

module.exports = {
    unzipArquivo,
    listaArquivos,
    deletaArquivo
};
