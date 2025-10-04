const express = require('express');
const multer = require('multer');
const path = require('path');
const app = express();
const PORT = 21041;

// Serve arquivos estáticos da pasta 'public'
// Isso permite que você acesse index.html, styles.css, etc., diretamente.
app.use(express.static('public'));

// Define o diretório de destino para os arquivos enviados
const tempDir = 'temp/';

// Configuração do Multer para definir onde e como o arquivo será salvo
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, tempDir);
    },
    filename: (req, file, cb) => {
        // Gera um nome de arquivo único com a extensão original
        cb(null, `${Date.now()}-${file.originalname}`);
    }
});

const upload = multer({ storage: storage });

// Inicia o servidor e define o timeout
const server = app.listen(PORT, () => {
    console.log(`Servidor iniciado na porta ${PORT}`);
});

server.timeout = 30 * 60 * 1000; // 30 minutos

// Nova rota para upload de arquivos
// O 'arquivo' deve corresponder ao 'name' do input no formulário HTML
app.post('/carregar', upload.single('arquivo'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('Nenhum arquivo enviado.');
    }
    console.log('Arquivo salvo:', req.file.path);
    res.send(`Arquivo "${req.file.originalname}" enviado e salvo em "${tempDir}" com sucesso!`);
});