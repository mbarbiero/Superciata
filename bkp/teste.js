const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

const app = express();
const port = 21041;

// Define a pasta onde os arquivos serão salvos
const uploadDir = path.join(__dirname, 'uploads');

// Cria a pasta de uploads se ela não existir
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir);
}

// Configuração do Multer para salvar os arquivos na pasta 'uploads'
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, uploadDir); // Pasta de destino
    },
    filename: function (req, file, cb) {
        // Usa o nome original do arquivo com timestamp para evitar colisões
        cb(null, Date.now() + '-' + file.originalname);
    }
});

const upload = multer({ storage: storage });

// Rota para receber o arquivo (POST)
app.post('/upload', upload.single('meuArquivo'), (req, res) => {
    // Se tudo correr bem, o arquivo já está salvo no disco
    res.send('Arquivo enviado e salvo com sucesso!');
}, (error, req, res, next) => {
    // Tratamento de erro do Multer
    res.status(400).send({ error: error.message });
});

// Inicia o servidor
app.listen(port, () => {
    console.log(`Servidor a ouvir em http://localhost:${port}`);
});