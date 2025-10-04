const express = require('express');
const multer = require('multer');
const path = require('path');
const app = express();
const port = 3000;

// Configuração do Multer para salvar o arquivo na pasta 'uploads'
const upload = multer({ dest: 'uploads/' });

// Rota para a página principal que terá o formulário de upload
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'teste_server.html'));
});

// Rota para processar o upload do arquivo
// O nome 'arquivo' deve corresponder ao 'name' do input no formulário HTML
app.post('/upload', upload.single('arquivo'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('Nenhum arquivo enviado.');
    }
    res.send(`Arquivo "${req.file.originalname}" enviado com sucesso!`);
    console.log('Arquivo salvo em:', req.file.path);
});

// Inicia o servidor
app.listen(port, () => {
    console.log(`Servidor rodando em http://localhost:${port}`);
});