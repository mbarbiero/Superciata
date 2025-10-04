const multer = require('multer');
const path = require('path');
const fs = require('fs');

// Configurar armazenamento com multer
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    // Criar diretório temp se não existir
    const tempDir = './temp';
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }
    cb(null, tempDir);
  },
  filename: function (req, file, cb) {
    // Manter o nome original do arquivo
    cb(null, file.originalname);
  }
});

// Filtro para aceitar apenas arquivos ZIP
const fileFilter = (req, file, cb) => {
  if (file.mimetype === 'application/zip' || 
      file.mimetype === 'application/x-zip-compressed' ||
      path.extname(file.originalname).toLowerCase() === '.zip') {
    cb(null, true);
  } else {
    cb(new Error('Apenas arquivos ZIP são permitidos!'), false);
  }
};

const upload = multer({ 
  storage: storage,
  fileFilter: fileFilter,
  limits: {
    fileSize: 100 * 1024 * 1024 // Limite de 100MB
  }
});



async function CarregaZip() {
}

module.exports = CN_Processa;
