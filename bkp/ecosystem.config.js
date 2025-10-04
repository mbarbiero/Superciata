module.exports = {
  apps: [{
    name: "superciata",         // Nome da sua aplicação
    script: "superciata.js",    // Mome do seu arquivo (ex: index.js, app.js)
    watch: true,                // Habilitar o modo watch
    ignore_watch: ["temp"]      // Lista de pastas ou arquivos a serem ignorados
  }]
};


