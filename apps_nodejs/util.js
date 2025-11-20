/*
   🧩 UTILITÁRIOS DE STRINGS
   Normalização e expansão de abreviações de logradouros em português
*/

// 📘 Dicionário de substituições (regex-friendly)
const substituicoes = {
  "AV\\.?": "AVENIDA",
  "R\\.?": "RUA",
  "ROD\\.?": "RODOVIA",
  "TRV\\.?": "TRAVESSA",
  "TV\\.?": "TRAVESSA",
  "PCA\\.?": "PRACA",
  "AL\\.?": "ALAMEDA",
  "DR\\.?": "DOUTOR",
  "STA\\.?": "SANTA",
  "STO\\.?": "SANTO",
  "PROF\\.?": "PROFESSOR",
  "ENG\\.?": "ENGENHEIRO",
  "CEL\\.?": "CORONEL",
  "GEN\\.?": "GENERAL",
  "JD\\.?": "JARDIM",
  "VL\\.?": "VILA",
  "COND\\.?": "CONDOMINIO",
  "CONJ\\.?": "CONJUNTO",
  "LOT\\.?": "LOTEAMENTO",
  "QD\\.?": "QUADRA",
  "N°": "NUMERO",
  "NO\\.?": "NUMERO",
  "NR\\.?": "NUMERO",
  "ED\\.?": "EDIFICIO",
  "PTO\\.?": "PORTO",
  "PC\\.?": "PRACA",
  "ESTR\\.?": "ESTRADA"
};

/*
🧹 Normalização de strings para o português
*/
function NormalizaString(str) {
  if (!str) return '';

  return str
    .toUpperCase()
    .normalize("NFD") // separa letras e acentos
    .replace(/[\u0300-\u036f]/g, "") // remove acentos
    .replace(/Ç/g, "C") // trata cedilha
    .replace(/\s+/g, " ") // colapsa espaços múltiplos
    .trim();
}

/*
🔄 Expansão de abreviações (com ou sem ponto)
*/
function TrocaAbreviaturas(str) {
  if (!str) return '';

  for (const [abreviatura, completa] of Object.entries(substituicoes)) {
    // Permite abreviação com ou sem ponto, e ignora diferenças de espaço
    const regex = new RegExp(`\\b${abreviatura}\\b`, 'gi');
    str = str.replace(regex, completa);
  }

  // Remove pontos isolados remanescentes (ex: "DOUTOR.")
  str = str.replace(/\s*\.\s*/g, " ");

  // Remove espaços duplos
  str = str.replace(/\s+/g, " ").trim();

  return str;
}

/*
🧩 Exporta funções
*/
module.exports = {
  NormalizaString,
  TrocaAbreviaturas
};
