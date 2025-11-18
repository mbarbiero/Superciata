/*
   UTILITÁRIOS
*/

// 📘 Dicionário de substituições (JSON embutido)
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
Normalização de strings para o português
*/
function NormalizaString(str) {
   if (!str) return '';

   return str
      .toUpperCase() // 1️⃣ converte para maiúsculas
      .normalize("NFD") // separa letras e diacríticos (acentos)
      .replace(/[\u0300-\u036f]/g, "") // remove acentos
      .replace(/Ç/g, "C"); // trata cedilha explicitamente
}

/*
🔄 Aplicar substituições
*/
function TrocaAbreviaturas(str) {
   for (const [abreviatura, completa] of Object.entries(substituicoes)) {
      const regex = new RegExp(`\\b${abreviatura}\\b`, 'g');
      str = str.replace(regex, completa);
   }
   return(str);
}

module.exports = {
  NormalizaString,
  TrocaAbreviaturas
};