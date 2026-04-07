/*
   🧩 UTILITÁRIOS DE STRINGS
   Normalização e expansão de abreviações de logradouros em português
*/

// 📘 Dicionário de substituições (regex-friendly)
const substituicoes = {
  "AV\\.?": "AVENIDA ",
  "R\\.?": "RUA ",
  "ROD\\.?": "RODOVIA ",
  "TRV\\.?": "TRAVESSA ",
  "TR\\.?": "TRAVESSA ",
  "TR\\ ?": "TRAVESSA ",
  "TV\\.?": "TRAVESSA",
  "TV\\ ?": "TRAVESSA ",
  "PCA\\.?": "PRACA ",
  "AL\\.?": "ALAMEDA ",
  "DR\\.?": "DOUTOR ",
  "STA\\.?": "SANTA ",
  "STO\\.?": "SANTO ",
  "PROF\\.?": "PROFESSOR ",
  "ENG\\.?": "ENGENHEIRO ",
  "CEL\\.?": "CORONEL ",
  "GEN\\.?": "GENERAL ",
  "GOV\\.?": "GOVERNADOR ",
  "JD\\.?": "JARDIM ",
  "VL\\.?": "VILA ",
  "COND\\.?": "CONDOMINIO ",
  "CONJ\\.?": "CONJUNTO ",
  "LOT\\.?": "LOTEAMENTO ",
  "QD\\.?": "QUADRA ",
  "N° ": "NUMERO ",
  "NO\\.?": "NUMERO ",
  "NR\\.?": "NUMERO ",
  "ED\\.?": "EDIFICIO ",
  "PTO\\.?": "PORTO ",
  "PC\\.?": "PRACA ",
  "ESTR\\.?": "ESTRADA ",
  " 7 ": " SETE ",
  " 15 ": " QUINZE ",
  " XV ": " QUINZE ",
  "/[Nn][°º]\.?\s+(\d+)/": "NUMERO "
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

/**
 * Calcula a Distância de Levenshtein (Edit Distance) entre duas strings.
 * @param {string} str1 - A primeira string.
 * @param {string} str2 - A segunda string.
 * @returns {number} O número mínimo de edições necessárias.
 */
function LevenshteinDistance(str1, str2) {
    const m = str1.length;
    const n = str2.length;

    // 1. Casos base (se uma das strings é vazia, a distância é o tamanho da outra)
    if (m === 0) return n;
    if (n === 0) return m;

    // 2. Inicializa a matriz (tabela) de programação dinâmica
    // O array 'd' terá (m + 1) linhas e (n + 1) colunas (mas implementado de forma otimizada aqui)
    // d[i][j] armazena a distância entre os i primeiros caracteres de str1 e os j primeiros de str2.
    const d = [];
    
    // Inicializa a primeira linha
    for (let i = 0; i <= m; i++) {
        d[i] = [i]; // d[i][0] = i (custo de apagar i caracteres)
    }

    // Inicializa a primeira coluna (já feita acima para d[i][0])
    for (let j = 0; j <= n; j++) {
        d[0][j] = j; // d[0][j] = j (custo de inserir j caracteres)
    }

    // 3. Preenchimento da matriz
    for (let i = 1; i <= m; i++) {
        for (let j = 1; j <= n; j++) {
            
            // Custo da substituição (0 se os caracteres são iguais, 1 se são diferentes)
            const cost = (str1[i - 1] === str2[j - 1]) ? 0 : 1;

            // d[i][j] é o mínimo de:
            // a) Deleção: d[i - 1][j] + 1
            // b) Inserção: d[i][j - 1] + 1
            // c) Substituição: d[i - 1][j - 1] + cost
            d[i][j] = Math.min(
                d[i - 1][j] + 1,       // Deleção
                d[i][j - 1] + 1,       // Inserção
                d[i - 1][j - 1] + cost // Substituição
            );
        }
    }

    // 4. O resultado final está no último elemento da matriz
    return d[m][n];
}

/*
🧩 Exporta funções
*/
module.exports = {
  NormalizaString,
  TrocaAbreviaturas,
  LevenshteinDistance
};
