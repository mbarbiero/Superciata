const mysql = require('mysql2/promise');

const dbConfig = {
  host: 'mysql.smuu.com.br',
  user: 'smuu_add1',
  password: 'SmuuBd1',
  database: 'smuu',
  port: 3306
};

async function atualizarGeom() {
  const connection = await mysql.createConnection(dbConfig);

  // Buscar todos os pontos válidos
  const [rows] = await connection.execute(`
    SELECT 
      NUM_QUADRA, 
      LATITUDE_CENTROIDE, 
      LONGITUDE_CENTROIDE
    FROM 
      CNEFE_FACES
    WHERE 
      LATITUDE_CENTROIDE IS NOT NULL AND LONGITUDE_CENTROIDE IS NOT NULL

  `);

  // Agrupar por NUM_QUADRA
  const quadras = {};
  for (const row of rows) {
    if (!quadras[row.NUM_QUADRA]) {
      quadras[row.NUM_QUADRA] = [];
    }
    quadras[row.NUM_QUADRA].push([row.LONGITUDE_CENTROIDE, row.LATITUDE_CENTROIDE]);
  }

  // Gerar e executar updates
  for (const [numQuadra, pontos] of Object.entries(quadras)) {
    const multipoint = pontos.map(p => `${p[0]} ${p[1]}`).join(', ');
    const wkt = `MULTIPOINT(${multipoint})`;

    const sql = `
      UPDATE CNEFE_QUADRAS
      SET geom = ST_ConvexHull(ST_GeomFromText(?))
      WHERE NUM_QUADRA = ?;
    `;

    try {
      await connection.execute(sql, [wkt, numQuadra]);
      console.log(`Atualizado: ${numQuadra}`);
    } catch (err) {
      console.error(`Erro ao atualizar ${numQuadra}:`, err.message);
    }
  }

  await connection.end();
  console.log('Atualização concluída.');
}

atualizarGeom();
