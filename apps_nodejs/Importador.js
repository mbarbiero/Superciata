const fs = require('fs');
const csv = require('fast-csv');
const path = require('path');

async function importaPontosCnefe(caminhoCSV, conexao) {
   console.error('importaPontosCnefe');

   return new Promise((resolve, reject) => {
      const stream = fs.createReadStream(caminhoCSV);
      const csvStream = csv.parse({ headers: true, delimiter: ';' });

      console.error('Promise');

      let contador = 0;
      let erros = 0;

      const logPath = path.join(__dirname, 'log_importacao.txt');
      const logStream = fs.createWriteStream(logPath, { flags: 'a' });

      csvStream.on('error', (err) => {
         console.error('❌ Erro geral no CSV:', err.message);
         reject(err);
      });

      csvStream.on('data', async (row) => {
         console.error(row);
         try {
            const values = [
               row.COD_UNICO_ENDERECO, row.COD_UF, row.COD_MUNICIPIO,
               row.COD_DISTRITO, row.COD_SUBDISTRITO, row.COD_SETOR,
               row.NUM_QUADRA, row.NUM_FACE, row.CEP, row.DSC_LOCALIDADE,
               row.NOM_TIPO_SEGLOGR, row.NOM_TITULO_SEGLOGR, row.NOM_SEGLOGR,
               row.NUM_ENDERECO, row.DSC_MODIFICADOR, row.NOM_COMP_ELEM1,
               row.VAL_COMP_ELEM1, row.NOM_COMP_ELEM2, row.VAL_COMP_ELEM2,
               row.NOM_COMP_ELEM3, row.VAL_COMP_ELEM3, row.NOM_COMP_ELEM4,
               row.VAL_COMP_ELEM4, row.NOM_COMP_ELEM5, row.VAL_COMP_ELEM5,
               row.LATITUDE, row.LONGITUDE, row.NV_GEO_COORD, row.COD_ESPECIE,
               row.DSC_ESTABELECIMENTO, row.COD_INDICADOR_ESTAB_ENDERECO,
               row.COD_INDICADOR_CONST_ENDERECO, row.COD_INDICADOR_FINALIDADE_CONST,
               row.COD_TIPO_ESPECI,
               `POINT(${row.LONGITUDE} ${row.LATITUDE})`
            ];

            const sql = `
          INSERT INTO PONTOS_CNEFE (
            COD_UNICO_ENDERECO, COD_UF, COD_MUNICIPIO, COD_DISTRITO, COD_SUBDISTRITO,
            COD_SETOR, NUM_QUADRA, NUM_FACE, CEP, DSC_LOCALIDADE,
            NOM_TIPO_SEGLOGR, NOM_TITULO_SEGLOGR, NOM_SEGLOGR, NUM_ENDERECO,
            DSC_MODIFICADOR, NOM_COMP_ELEM1, VAL_COMP_ELEM1,
            NOM_COMP_ELEM2, VAL_COMP_ELEM2, NOM_COMP_ELEM3, VAL_COMP_ELEM3,
            NOM_COMP_ELEM4, VAL_COMP_ELEM4, NOM_COMP_ELEM5, VAL_COMP_ELEM5,
            LATITUDE, LONGITUDE, NV_GEO_COORD, COD_ESPECIE, DSC_ESTABELECIMENTO,
            COD_INDICADOR_ESTAB_ENDERECO, COD_INDICADOR_CONST_ENDERECO,
            COD_INDICADOR_FINALIDADE_CONST, COD_TIPO_ESPECI, COORDS_PONTO
          )
          VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?)
          )
        `;

            await conexao.execute(sql, values);
            contador++;

            if (contador % 100 === 0) {
               console.log(`✔️  ${contador} linhas inseridas...`);
            }

         } catch (err) {
            erros++;
            const logMsg = `Erro na linha ${contador + 1}: ${err.message} - Dados: ${JSON.stringify(row)}\n`;
            logStream.write(logMsg);
            console.error(`⚠️  Linha com erro registrada (${erros} erros até agora)`);
         }
      });

      csvStream.on('end', () => {
         logStream.end();
         console.log(`✅ Finalizado. ${contador} inseridas. ${erros} erros.`);
         resolve({ inseridos: contador, erros });
      });

      stream.pipe(csvStream);
   });
}

module.exports = importaPontosCnefe;
