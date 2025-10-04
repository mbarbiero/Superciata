// db.js
const mysql_p = require('mysql2/promise');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');

let conexao;

const PONTOS_CNEFE = `
   CREATE TABLE IF NOT EXISTS PONTOS_CNEFE(
      COD_UNICO_ENDERECO VARCHAR(20),
      COD_UF VARCHAR(2),
      COD_MUNICIPIO VARCHAR(7),
      COD_DISTRITO VARCHAR(10),
      COD_SUBDISTRITO VARCHAR(12),
      COD_SETOR VARCHAR(17),
      NUM_QUADRA VARCHAR(3),
      NUM_FACE VARCHAR(3),
      CEP VARCHAR(8),
      DSC_LOCALIDADE VARCHAR(100),
      NOM_TIPO_SEGLOGR VARCHAR(50),
      NOM_TITULO_SEGLOGR VARCHAR(50),
      NOM_SEGLOGR VARCHAR(100),
      NUM_ENDERECO VARCHAR(10),
      DSC_MODIFICADOR VARCHAR(50),
      NOM_COMP_ELEM1 VARCHAR(50),
      VAL_COMP_ELEM1 VARCHAR(50),
      NOM_COMP_ELEM2 VARCHAR(50),
      VAL_COMP_ELEM2 VARCHAR(50),
      NOM_COMP_ELEM3 VARCHAR(50),
      VAL_COMP_ELEM3 VARCHAR(50),
      NOM_COMP_ELEM4 VARCHAR(50),
      VAL_COMP_ELEM4 VARCHAR(50),
      NOM_COMP_ELEM5 VARCHAR(50),
      VAL_COMP_ELEM5 VARCHAR(50),
      LATITUDE DECIMAL(10, 6),
      LONGITUDE DECIMAL(10, 6),
      NV_GEO_COORD VARCHAR(20),
      COD_ESPECIE VARCHAR(20),
      DSC_ESTABELECIMENTO VARCHAR(50),
      COD_INDICADOR_ESTAB_ENDERECO VARCHAR(10),
      COD_INDICADOR_CONST_ENDERECO VARCHAR(10),
      COD_INDICADOR_FINALIDADE_CONST VARCHAR(10),
      COD_TIPO_ESPECI VARCHAR(10),
      COORDS_PONTO GEOMETRY
);`;


// TABELA DE PONTOS ÚNICOS DO CNEFE
const PONTOS_UNICOS_CNEFE = `
   DROP TABLE IF EXISTS PONTOS_UNICOS_CNEFE;
   CREATE TABLE PONTOS_UNICOS_CNEFE AS (
      SELECT DISTINCT
         COD_MUNICIPIO,
         CONCAT (COD_SETOR, NUM_QUADRA) AS ID_QUADRA,
         NUM_FACE,
         REPLACE(CONCAT(NOM_TIPO_SEGLOGR, " ", NOM_TITULO_SEGLOGR, " ", NOM_SEGLOGR), "  ", " ") AS NOM_LOGRADOURO,
         NUM_ENDERECO,
         LATITUDE,
         LONGITUDE,
         ST_PointFromText(CONCAT('POINT(', LONGITUDE, ' ', LATITUDE, ')'), 4326) AS COORDS_PONTO
      FROM smuu.PONTOS_CNEFE
      WHERE 
         LATITUDE IS NOT NULL 
         AND LONGITUDE IS NOT NULL
      ORDER BY 
         ID_QUADRA
   );`;

// cálculo dos centroides para cada face
const CENTROIDES_FACES_CNEFE = `
   DROP TABLE IF EXISTS CENTROIDES_FACES_CNEFE;
   CREATE TABLE CENTROIDES_FACES_CNEFE AS (
      SELECT 
         COD_MUNICIPIO,
         ID_QUADRA,
         NUM_FACE,
         NOM_LOGRADOURO,
         AVG(LATITUDE) AS LATITUDE_CENTROIDE,
         AVG(LONGITUDE) AS LONGITUDE_CENTROIDE,
         COUNT(*) AS TOTAL_ENDERECOS,
         ST_PointFromText(CONCAT('POINT(', AVG(LONGITUDE), ' ', AVG(LATITUDE), ')'), 4326) AS COORDS_CENTROIDE
      FROM smuu.PONTOS_UNICOS_CNEFE
      WHERE 
         LATITUDE IS NOT NULL 
         AND LONGITUDE IS NOT NULL
      GROUP BY 
         COD_MUNICIPIO,
         ID_QUADRA, 
         NUM_FACE,
         NOM_LOGRADOURO
   );`;


// cálculo dos centroides para cada quadra
const CENTROIDES_QUADRAS_CNEFE = `
   DROP TABLE IF EXISTS CENTROIDES_QUADRAS_CNEFE;
   CREATE TABLE CENTROIDES_QUADRAS_CNEFE AS (
      SELECT 
         COD_MUNICIPIO,
         ID_QUADRA,
         AVG(LATITUDE_CENTROIDE) AS LATITUDE_CENTROIDE,
         AVG(LONGITUDE_CENTROIDE) AS LONGITUDE_CENTROIDE,
         COUNT(*) AS TOTAL_ENDERECOS,
         ST_PointFromText(CONCAT('POINT(', AVG(LONGITUDE_CENTROIDE), ' ', AVG(LONGITUDE_CENTROIDE), ')'), 4326) AS COORDS_CENTROIDE
      FROM smuu.CENTROIDES_FACES_CNEFE
      WHERE 
         LATITUDE_CENTROIDE IS NOT NULL 
         AND LONGITUDE_CENTROIDE IS NOT NULL
      GROUP BY 
         COD_MUNICIPIO,
         ID_QUADRA
      );`;


async function conectar() {
   return await mysql_p.createConnection({
      host: 'mysql.smuu.com.br',
      user: 'smuu_add1',
      password: 'SmuuBd1',
      database: 'smuu',
      port: 3306,
      multipleStatements: true
   });
}

async function CriaTabPontosCnefe() {
   try {
      conexao = await conectar();
      await conexao.query(PONTOS_CNEFE);
      return { sucesso: true, mensagem: 'Tabela PONTOS_CNEFE criada com sucesso.' };
   } catch (erro) {
      return {
         sucesso: false,
         mensagem: 'Erro ao criar tabela PONTOS_CNEFE.',
         erro: erro.message,
         codigo: erro.code
      };
   } finally {
      if (conexao) await conexao.end();
   }
}

async function CriaPontosUnicosCnefe() {
   try {
      conexao = await conectar();
      await conexao.query(PONTOS_UNICOS_CNEFE);
      return { sucesso: true, mensagem: 'Tabela PONTOS_UNICOS_CNEFE criada com sucesso.' };
   } catch (erro) {
      return {
         sucesso: false,
         mensagem: 'Erro ao criar tabela PONTOS_UNICOS_CNEFE.',
         erro: erro.message,
         codigo: erro.code
      };
   } finally {
      if (conexao) await conexao.end();
   }
}

async function CriaCentroidesFacesCnefe() {
   try {
      conexao = await conectar();
      await conexao.query(CENTROIDES_FACES_CNEFE);
      return { sucesso: true, mensagem: 'Tabela CENTROIDES_FACES_CNEFE criada com sucesso.' };
   } catch (erro) {
      return {
         sucesso: false,
         mensagem: 'Erro ao criar tabela CENTROIDES_FACES_CNEFE.',
         erro: erro.message,
         codigo: erro.code
      };
   } finally {
      if (conexao) await conexao.end();
   }
}

// CENTROIDES_QUADRAS_CNEFE
async function CriaCentroidesQuadrasCnefe() {
   try {
      conexao = await conectar();
      await conexao.query(CENTROIDES_QUADRAS_CNEFE);
      return { sucesso: true, mensagem: 'Tabela CENTROIDES_QUADRAS_CNEFE criada com sucesso.' };
   } catch (erro) {
      return {
         sucesso: false,
         mensagem: 'Erro ao criar tabela CENTROIDES_QUADRAS_CNEFE.',
         erro: erro.message,
         codigo: erro.code
      };
   } finally {
      if (conexao) await conexao.end();
   }
}

module.exports = {
   CriaTabPontosCnefe,
   CriaPontosUnicosCnefe,
   CriaCentroidesFacesCnefe,
   CriaCentroidesQuadrasCnefe
};
