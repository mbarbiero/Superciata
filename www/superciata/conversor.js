const fs = require('fs');

function csvToJson(csvData) {
    try {
        const lines = csvData.trim().split('\n');
        const result = {};
        let linhasProcessadas = 0;
        let linhasIgnoradas = 0;
        
        // Validar cabeçalho
        const cabecalho = lines[0].split(';');
        if (cabecalho[0] !== 'COD_MUNICIPIO' || cabecalho[1] !== 'NOM_MUNICIPIO' || cabecalho[2] !== 'UF') {
            console.warn('Cabeçalho do CSV não corresponde ao formato esperado');
        }
        
        for (let i = 1; i < lines.length; i++) {
            const line = lines[i].trim();
            if (!line) {
                linhasIgnoradas++;
                continue;
            }
            
            const parts = line.split(';');
            
            if (parts.length < 3) {
                console.warn(`Linha ${i + 1} ignorada - formato inválido`);
                linhasIgnoradas++;
                continue;
            }
            
            const codigo = parts[0].trim();
            const nome = parts[1].trim();
            const uf = parts[2].trim();
            
            if (!codigo || !nome || !uf) {
                console.warn(`Linha ${i + 1} ignorada - dados incompletos`);
                linhasIgnoradas++;
                continue;
            }
            
            if (!result[uf]) {
                result[uf] = [];
            }
            
            result[uf].push({
                codigo: codigo,
                nome: nome
            });
            
            linhasProcessadas++;
        }
        
        console.log(`Linhas processadas: ${linhasProcessadas}`);
        console.log(`Linhas ignoradas: ${linhasIgnoradas}`);
        
        // Ordenar
        const sortedResult = {};
        Object.keys(result)
            .sort()
            .forEach(uf => {
                sortedResult[uf] = result[uf].sort((a, b) => 
                    a.nome.localeCompare(b.nome)
                );
            });
        
        return sortedResult;
        
    } catch (error) {
        console.error('Erro na conversão:', error.message);
        return {};
    }
}

// Ler e processar arquivo
try {
    const csvData = fs.readFileSync('municipios.csv', 'utf8');
    const jsonResult = csvToJson(csvData);
    
    if (Object.keys(jsonResult).length > 0) {
        fs.writeFileSync('municipios.json', JSON.stringify(jsonResult, null, 2));
        console.log('\n✅ Arquivo JSON gerado com sucesso!');
        
        // Relatório final
        console.log('\n📊 RELATÓRIO FINAL:');
        console.log('==================');
        Object.keys(jsonResult).forEach(uf => {
            console.log(`🏙️  ${uf}: ${jsonResult[uf].length} municípios`);
        });
        
        const totalMunicipios = Object.values(jsonResult)
            .reduce((total, municipios) => total + municipios.length, 0);
        
        console.log(`\n📈 Total: ${Object.keys(jsonResult).length} UFs e ${totalMunicipios} municípios`);
    } else {
        console.log('❌ Nenhum dado foi processado');
    }
    
} catch (error) {
    console.error('❌ Erro ao processar arquivo:', error.message);
}