// municipios.js
async function carregarMunicipios(selectId, csvPath = "municipios.csv") {
  const select = document.getElementById(selectId);
  if (!select) return;

  try {
    const resp = await fetch(csvPath);
    if (!resp.ok) throw new Error("Erro ao carregar municipios.csv");
    const texto = await resp.text();

    // Quebra as linhas e mapeia as colunas
    const linhas = texto.trim().split("\n");
    const municipios = linhas.map(l => {
      const [codigo, nome, uf] = l.split(";").map(v => v.trim());
      return { codigo, nome, uf };
    });

    // Limpa e adiciona a opção padrão
    select.innerHTML = '<option value="">Selecione o município...</option>';

    // Adiciona opções ao combo
    municipios.forEach(m => {
      const opt = document.createElement("option");
      opt.value = m.codigo;
      opt.textContent = `${m.nome} (${m.uf})`;
      opt.dataset.uf = m.uf;
      select.appendChild(opt);
    });

    // Recarrega seleção anterior (se existir)
    const salvo = localStorage.getItem("cod_municipio");
    const ufSalva = localStorage.getItem("uf_municipio");
    if (salvo) {
      select.value = salvo;
      console.log(`Município restaurado: ${salvo} (${ufSalva})`);
    }

    // Atualiza armazenamento quando mudar
    select.addEventListener("change", () => {
      const cod = select.value;
      const uf = select.options[select.selectedIndex]?.dataset?.uf || "";
      if (cod) {
        localStorage.setItem("cod_municipio", cod);
        localStorage.setItem("uf_municipio", uf);
        console.log(`Município selecionado: ${cod} (${uf})`);
      } else {
        localStorage.removeItem("cod_municipio");
        localStorage.removeItem("uf_municipio");
      }
    });

  } catch (e) {
    console.error("Erro ao carregar municípios:", e);
    select.innerHTML = '<option value="">Erro ao carregar municípios</option>';
  }
}
