# 📚 Chatbot Católico - Documentos Oficiais da Igreja

Este repositório armazena **documentos oficiais da Igreja Católica** coletados diretamente do site do Vaticano, organizados por **tipo, idioma, ano e Papa**, com metadados para facilitar o uso em aplicações como chatbots, pesquisas e projetos acadêmicos.

## 📌 O que está incluído?

- 📜 **Catecismo da Igreja Católica** (português, inglês, latim)
- 📗 **Código de Direito Canônico**
- 📘 **Compêndio da Doutrina Social da Igreja**
- 🧾 **Documentos dos Papas** (discursos, encíclicas, cartas, angelus etc.)
- 🏛️ **Concílios** (incluindo Vaticano I e II)
- 🗂️ **Atos Oficiais da Santa Sé**
  - *Acta Sanctae Sedis (ASS)*
  - *Acta Apostolicae Sedis (AAS)*
  - *Documentos sobre a II Guerra Mundial*

## 🧠 Finalidade

Este acervo serve como base para um **chatbot treinado com doutrina católica oficial**, garantindo respostas confiáveis e embasadas nos documentos da Igreja.

## 🗃️ Estrutura

```
documentos/
├── papas/
│   ├── francesco/
│   └── ...
├── catecismo/
├── compendio/
├── codigo-direito-canonico/
├── concilios/
├── atos-oficiais/
│   ├── acta-apostolicae-sedis/
│   ├── acta-sanctae-sedis/
│   └── documentos-ii-guerra/
├── metadados/
├── cache/
└── logs/
```

## 📥 Coleta automatizada

A coleta é feita com um script Python executável via Google Colab, que:

- Acessa recursivamente páginas e sublinks do Vaticano
- Baixa arquivos `.txt`, `.html`, `.pdf`
- Organiza os documentos por idioma, ano e tipo
- Gera metadados `.json` e logs de coleta
- Armazena cache para evitar repetições

## ⚠️ Observação

Os arquivos PDF são salvos como estão, sem OCR. Traduções automáticas são geradas apenas quando necessário e indicadas nos metadados.

## 📅 Atualização

Você pode rodar o script uma vez por semana via [Google Colab](https://colab.research.google.com/) para manter os dados atualizados com novos documentos publicados pelo Vaticano.

## 🛠️ Contribuição

Contribuições são bem-vindas! Envie um pull request ou abra uma issue para sugestões e melhorias.

---

**Desenvolvido por:** [Henrique Frazão Jr.](https://github.com/henriqueffjr)  
**Licença:** [MIT](LICENSE)
