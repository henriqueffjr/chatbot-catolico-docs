# Chat Católico - Documentos do Vaticano

![GitHub](https://img.shields.io/github/license/henriqueffjr/chatbot-catolico-docs)
![Python](https://img.shields.io/badge/python-3.8+-blue.svg)

Um sistema completo para coleta, processamento e disponibilização de documentos oficiais da Igreja Católica, incluindo encíclicas, exortações, homilias e outros textos do Vaticano.

## 📦 Recursos Principais

- **Crawler Automatizado**: Coleta documentos do site do Vaticano
- **Processamento Avançado**:
  - Extração de metadados
  - Identificação de referências bíblicas
  - Sumarização automática de textos
- **API RESTful**: Acesso programático aos documentos
- **Busca Semântica**: Encontre documentos por conteúdo teológico

## 🛠️ Instalação

### Pré-requisitos
- Python 3.8+
- Git
- pip

### Configuração

1. Clone o repositório:

```bash
git clone https://github.com/henriqueffjr/chatbot-catolico-docs.git
cd chatbot-catolico-docs
```

2. Configure o ambiente virtual:

```bash
python -m venv venv
source venv/bin/activate  # Linux/MacOS
venv\Scripts\activate     # Windows
```

3. Instale as dependências:

```bash
pip install -r requirements.txt
```

## 🚀 Como Usar

### 1. Executar o Crawler

```bash
python crawler/vatican_crawler.py
```

### 2. Iniciar a API

```bash
cd api
uvicorn main:app --reload
```

A API estará disponível em http://localhost:8000 com documentação Swagger em http://localhost:8000/docs

### 3. Usar no seu Chatbot

```python
import requests

response = requests.post(
    "http://localhost:8000/search",
    json={"text": "Eucaristia", "lang": "pt"}
)
documents = response.json()
```

## 🗂 Estrutura do Projeto

```
chatbot-catolico-docs/
├── crawler/             # Código de coleta de documentos
├── api/                 # API de acesso aos documentos
├── processing/          # Processamento de texto avançado
├── data/                # Documentos coletados
├── tests/               # Testes automatizados
├── LICENSE              # Licença MIT
└── README.md            # Este arquivo
```

## 🤝 Como Contribuir

1. Faça um fork do projeto  
2. Crie sua branch (`git checkout -b feature/nova-feature`)  
3. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`)  
4. Push para a branch (`git push origin feature/nova-feature`)  
5. Abra um Pull Request

## 📄 Licença

Este projeto está licenciado sob a Licença MIT – veja o arquivo LICENSE para mais detalhes.

## 🙏 Reconhecimentos

- Site oficial do Vaticano por disponibilizar os documentos
- Hugging Face pelos modelos de sumarização
- Comunidade open source pelas bibliotecas utilizadas

## 📛 Badges Adicionais (opcional)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)  
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)  
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
