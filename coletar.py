import os
import sys
import time
import json
import hashlib
import logging
import gc
import requests
import random
import psutil
from collections import Counter  # <-- Importação corrigida!
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict

from config import Config
from nlp import NLPProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_random_user_agent():
    return random.choice(Config.USER_AGENTS)

def normalizar_url(url: str) -> str:
    parsed = urlparse(url)
    path = os.path.normpath(parsed.path).rstrip("/")
    return parsed._replace(path=path, fragment="").geturl()

def identificar_categoria(url: str) -> str:
    url = url.lower()
    if "/holy-father/" in url or "/papa/" in url:
        return "papas"
    elif "/content/francesco/" in url:
        return "francisco"
    elif "/content/benedict-xvi/" in url:
        return "bento-xvi"
    elif "/content/john-paul-ii/" in url:
        return "joao-paulo-ii"
    elif "/documents/" in url:
        return "documentos"
    elif "/archive/ccc/" in url or "catechism" in url:
        return "catecismo"
    elif "/archive/cdc/" in url or "canon-law" in url:
        return "direito-canonico"
    elif "/roman_curia/" in url or "/content/romancuria/" in url:
        return "curia-romana"
    elif "/content/speeches/" in url or "/content/messages/" in url:
        return "discursos-mensagens"
    elif "/content/homilies/" in url or "/content/angelus/" in url:
        return "homilias-angelus"
    elif "/archive/" in url:
        return "arquivo"
    else:
        return "outros"

def extrair_links(html_content: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html_content, 'html.parser')
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
            continue
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if ('vatican.va' in parsed.netloc and
                parsed.scheme in ('http', 'https') and
                not any(p in full_url for p in Config.IGNORE_PATTERNS)):
            normalized_url = normalizar_url(full_url)
            if normalized_url:
                links.add(normalized_url)
    return list(links)

def eh_tipo_valido(response):
    content_type = response.headers.get('content-type', '').lower()
    return 'text/html' in content_type

def extrair_titulo(soup: BeautifulSoup) -> str:
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    h1 = soup.find('h1')
    if h1:
        return h1.get_text().strip()
    return ""

def extrair_texto_principal(soup: BeautifulSoup) -> str:
    for tag in ['script', 'style', 'nav', 'footer', 'iframe', 'header']:
        for element in soup.find_all(tag):
            element.decompose()
    main_content = (
        soup.find('main') or
        soup.find('div', {'id': 'content'}) or
        soup.find('div', {'class': 'content'}) or
        soup.find('article') or
        soup
    )
    return main_content.get_text(separator='\n', strip=True)

def extrair_palavras_chave(texto: str, max_palavras: int = 15) -> List[str]:
    import re
    palavras = re.findall(r'\b\w+\b', texto.lower())
    freq = {}
    stop_words = {'de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas',
                  'por', 'para', 'com', 'e', 'ou', 'que', 'se', 'um', 'uma', 'os', 'as'}
    for palavra in palavras:
        if len(palavra) > 3 and palavra not in stop_words:
            freq[palavra] = freq.get(palavra, 0) + 1
    return sorted(freq.keys(), key=lambda x: freq[x], reverse=True)[:max_palavras]

def eh_conteudo_importante(texto: str) -> bool:
    texto_lower = texto.lower()
    if any(palavra in texto_lower for palavra in Config.PALAVRAS_IMPORTANTES):
        return True
    if len(texto.split()) > 200:
        return True
    return False

class DocumentMetadata:
    def __init__(self, url: str, categoria: str):
        self.url = url
        self.categoria = categoria
        self.titulo = ""
        self.data_coleta = datetime.utcnow().isoformat()
        self.tamanho_html = 0
        self.tamanho_texto = 0
        self.num_links = 0
        self.links = []
        self.palavras_chave = []
        self.eh_importante = False
        self.prioridade = 0
        self.idioma = ""
        self.texto = ""
        self.resumo = ""
        self.entidades = {}
        self.temas = []
        self.sentimentos = {}
        self.termos_doutrinais = {}
        self.estatisticas_linguisticas = {}
        self.tipo_documento = ""

    def update(self, dados: Dict):
        for chave, valor in dados.items():
            setattr(self, chave, valor)

    def to_dict(self) -> Dict:
        return self.__dict__

def baixar_e_processar(url: str, depth: int = 0) -> Optional[DocumentMetadata]:
    try:
        session = requests.Session()
        session.headers.update({
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'
        })
        categoria = identificar_categoria(url)
        metadata = DocumentMetadata(url, categoria)
        nome_arquivo = hashlib.md5(url.encode()).hexdigest()
        caminho_raw = os.path.join(Config.RAW_DIR, f"{nome_arquivo}.html")
        with session.get(url, stream=True, timeout=Config.TIMEOUT) as response:
            response.raise_for_status()
            if not eh_tipo_valido(response):
                logging.warning(f"Tipo inválido: {url}")
                return None
            content = b''
            total_size = 0
            with open(caminho_raw, 'wb') as f:
                for chunk in response.iter_content(chunk_size=Config.CHUNK_SIZE):
                    if chunk:
                        total_size += len(chunk)
                        if total_size > Config.MAX_DOC_SIZE_MB * 1024 * 1024:
                            os.remove(caminho_raw)
                            logging.warning(f"Arquivo muito grande: {url}")
                            return None
                        content += chunk
                        f.write(chunk)
            html_content = content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_content, 'html.parser')
            metadata.texto = extrair_texto_principal(soup)
            metadata.tamanho_texto = len(metadata.texto)
            metadata.titulo = extrair_titulo(soup)
            metadata.tamanho_html = total_size
            metadata.palavras_chave = extrair_palavras_chave(metadata.texto)
            metadata.eh_importante = eh_conteudo_importante(metadata.texto)
            metadata.prioridade = 100 - depth * 10
            links = extrair_links(html_content, url)
            metadata.links = links
            metadata.num_links = len(links)
            nlp_processor = NLPProcessor()
            analise_nlp = nlp_processor.processar_documento(metadata.texto, doc_id=nome_arquivo)
            metadata.update({
                'resumo': analise_nlp.get('resumo', ''),
                'entidades': analise_nlp.get('entidades', {}),
                'temas': analise_nlp.get('temas', []),
                'sentimentos': analise_nlp.get('sentimentos', {}),
                'termos_doutrinais': analise_nlp.get('termos_doutrinais', {}),
                'estatisticas_linguisticas': analise_nlp.get('estatisticas', {})
            })
            if metadata.termos_doutrinais:
                categorias = list(metadata.termos_doutrinais.keys())
                metadata.tipo_documento = categorias[0] if categorias else "outro"
            # Salva txt, links, metadata
            with open(os.path.join(Config.TXT_DIR, f"{nome_arquivo}.txt"), "w", encoding="utf-8") as f:
                f.write(metadata.texto)
            with open(os.path.join(Config.LINKS_DIR, f"{nome_arquivo}.json"), "w", encoding="utf-8") as f:
                json.dump(metadata.links, f, ensure_ascii=False, indent=2)
            with open(os.path.join(Config.METADATA_DIR, f"{nome_arquivo}.json"), "w", encoding="utf-8") as f:
                json.dump(metadata.to_dict(), f, ensure_ascii=False, indent=2)
            return metadata
    except Exception as e:
        logging.error(f"Erro ao processar {url}: {e}")
        return None

def main():
    print("Iniciando o crawler...")  # Para visualização no terminal
    Config.criar_diretorios()
    urls_iniciais = [
        "https://www.vatican.va/content/vatican/pt.html",
        "https://www.vatican.va/content/francesco/pt.html"
    ]
    for url in urls_iniciais:
        print(f"Processando: {url}")
        baixar_e_processar(url, depth=0)
        gc.collect()
    print("Crawler finalizado.")

if __name__ == "__main__":
    main()