#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logging
import requests
import time
import hashlib
import psutil
import gc
import subprocess
import json
import re
from typing import Optional, List, Set, Dict, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
from datetime import datetime

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class Config:
    BASE_DIR = os.path.abspath("vatican_docs")
    RAW_DIR = os.path.join(BASE_DIR, "raw")
    TXT_DIR = os.path.join(BASE_DIR, "txt")
    LINKS_DIR = os.path.join(BASE_DIR, "links")
    STATS_DIR = os.path.join(BASE_DIR, "stats")
    ERROR_DIR = os.path.join(BASE_DIR, "errors")
    METADATA_DIR = os.path.join(BASE_DIR, "metadata")
    QUEUE_FILE = os.path.join(BASE_DIR, "url_queue.json")
    PROCESSED_URLS_FILE = os.path.join(BASE_DIR, "processed_urls.json")
    STATS_FILE = os.path.join(STATS_DIR, "crawler_stats.json")
    
    TIMEOUT = 30
    MAX_DOC_SIZE_MB = 25
    MAX_MEMORY_PERCENT = 80
    CHUNK_SIZE = 4096
    MAX_DEPTH = 3
    MAX_URLS = 10000
    MAX_RETRIES = 3
    RETRY_DELAY = 60
    STATS_INTERVAL = 300  # 5 minutos
    
    # Padrões de URLs e conteúdo
    PRIORITY_PATTERNS = [
        '/documents/',
        '/content/francesco/pt/',
        '/archive/ccc/',
        '/archive/cdc/',
        '/holy-father/',
        '/content/benedict-xvi/pt/',
        '/content/john-paul-ii/pt/'
    ]
    
    IGNORE_PATTERNS = [
        '.pdf', '.jpg', '.jpeg', '.png', '.gif',
        'youtube.com', 'twitter.com', 'facebook.com',
        'instagram.com', 'javascript:', 'mailto:'
    ]
    
    PALAVRAS_IMPORTANTES = [
        'encíclica', 'exortação', 'constituição', 'carta apostólica',
        'homilia', 'catequese', 'discurso', 'mensagem', 'bula',
        'motu proprio', 'audiência', 'angelus', 'decreto'
    ]
    
    @classmethod
    def criar_diretorios(cls):
        """Cria os diretórios necessários"""
        for dir_path in [cls.BASE_DIR, cls.RAW_DIR, cls.TXT_DIR, cls.LINKS_DIR,
                        cls.STATS_DIR, cls.ERROR_DIR, cls.METADATA_DIR]:
            os.makedirs(dir_path, exist_ok=True)

class DocumentMetadata:
    def __init__(self, url: str, categoria: str):
        self.url = url
        self.categoria = categoria
        self.titulo = ""
        self.descricao = ""
        self.data_coleta = datetime.utcnow().isoformat()
        self.tamanho_html = 0
        self.tamanho_texto = 0
        self.num_links = 0
        self.links = []
        self.palavras_chave = []
        self.eh_importante = False
        self.prioridade = 0

class CrawlerStats:
    def __init__(self):
        self.inicio = time.time()
        self.ultima_atualizacao = 0
        self.urls_processadas = 0
        self.urls_falhas = 0
        self.bytes_baixados = 0
        self.links_encontrados = 0
        self.erros_timeout = 0
        self.erros_404 = 0
        self.erros_connection = 0
        self.erros_rate_limit = 0
        self.docs_importantes = 0
        self.categorias = {}
        self.carregar_estado()

    def carregar_estado(self):
        """Carrega estatísticas anteriores"""
        try:
            if os.path.exists(Config.STATS_FILE):
                with open(Config.STATS_FILE, 'r') as f:
                    data = json.load(f)
                    self.__dict__.update(data)
        except Exception as e:
            logging.error(f"Erro ao carregar estatísticas: {e}")
    
    def atualizar(self, categoria: str = None, importante: bool = False):
        """Atualiza e salva estatísticas"""
        if categoria:
            self.categorias[categoria] = self.categorias.get(categoria, 0) + 1
        if importante:
            self.docs_importantes += 1
        
        agora = time.time()
        if agora - self.ultima_atualizacao >= Config.STATS_INTERVAL:
            self.imprimir_resumo()
            self.salvar()
            self.ultima_atualizacao = agora
    
    def imprimir_resumo(self):
        """Imprime resumo detalhado das estatísticas"""
        tempo_decorrido = time.time() - self.inicio
        horas = int(tempo_decorrido // 3600)
        minutos = int((tempo_decorrido % 3600) // 60)
        
        taxa_sucesso = (self.urls_processadas / (self.urls_processadas + self.urls_falhas)) * 100 if self.urls_processadas > 0 else 0
        taxa_bytes = self.bytes_baixados / tempo_decorrido / 1024  # KB/s
        media_links = self.links_encontrados / self.urls_processadas if self.urls_processadas > 0 else 0
        
        logging.info("\n" + "="*70)
        logging.info("RELATÓRIO DETALHADO DO CRAWLER")
        logging.info("=" * 70)
        logging.info(f"Tempo de execução: {horas}h {minutos}m")
        logging.info(f"URLs processadas: {self.urls_processadas}")
        logging.info(f"Taxa de sucesso: {taxa_sucesso:.1f}%")
        logging.info(f"Velocidade média: {taxa_bytes:.2f} KB/s")
        logging.info(f"Média de links por página: {media_links:.1f}")
        logging.info(f"Documentos importantes: {self.docs_importantes}")
        logging.info(f"\nErros:")
        logging.info(f"  404: {self.erros_404}")
        logging.info(f"  Timeout: {self.erros_timeout}")
        logging.info(f"  Conexão: {self.erros_connection}")
        logging.info(f"  Rate Limit: {self.erros_rate_limit}")
        logging.info(f"\nTop Categorias:")
        for cat, count in sorted(self.categorias.items(), key=lambda x: x[1], reverse=True)[:5]:
            logging.info(f"  {cat}: {count} documentos")
        logging.info("=" * 70 + "\n")
    
    def salvar(self):
        """Salva estatísticas em arquivo"""
        try:
            with open(Config.STATS_FILE, 'w') as f:
                json.dump(self.__dict__, f, indent=2)
        except Exception as e:
            logging.error(f"Erro ao salvar estatísticas: {e}")

class URLQueue:
    def __init__(self):
        self.queue = deque()
        self.processadas = set()
        self.carregar_estado()
    
    def carregar_estado(self):
        """Carrega estado anterior do crawler"""
        try:
            if os.path.exists(Config.QUEUE_FILE):
                with open(Config.QUEUE_FILE, 'r') as f:
                    data = json.load(f)
                    # Converte URLs antigas (tuplas de 2) para o novo formato (tuplas de 3)
                    for item in data['queue']:
                        if len(item) == 2:  # formato antigo: (url, depth)
                            url, depth = item
                            self.queue.append((url, depth, calcular_prioridade(url, depth)))
                        else:  # formato novo: (url, depth, prioridade)
                            self.queue.append(tuple(item))
                    self.processadas.update(data['processadas'])
        except Exception as e:
            logging.error(f"Erro ao carregar estado: {e}")
    
    def salvar_estado(self):
        """Salva estado atual do crawler"""
        try:
            with open(Config.QUEUE_FILE, 'w') as f:
                json.dump({
                    'queue': list(self.queue),
                    'processadas': list(self.processadas)
                }, f, indent=2)
        except Exception as e:
            logging.error(f"Erro ao salvar estado: {e}")
    
    def adicionar(self, url: str, depth: int = 0, prioridade: int = None):
        """Adiciona URL à fila se não foi processada ainda"""
        if (url not in self.processadas and 
            url not in [x[0] for x in self.queue] and 
            depth <= Config.MAX_DEPTH and
            len(self.processadas) < Config.MAX_URLS and
            not any(p in url for p in Config.IGNORE_PATTERNS)):
            
            if prioridade is None:
                prioridade = calcular_prioridade(url, depth)
            
            self.queue.append((url, depth, prioridade))
            # Ordena a fila por prioridade
            self.queue = deque(sorted(self.queue, key=lambda x: x[2], reverse=True))
    
    def obter_proxima(self) -> Optional[Tuple[str, int, int]]:
        """Retorna próxima URL a ser processada"""
        if not self.queue:
            return None
        
        item = self.queue.popleft()
        if len(item) == 2:  # formato antigo
            url, depth = item
            return (url, depth, calcular_prioridade(url, depth))
        return item  # formato novo
    
    def marcar_processada(self, url: str):
        """Marca URL como processada"""
        self.processadas.add(url)
        self.salvar_estado()

def identificar_categoria(url: str) -> str:
    """Identifica a categoria do documento baseado na URL"""
    if "/holy-father/" in url:
        return "papas"
    elif "/documents/" in url:
        return "documentos"
    elif "/archive/ccc/" in url:
        return "catecismo"
    elif "/archive/cdc/" in url:
        return "direito-canonico"
    elif "/francesco/" in url:
        return "francisco"
    elif "/benedict-xvi/" in url:
        return "bento-xvi"
    elif "/john-paul-ii/" in url:
        return "joao-paulo-ii"
    else:
        return "outros"

def calcular_prioridade(url: str, depth: int) -> int:
    """Calcula prioridade da URL baseado em regras"""
    prioridade = 0
    
    # Prioridade base pela profundidade
    prioridade += max(0, Config.MAX_DEPTH - depth) * 10
    
    # Bônus para documentos importantes
    if any(pattern in url for pattern in Config.PRIORITY_PATTERNS):
        prioridade += 50
    
    # Bônus para documentos em português
    if '/pt/' in url or '_po.' in url:
        prioridade += 30
    
    return prioridade

def eh_conteudo_importante(texto: str) -> bool:
    """Detecta se o conteúdo é importante baseado em palavras-chave"""
    texto_lower = texto.lower()
    return any(palavra in texto_lower for palavra in Config.PALAVRAS_IMPORTANTES)

def extrair_palavras_chave(texto: str, max_palavras: int = 10) -> List[str]:
    """Extrai palavras-chave do texto"""
    palavras = re.findall(r'\b\w+\b', texto.lower())
    freq = {}
    for palavra in palavras:
        if len(palavra) > 3:  # Ignora palavras muito curtas
            freq[palavra] = freq.get(palavra, 0) + 1
    return sorted(freq.keys(), key=lambda x: freq[x], reverse=True)[:max_palavras]

def extrair_titulo(soup: BeautifulSoup) -> str:
    """Extrai título da página"""
    if soup.title:
        return soup.title.string.strip() if soup.title.string else ""
    h1 = soup.find('h1')
    if h1:
        return h1.get_text().strip()
    return ""

def extrair_descricao(soup: BeautifulSoup) -> str:
    """Extrai descrição da página"""
    meta_desc = soup.find('meta', {'name': 'description'})
    if meta_desc:
        return meta_desc.get('content', '').strip()
    return ""

def forcar_limpeza_memoria():
    """Força uma limpeza mais agressiva da memória"""
    try:
        gc.collect()
        subprocess.run(['sync'], check=True)
        try:
            with open('/proc/sys/vm/drop_caches', 'w') as f:
                f.write('3')
        except PermissionError:
            logging.warning("Sem permissão para limpar caches do sistema")
        time.sleep(1)
    except Exception as e:
        logging.error(f"Erro ao limpar memória: {e}")

def verificar_memoria():
    """Verifica o uso de memória"""
    try:
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        logging.info(f"Memória: {mem.percent}% usado | Swap: {swap.percent}% usado")
        
        if mem.percent > Config.MAX_MEMORY_PERCENT:
            logging.info("Iniciando limpeza de memória...")
            forcar_limpeza_memoria()
            mem = psutil.virtual_memory()
            logging.info(f"Após limpeza - Memória: {mem.percent}% usado")
        
        return mem.percent < Config.MAX_MEMORY_PERCENT
    except Exception as e:
        logging.error(f"Erro ao verificar memória: {e}")
        return False

def extrair_links(html_content: str, base_url: str) -> List[str]:
    """Extrai links relevantes da página"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        links = set()
        
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(base_url, href)
            
            if ('vatican.va' in full_url and 
                any(lang in full_url for lang in ['/pt/', '_po.', '/po/'])):
                links.add(full_url)
        
        return list(links)
    except Exception as e:
        logging.error(f"Erro ao extrair links: {e}")
        return []

def baixar_e_processar(url: str, depth: int, stats: CrawlerStats) -> Optional[DocumentMetadata]:
    """Baixa e processa uma página"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        categoria = identificar_categoria(url)
        metadata = DocumentMetadata(url, categoria)
        
        nome_arquivo = hashlib.md5(url.encode()).hexdigest()
        caminho_raw = os.path.join(Config.RAW_DIR, f"{nome_arquivo}.html")
        caminho_txt = os.path.join(Config.TXT_DIR, f"{nome_arquivo}.txt")
        caminho_meta = os.path.join(Config.METADATA_DIR, f"{nome_arquivo}.json")
        
        with requests.get(url, headers=headers, stream=True, timeout=Config.TIMEOUT) as response:
            response.raise_for_status()
            
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
            
            stats.bytes_baixados += total_size
            logging.info(f"Arquivo raw salvo: {caminho_raw}")
            
            # Processa o conteúdo
            html_content = content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extrai metadados
            metadata.titulo = extrair_titulo(soup)
            metadata.descricao = extrair_descricao(soup)
            metadata.tamanho_html = total_size
            
            # Extrai texto
            for script in soup(["script", "style"]):
                script.decompose()
            texto = soup.get_text(separator='\n', strip=True)
            metadata.tamanho_texto = len(texto)
            
            # Salva texto
            with open(caminho_txt, 'w', encoding='utf-8') as f:
                f.write(texto)
            logging.info(f"Arquivo texto salvo: {caminho_txt}")
            
            # Extrai e salva links
            links = extrair_links(html_content, url)
            metadata.links = links
            metadata.num_links = len(links)
            stats.links_encontrados += len(links)
            
            # Extrai palavras-chave e verifica importância
            metadata.palavras_chave = extrair_palavras_chave(texto)
            metadata.eh_importante = eh_conteudo_importante(texto)
            metadata.prioridade = calcular_prioridade(url, depth)
            
            # Salva metadados
            with open(caminho_meta, 'w', encoding='utf-8') as f:
                json.dump(metadata.__dict__, f, ensure_ascii=False, indent=2)
            
            return metadata
            
    except Exception as e:
        logging.error(f"Erro ao baixar e processar {url}: {e}")
        return None

def main():
    Config.criar_diretorios()
    stats = CrawlerStats()
    queue = URLQueue()
    
    try:
        if not queue.queue