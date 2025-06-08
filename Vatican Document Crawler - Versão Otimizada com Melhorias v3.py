-- coding: utf-8 --

"""Vatican Document Crawler - Versão Otimizada com Melhorias"""

import os
import requests
from bs4 import BeautifulSoup
import pdfplumber
import html2text
from urllib.parse import urljoin, urlparse
import hashlib
from datetime import datetime
import json
import csv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import subprocess
import re
from collections import Counter
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Set
from transformers import pipeline
import uvicorn
import torch
import time
import uuid
from enum import Enum
from functools import lru_cache, partial
import zipfile
from jsonschema import validate
from tqdm import tqdm
import importlib
import socket
from prometheus_client import start_http_server, Counter, Gauge, Histogram, Summary
from langdetect import detect
import heapq
import threading
import concurrent.futures
import functools
import pickle
import glob
import nltk
from nltk.tokenize import sent_tokenize
from sentence_transformers import SentenceTransformer, util
from xml.etree.ElementTree import Element, tostring
from collections import defaultdict
import smtplib
from email.mime.text import MIMEText
import random

==================== CONFIGURAÇÕES AVANÇADAS ====================

class Config:
# Configurações básicas
MONITOR_PORT = int(os.getenv("MONITOR_PORT", 8001))
BASE_URL = "https://www.vatican.va"
LANGUAGES = ["pt", "en", "la", "it"]
MAX_DEPTH = 6
TIMEOUT = 30
MAX_RETRIES = 3
MAX_FILES = 50000
MIN_FILES_TO_PUSH = 1  # Mínimo de arquivos para enviar ao GitHub
MAX_WORKERS = 5  # Threads para processamento paralelo
RATE_LIMIT_DELAY = 0.5  # Delay entre requisições em segundos
CACHE_TTL = 300  # 5 minutos
PLUGINS = []  # Plugins para carregar dinamicamente
HEARTBEAT_INTERVAL = 60  # Segundos para verificação de atividade

# Caminhos bloqueados  
BLOCKED_PATHS = [  
    "/content/vatican/en/search.html",  
    "/photogallery/",  
    "/news_services/",  
    "/resources/",  
    "/siti_va/",  
    "/latest/"  
]  

# Tipos de documentos  
DOC_TYPES = {  
    'enciclicas': ['encyclical', 'enciclica'],  
    'exortacoes': ['exhortation', 'exortacao'],  
    'homilias': ['homily', 'homilia'],  
    'mensagens': ['message', 'mensagem'],  
    'discursos': ['speech', 'discourse', 'discurso'],  
    'angelus': ['angelus'],  
    'audiencias': ['audience', 'audiencia']  
}  

SPECIAL_DOCUMENTS = {  
    'codigo_direito_canonico': 'https://www.vatican.va/archive/cdc/index_po.htm',  
    'catecismo': 'https://www.vatican.va/archive/ccc/index_po.htm',  
    'compendio_doutrina': 'https://www.vatican.va/roman_curia/pontifical_councils/justpeace/documents/rc_pc_justpeace_doc_20060526_compendio-dott-soc_po.html',  
    'atos_oficiais': 'https://www.vatican.va/archive/atti-ufficiali-santa-sede/index_po.htm',  
    'concilios': 'https://www.vatican.va/archive/hist_councils/index_po.htm',  
    'enciclicas': 'https://www.vatican.va/content/vatican/pt.html',  # link raiz — pode ser usado para rastrear todas  
    'doutrina_social': 'https://www.vatican.va/roman_curia/pontifical_councils/justpeace/documents/index_po.htm',  
    'liturgia': 'https://www.vatican.va/news_services/liturgy/index_po.htm',  
    'documentos_papas': 'https://www.vatican.va/content/francesco/pt/index.html'  
}  

# GitHub  
GITHUB_TOKEN = "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"  
REPO_NAME = "henriqueffjr/chatbot-catolico-docs"  
GIT_USER = "Vatican Bot"  
GIT_EMAIL = "vatican-bot@example.com"  

# Entidades sagradas  
SACRED_ENTITIES = {  
    'deus': ['senhor', 'todo-poderoso', 'altíssimo', 'pai eterno', 'criador'],  
    'maria': ['virgem maria', 'nossa senhora', 'mãe de deus', 'imaculada', 'mãe da igreja'],  
    'jesus': ['cristo', 'filho de deus', 'salvador', 'messias', 'cordeiro de deus', 'verbo encarnado'],  
    'espirito_santo': ['espírito santo', 'paráclito', 'espírito de deus'],  
    'santos': ['são josé', 'são pedro', 'santa teresinha', 'beato carlo acutis', 'são joão paulo ii']  
}  

# Tags doutrinais  
DOCTRINE_TAGS = {  
    'sacramentos': ['batismo', 'eucaristia', 'crisma', 'penitência', 'matrimônio', 'ordem', 'unção dos enfermos'],  
    'moral': ['pecado', 'virtude', 'consciência', 'tentação', 'graça', 'liberdade'],  
    'igreja': ['bispos', 'padres', 'concílio', 'magistério', 'papa', 'diocese', 'paróquia'],  
    'vida_crista': ['oração', 'jejum', 'caridade', 'fé', 'esperança', 'amor'],  
    'escatologia': ['juízo final', 'céu', 'inferno', 'purgatório', 'ressurreição'],  
    'biblia': ['revelação', 'inspiração divina', 'evangelho', 'escritura sagrada'],  
    'doutrina_social': ['justiça social', 'bem comum', 'trabalho humano', 'família', 'solidariedade']  
}  

# Livros bíblicos  
LIVROS_BIBLICOS = {  
    # Antigo Testamento  
    'gn': 'Gênesis', 'ex': 'Êxodo', 'lv': 'Levítico', 'nm': 'Números', 'dt': 'Deuteronômio',  
    'sl': 'Salmos', 'pv': 'Provérbios', 'is': 'Isaías', 'jr': 'Jeremias', 'dn': 'Daniel',  

    # Novo Testamento  
    'mt': 'Mateus', 'mc': 'Marcos', 'lc': 'Lucas', 'jo': 'João',  
    'atos': 'Atos dos Apóstolos', 'rm': 'Romanos', '1cor': '1 Coríntios', '2cor': '2 Coríntios',  
    'ap': 'Apocalipse'  
}  

# Sumarização  
SUMMARY_MODEL = "facebook/bart-large-cnn"  
SUMMARY_MAX_LENGTH = 150  
SUMMARY_MIN_LENGTH = 30  

# Schema para validação de documentos  
DOCUMENT_SCHEMA = {  
    "type": "object",  
    "properties": {  
        "id": {"type": "string"},  
        "url": {"type": "string"},  
        "pope": {"type": "string"},  
        "type": {"type": "string"},  
        "lang": {"type": "string"},  
        "filename": {"type": "string"},  
        "full_path": {"type": "string"},  
        "pub_date": {"type": ["string", "null"]},  
        "timestamp": {"type": "string"},  
        "batch_id": {"type": "string"}  
    },  
    "required": ["id", "url", "pope", "type", "lang", "filename"]  
}  

@classmethod  
def from_env(cls):  
    """Carrega configurações de variáveis de ambiente"""  
    config = cls()  
    config.MAX_FILES = int(os.getenv('MAX_FILES', config.MAX_FILES))  
    config.MIN_FILES_TO_PUSH = int(os.getenv('MIN_FILES_TO_PUSH', config.MIN_FILES_TO_PUSH))  
    config.MAX_WORKERS = int(os.getenv('MAX_WORKERS', config.MAX_WORKERS))  
    config.GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', config.GITHUB_TOKEN)  
    config.HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', config.HEARTBEAT_INTERVAL))  
    return config

==================== SISTEMA DE CACHE ====================

class DocumentCache:
def init(self, crawler):
self.crawler = crawler
self.cache_dir = os.path.join(crawler.base_dir, "cache")
os.makedirs(self.cache_dir, exist_ok=True)

def get_cache_key(self, url):  
    return hashlib.md5(url.encode()).hexdigest()  
      
def get(self, url):  
    key = self.get_cache_key(url)  
    cache_file = os.path.join(self.cache_dir, f"{key}.cache")  
      
    if os.path.exists(cache_file):  
        with open(cache_file, 'rb') as f:  
            data = pickle.load(f)  
            if time.time() - data['timestamp'] < self.crawler.config.CACHE_TTL:  
                return data['content']  
    return None  
      
def set(self, url, content):  
    key = self.get_cache_key(url)  
    cache_file = os.path.join(self.cache_dir, f"{key}.cache")  
      
    data = {  
        'url': url,  
        'content': content,  
        'timestamp': time.time()  
    }  
      
    with open(cache_file, 'wb') as f:  
        pickle.dump(data, f)

==================== SISTEMA DE PRIORIZAÇÃO ====================

class SmartPriorityQueue(PriorityQueue):
def init(self, crawler):
super().init()
self.crawler = crawler
self.url_scores = {}

def calculate_priority(self, url):  
    """Calcula prioridade baseada em múltiplos fatores"""  
    score = 0  
      
    # Prioriza documentos oficiais  
    for doc_type, special_url in self.crawler.config.SPECIAL_DOCUMENTS.items():  
        if special_url in url:  
            score += 100  
            break  
              
    # Prioriza por idioma (português primeiro)  
    if '/pt/' in url.lower():  
        score += 50  
    elif '/en/' in url.lower():  
        score += 30  
          
    # Prioriza por tipo de documento  
    for dtype, keywords in self.crawler.config.DOC_TYPES.items():  
        if any(kw in url.lower() for kw in keywords):  
            score += 20  
            break  
              
    # Penaliza URLs longas (geralmente menos importantes)  
    score -= min(len(url) // 10, 20)  
      
    return score  
  
def add_url(self, url):  
    priority = self.calculate_priority(url)  
    super().add_url(url, priority)  
    self.url_scores[url] = priority

==================== CRAWLER OTIMIZADO ====================

class VaticanCrawler:
def init(self):
self.config = Config()
self.base_dir = os.path.join(os.getcwd(), "vatican_docs")
self.repo_url = f"https://{self.config.GITHUB_TOKEN}@github.com/{self.config.REPO_NAME}.git"
self.setup_logging()
self.setup_folders()
self.session = self.setup_session()
self.summarizer = self.setup_summarizer()
self.downloaded_urls: Set[str] = set()
self.file_count = 0
self.error_count = 0
self.start_time = time.time()
self.current_batch_id = str(uuid.uuid4())
self.processors = []
self.priority_queue = PriorityQueue()
self.last_activity_time = time.time()
self.heartbeat_interval = self.config.HEARTBEAT_INTERVAL

# Sistemas avançados  
    self.document_cache = DocumentCache(self)  
    self.plugin_system = PluginSystem(self)  
    self.monitoring = AdvancedMonitoring(self)  
    self.validator = TheologicalValidator()  
    self.translation = TranslationSystem()  
    self.indexer = DocumentIndexer(self)  
    self.notifier = NotificationSystem(self)  
    self.cleaner = AutoCleaner(self)  
    self.updater = AutoUpdater(self)  
    self.trend_analyzer = TrendAnalyzer(self)  
    self.exporter = ChurchFormatsExporter(self)  
      
    # Sistema de checkpoint  
    self.checkpoint_path = os.path.join(self.base_dir, "checkpoints", "download_checkpoint.txt")  
    self.setup_checkpoint_system()  
    self.load_downloaded_urls()  
      
    # Monitoramento  
    self.request_counter = Counter('requests_total', 'Total de URLs processadas')  
    self.start_monitoring()  
    self.setup_heartbeat_monitor()  
      
    # Plugins  
    self.load_plugins()  

def setup_heartbeat_monitor(self):  
    """Monitora atividade do crawler para detectar travamentos"""  
    def monitor():  
        while True:  
            time.sleep(self.heartbeat_interval)  
            inactive_time = time.time() - self.last_activity_time  
            if inactive_time > self.heartbeat_interval * 2:  
                self.logger.error(f"Possível travamento detectado - Sem atividade por {inactive_time:.1f}s")  
                # Tenta recuperação automática  
                self.recover_from_stall()  

    threading.Thread(target=monitor, daemon=True).start()  

def recover_from_stall(self):  
    """Tenta recuperar de um travamento"""  
    self.logger.warning("Iniciando recuperação de travamento...")  
    try:  
        # 1. Reinicia a sessão HTTP  
        self.session.close()  
        self.session = self.setup_session()  
          
        # 2. Limpa workers presos  
        if hasattr(self, '_executor'):  
            self._executor.shutdown(wait=False)  
          
        # 3. Registra o incidente  
        self.error_count += 1  
        self.logger.warning("Recuperação de travamento concluída")  
    except Exception as e:  
        self.logger.error(f"Falha na recuperação: {str(e)}")  

def setup_session(self):  
    """Configura a sessão HTTP com timeouts robustos"""  
    session = requests.Session()  
      
    # Timeouts mais agressivos  
    retry_strategy = Retry(  
        total=self.config.MAX_RETRIES,  
        backoff_factor=1,  
        status_forcelist=[408, 429, 500, 502, 503, 504],  
        allowed_methods=["GET", "HEAD"],  
        raise_on_status=False  # Não levanta exceção após retries  
    )  
      
    adapter = HTTPAdapter(  
        max_retries=retry_strategy,  
        pool_connections=20,  
        pool_maxsize=100,  
        pool_block=True  
    )  
      
    session.mount("https://", adapter)  
    session.mount("http://", adapter)  
      
    # Timeouts explícitos para todas as operações  
    session.request = functools.partial(  
        session.request,  
        timeout=(self.config.TIMEOUT, self.config.TIMEOUT)  # Connect + Read timeout  
    )  
      
    # Headers personalizados  
    session.headers.update({  
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',  
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',  
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'  
    })  
      
    return session  

def setup_checkpoint_system(self):  
    """Configuração completa do sistema de checkpoint"""  
    os.makedirs(os.path.dirname(self.checkpoint_path), exist_ok=True)  
      
    if not self.verify_checkpoint_integrity():  
        self.logger.warning("Problemas no arquivo de checkpoint, criando novo")  
        open(self.checkpoint_path, 'a').close()  

def load_downloaded_urls(self):  

self.failed_urls_path = os.path.join(self.base_dir, "checkpoints", "failed_urls.txt")
self.failed_urls = set()
if os.path.exists(self.failed_urls_path):
    with open(self.failed_urls_path, "r", encoding="utf-8") as f:
        self.failed_urls = set(line.strip() for line in f if line.strip())


    """Carrega URLs processadas com tratamento robusto"""  
    try:  
        if os.path.exists(self.checkpoint_path):  
            with open(self.checkpoint_path, 'r', encoding='utf-8') as f:  
                self.downloaded_urls = {line.strip() for line in f if line.strip()}  
            self.logger.info(f"Checkpoint carregado - {len(self.downloaded_urls)} URLs registradas")  
        else:  
            self.logger.info("Iniciando novo arquivo de checkpoint")  
            self.downloaded_urls = set()  
    except Exception as e:  
        self.logger.error(f"Falha ao carregar checkpoint: {str(e)}")  
        self.downloaded_urls = set()  

def save_downloaded_url(self, url):  
    """Salva URL atomicamente com tratamento de erro"""  
    try:  
        with open(self.checkpoint_path, 'a', encoding='utf-8') as f:  
            f.write(f"{url}\n")  
        self.downloaded_urls.add(url)  
    except Exception as e:  
        self.logger.error(f"Falha ao registrar URL no checkpoint: {str(e)}")  

def verify_checkpoint_integrity(self):  
    """Verifica se o arquivo de checkpoint está acessível"""  
    try:  
        # Testa leitura e escrita  
        if os.path.exists(self.checkpoint_path):  
            with open(self.checkpoint_path, 'r+', encoding='utf-8') as f:  
                f.read(1)  
                f.write('')  # Teste de escrita  
        return True  
    except Exception as e:  
        self.logger.error(f"Problema de integridade no checkpoint: {str(e)}")  
        return False  

def cleanup_checkpoint(self):  
    """Opcional: limpa URLs inválidas do checkpoint"""  
    valid_urls = set()  
    try:  
        with open(self.checkpoint_path, 'r', encoding='utf-8') as f:  
            for line in f:  
                if line.strip() and self.is_valid_url(line.strip()):  
                    valid_urls.add(line.strip())  
          
        with open(self.checkpoint_path, 'w', encoding='utf-8') as f:  
            f.write('\n'.join(valid_urls))  
          
        self.downloaded_urls = valid_urls  
        self.logger.info(f"Checkpoint limpo - {len(valid_urls)} URLs válidas")  
    except Exception as e:  
        self.logger.error(f"Falha na limpeza do checkpoint: {str(e)}")  

def is_valid_url(self, url):  
    """Validação básica de URL"""  
    try:  
        result = urlparse(url)  
        return all([result.scheme, result.netloc])  
    except:  
        return False  

def setup_logging(self):  
    """Configura logging detalhado com formatação aprimorada"""  
    log_dir = os.path.join(self.base_dir, "logs")  
    os.makedirs(log_dir, exist_ok=True)  

    self.logger = logging.getLogger('vatican_crawler')  
    self.logger.setLevel(logging.DEBUG)  

    # Formato aprimorado  
    log_format = '%(asctime)s - %(levelname)s - %(message)s [%(filename)s:%(lineno)d]'  
    file_formatter = logging.Formatter(log_format)  
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')  

    # Handler para arquivo com rotação  
    file_handler = RotatingFileHandler(  
        os.path.join(log_dir, 'crawler.log'),  
        maxBytes=10*1024*1024,  # 10MB  
        backupCount=5,  
        encoding='utf-8'  
    )  
    file_handler.setFormatter(file_formatter)  

    # Handler para console  
    console_handler = logging.StreamHandler()  
    console_handler.setLevel(logging.INFO)  
    console_handler.setFormatter(console_formatter)  

    self.logger.addHandler(file_handler)  
    self.logger.addHandler(console_handler)  

def setup_folders(self):  
    """Configura a estrutura de pastas com verificação"""  
    required_folders = [  
        os.path.join(self.base_dir, "documents", "raw"),  
        os.path.join(self.base_dir, "documents", "processed"),  
        os.path.join(self.base_dir, "metadata"),  
        os.path.join(self.base_dir, "logs"),  
        os.path.join(self.base_dir, "checkpoints"),  
        os.path.join(self.base_dir, "versions"),  
        os.path.join(self.base_dir, "feedback"),  
        os.path.join(self.base_dir, "cache"),  
        os.path.join(self.base_dir, "search_index"),  
        os.path.join(self.base_dir, "backups")  
    ]  

    for folder in required_folders:  
        try:  
            os.makedirs(folder, exist_ok=True)  
        except OSError as e:  
            self.logger.error(f"Erro ao criar pasta {folder}: {str(e)}")  
            raise  

    # Pastas por tipo de documento  
    for doc_type in list(self.config.DOC_TYPES.keys()) + ['outros'] + list(self.config.SPECIAL_DOCUMENTS.keys()):  
        raw_path = os.path.join(self.base_dir, "documents", "raw", doc_type)  
        processed_path = os.path.join(self.base_dir, "documents", "processed", doc_type)  
        os.makedirs(raw_path, exist_ok=True)  
        os.makedirs(processed_path, exist_ok=True)  

def setup_summarizer(self):  
    """Configura o modelo de sumarização com verificação de GPU"""  
    try:  
        device = 0 if torch.cuda.is_available() else -1  
        self.logger.info(f"Configurando sumarizador na {'GPU' if device == 0 else 'CPU'}")  
          
        return pipeline(  
            "summarization",  
            model=self.config.SUMMARY_MODEL,  
            device=device,  
            torch_dtype=torch.float16 if device == 0 else None  
        )  
    except Exception as e:  
        self.logger.warning(f"Erro ao carregar sumarizador: {str(e)}")  
        return None  

def load_plugins(self):  
    """Carrega processadores adicionais dinamicamente"""  
    for plugin in self.config.PLUGINS:  
        try:  
            module = importlib.import_module(plugin)  
            self.processors.append(module.Processor())  
        except Exception as e:  
            self.logger.error(f"Falha ao carregar {plugin}: {str(e)}")  

def start_monitoring(self):  
    """Inicia dashboard de monitoramento"""  
    try:  
        start_http_server(getattr(self.config, 'MONITOR_PORT', 8001))
        self.logger.info("Monitoramento iniciado na porta 8001")  
    except Exception as e:  
        self.logger.error(f"Falha ao iniciar monitoramento: {str(e)}")  

def health_check(self):  
    """Verifica integridade do sistema"""  
    checks = {  
        'storage': os.access(self.base_dir, os.W_OK),  
        'network': self.check_network(),  
        'api': self.check_api_connection()  
    }  
    return all(checks.values()), checks  

def check_network(self):  
    """Verifica conectividade de rede"""  
    try:  
        requests.get('https://www.google.com', timeout=5)  
        return True  
    except:  
        return False  

def check_api_connection(self):  
    """Verifica conexão com API do Vaticano"""  
    try:  
        response = requests.get(self.config.BASE_URL, timeout=10)  
        return response.status_code == 200  
    except:  
        return False  

@lru_cache(maxsize=1000)  
def get_document_info_cached(self, url):  
    """Versão cacheada do processamento de metadados"""  
    return self.get_document_info(url)  

def generate_document_id(self, url: str) -> str:  
    """Cria um ID único consistente para cada documento"""  
    return hashlib.sha256(url.encode()).hexdigest()  

def is_relevant_url(self, url: str) -> bool:  
    """Verifica se a URL contém conteúdo documental relevante com cache"""  
    relevant_keywords = [  
        'encyclical', 'enciclica', 'exhortation', 'exortacao',  
        'homily', 'homilia', 'message', 'mensagem', 'speech',  
        'discourse', 'discurso', 'angelus', 'audience', 'audiencia',  
        'document', 'documents', 'cdc', 'ccc', 'compendio', 'atti', 'concilio'  
    ]  
    return any(kw in url.lower() for kw in relevant_keywords)  

def get_document_info(self, url: str) -> Dict:  
    """Extrai metadados estruturados da URL do documento"""  
    parsed = urlparse(url)  
    path_parts = [p for p in parsed.path.split('/') if p]  

    # Determina o Papa  
    pope = "outros"  
    if len(path_parts) > 1:  
        pope_mapping = {  
            'francesco': 'Papa Francisco',  
            'benedict-xvi': 'Papa Bento XVI',  
            'john-paul-ii': 'Papa João Paulo II'  
        }  
        pope = pope_mapping.get(path_parts[1].lower(), "outros")  

    # Determina tipo de documento  
    doc_type = "outros"  
    for dtype, special_url in self.config.SPECIAL_DOCUMENTS.items():  
        if special_url in url:  
            doc_type = dtype  
            break  

    if doc_type == "outros":  
        for dtype, keywords in self.config.DOC_TYPES.items():  
            if any(kw in parsed.path.lower() for kw in keywords):  
                doc_type = dtype  
                break  

    # Determina idioma  
    lang = "outros"  
    for l in self.config.LANGUAGES:  
        if f"/{l}/" in parsed.path.lower():  
            lang = l  
            break  

    # Data de publicação estimada  
    pub_date = None  
    date_pattern = r'/(\d{4})/(\d{2})/(\d{2})/'  
    match = re.search(date_pattern, url.lower())  
    if match:  
        try:  
            pub_date = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"  
        except:  
            pass  

    doc_info = {  
        "id": self.generate_document_id(url),  
        "url": url,  
        "pope": pope,  
        "type": doc_type,  
        "lang": lang,  
        "filename": os.path.basename(parsed.path),  
        "full_path": parsed.path,  
        "pub_date": pub_date,  
        "timestamp": datetime.utcnow().isoformat() + "Z",  
        "batch_id": self.current_batch_id  
    }  

    # Valida o schema do documento  
    self.validate_document(doc_info)  

    return doc_info  

def validate_document(self, doc):  
    """Valida estrutura do documento"""  
    try:  
        validate(instance=doc, schema=self.config.DOCUMENT_SCHEMA)  
    except Exception as e:  
        self.logger.error(f"Documento inválido: {str(e)}")  
        raise ValueError(f"Documento não corresponde ao schema: {str(e)}")  

def get_last_collection_date(self) -> Optional[str]:  
    """Obtém a data da última coleta com tratamento de erro"""  
    metadata_path = os.path.join(self.base_dir, "metadata", "documents.json")  
    if os.path.exists(metadata_path):  
        try:  
            with open(metadata_path, 'r', encoding='utf-8') as f:  
                metadata = json.load(f)  
                if metadata:  
                    return max(doc['timestamp'] for doc in metadata)  
        except Exception as e:  
            self.logger.error(f"Erro ao ler metadata: {str(e)}")  
    return None  

def is_document_modified(self, url: str, last_modified: str) -> bool:  
    """Verifica se o documento foi modificado usando HEAD"""  
    try:  
        response = self.session.head(url, timeout=self.config.TIMEOUT)  
        doc_last_modified = response.headers.get('Last-Modified')  
        if not doc_last_modified:  
            return False  
              
        doc_date = datetime.strptime(doc_last_modified, '%a, %d %b %Y %H:%M:%S %Z')  
        last_date = datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%S.%fZ')  
        return doc_date > last_date  
    except Exception as e:  
        self.logger.warning(f"Não foi possível verificar modificação em {url}: {str(e)}")  
        return False  

    def download_document(self, url: str) -> Optional[Dict]:  
        """Baixa o documento com tratamento robusto de erros"""  
        self.last_activity_time = time.time()  

    if url in self.failed_urls:
    self.logger.info(f"URL na blacklist, ignorando: {url}")
    return None
      
    # Verifica cache primeiro  
    cached = self.document_cache.get(url)  
    if cached:  
        return cached  
          
    if self.file_count >= self.config.MAX_FILES:  
        self.logger.warning(f"Limite de {self.config.MAX_FILES} arquivos atingido")  
        return None  

    if url in self.downloaded_urls:  
        self.logger.debug(f"URL já processada: {url}")  
        return None  

    if any(blocked in url for blocked in self.config.BLOCKED_PATHS):  
        self.logger.info(f"URL bloqueada: {url}")  
        return None  

    if not self.is_relevant_url(url):  
        self.logger.debug(f"URL não relevante ignorada: {url}")  
        return None  

    doc_info = self.get_document_info(url)  
    raw_path = os.path.join(  
        self.base_dir, "documents", "raw",   
        doc_info["type"],   
        f"{doc_info['id']}_{doc_info['filename']}"  
    )  

    if os.path.exists(raw_path):
    self.logger.info(f"Arquivo já existe no disco: {raw_path}")
    return None

    # Verifica se o arquivo existe e foi modificado  
    if os.path.exists(raw_path):  
        last_modified = self.get_last_collection_date()  
        if last_modified and not self.is_document_modified(url, last_modified):  
            self.logger.debug(f"Documento não modificado: {doc_info['filename']}")  
            return None  
        self.logger.info(f"Documento modificado, baixando novamente: {doc_info['filename']}")  

    try:  
        # Usa timeout separado para HEAD  
        head_timeout = min(10, self.config.TIMEOUT)  
        response = self.session.head(url, timeout=head_timeout)  
          
        # Verificação rápida de disponibilidade  
        if response.status_code >= 400:  
            self.logger.debug(f"URL não disponível: {url} (HTTP {response.status_code})")  
            return None  
              
        # Operação principal com timeout completo  
        response = self.session.get(url)  
        response.raise_for_status()  

        # Determina extensão do arquivo  
        content_type = response.headers.get('Content-Type', '')  
        if 'pdf' in content_type:  
            ext = '.pdf'  
        elif 'html' in content_type or url.endswith(('.html', '.htm')):  
            ext = '.html'  
        else:  
            self.logger.warning(f"Tipo de conteúdo não suportado: {content_type}")  
            return None  

        # Garante extensão correta  
        if not raw_path.endswith(ext):  
            raw_path = f"{os.path.splitext(raw_path)[0]}{ext}"  

        # Salva o arquivo  
        with open(raw_path, 'wb') as f:  
            f.write(response.content)  

        self.logger.info(f"Baixado: {doc_info['filename']} (total: {self.file_count + 1})")  
        doc_info['local_path'] = raw_path  
        doc_info['content_type'] = content_type  
        doc_info['size_kb'] = len(response.content) / 1024  
          
        self.file_count += 1  
        self.save_downloaded_url(url)  
        self.request_counter.inc()  
          
        # Atualiza cache  
        self.document_cache.set(url, doc_info)  
          
        # Atualiza métricas  
        self.monitoring.update_metrics(doc_info)  
          
        return doc_info  

    except requests.exceptions.Timeout:  
        self.logger.warning(f"Timeout ao acessar {url}")  
        return None  
    except requests.exceptions.RequestException as e:  
        self.error_count += 1  
        self.logger.error(f"Erro HTTP ao baixar {url}: {str(e)}")  
        # Contador de falhas por URL
    if not hasattr(self, 'url_fail_count'):
        self.url_fail_count = {}
    self.url_fail_count[url] = self.url_fail_count.get(url, 0) + 1
    if self.url_fail_count[url] >= 3:  # Número de tentativas antes de bloquear
    self.failed_urls.add(url)
    with open(self.failed_urls_path, "a", encoding="utf-8") as f:
        f.write(f"{url}\n")
    except Exception as e:  
        self.error_count += 1  
        self.logger.error(f"Erro inesperado ao baixar {url}: {str(e)}")  
      
    return None  

def clean_text(self, text: str) -> str:  
    """Limpeza avançada do conteúdo textual"""  
    if not text:  
        return ""  

    # Remove cabeçalhos/footers específicos do Vaticano  
    text = re.sub(r'© Copyright.*?Vatican\.va', '', text, flags=re.DOTALL | re.IGNORECASE)  
    text = re.sub(r'.*?', '', text)  # Remove notas entre colchetes  
      
    # Remove números de página e formatação  
    text = re.sub(r'\n\s*\d+\s*\n', '\n', text)  
    text = re.sub(r'\n{3,}', '\n\n', text)  # Normaliza quebras de linha  
      
    # Remove espaços extras  
    text = re.sub(r'[ \t]+', ' ', text).strip()  
      
    return text  

def extract_bible_references(self, text: str) -> List[Dict]:  
    """Identifica referências bíblicas com detalhes"""  
    pattern = r'([1-3]?\s?\w+)\s?(\d{1,3}):?(\d{1,3})?(?:-(\d{1,3}))?'  
    references = []  
      
    for ref in re.findall(pattern, text):  
        book_abbr = ref[0].lower()  
        book_name = self.config.LIVROS_BIBLICOS.get(book_abbr, book_abbr.title())  
        chapter = ref[1]  
        verse_start = ref[2] if ref[2] else None  
        verse_end = ref[3] if ref[3] else verse_start  
          
        ref_str = f"{book_name} {chapter}"  
        if verse_start:  
            ref_str += f":{verse_start}" + (f"-{verse_end}" if verse_end and verse_end != verse_start else "")  
          
        references.append({  
            'book': book_name,  
            'chapter': chapter,  
            'verse_start': verse_start,  
            'verse_end': verse_end,  
            'reference': ref_str  
        })  
          
    return references  

def tag_document(self, text: str) -> List[str]:  
    """Identifica tags doutrinais com contagem de ocorrências"""  
    tags = []  
    text_lower = text.lower()  
      
    for tag, keywords in self.config.DOCTRINE_TAGS.items():  
        keyword_counts = {}  
        for kw in keywords:  
            count = len(re.findall(r'\b' + re.escape(kw) + r'\b', text_lower))  
            if count > 0:  
                keyword_counts[kw] = count  
          
        if keyword_counts:  
            tags.append({  
                'tag': tag,  
                'keywords': keyword_counts,  
                'total': sum(keyword_counts.values())  
            })  
      
    return tags  

def identify_sacred_entities(self, text: str) -> Dict[str, int]:  
    """Conta ocorrências de entidades sagradas com aliases"""  
    entities = {}  
    text_lower = text.lower()  
      
    for entity, aliases in self.config.SACRED_ENTITIES.items():  
        total = sum(  
            len(re.findall(r'\b' + re.escape(alias) + r'\b', text_lower))  
            for alias in aliases  
        )  
        if total > 0:  
            entities[entity] = {  
                'name': entity.title(),  
                'count': total,  
                'aliases': {a: len(re.findall(r'\b' + re.escape(a) + r'\b', text_lower)) for a in aliases}  
            }  
      
    return entities  

def extract_document_structure(self, text: str, doc_type: str) -> List[Dict]:  
    """Extrai estrutura hierárquica do documento"""  
    structure = []  
    lines = text.split('\n')  
      
    # Extrai seções principais  
    if doc_type in ['enciclicas', 'exortacoes']:  
        sections = re.findall(r'\n([IVXLCDM]+)\.?\s+(.+?)\n', text)  
        for num, title in sections:  
            structure.append({  
                'type': 'section',  
                'level': 1,  
                'number': num,  
                'title': title.strip(),  
                'content': None  
            })  
      
    # Extrai títulos  
    headings = re.findall(r'\n([A-Z][A-Z0-9À-Ú\s,\-]+)\n', text)  
    for heading in headings:  
        if len(heading.split()) < 10:  # Filtra linhas longas que não são títulos  
            structure.append({  
                'type': 'heading',  
                'level': 2,  
                'title': heading.strip(),  
                'content': None  
            })  
      
    # Extrai parágrafos numerados  
    paragraphs = re.findall(r'\n(\d+)\.\s+(.+?)(?=\n\d+\.|\n[A-Z]|$)', text, re.DOTALL)  
    for num, content in paragraphs:  
        structure.append({  
            'type': 'paragraph',  
            'number': num,  
            'content': content.strip()  
        })  
      
    return structure  

def analyze_sentiment(self, text):  
    """Análise de tom do documento"""  
    try:  
        from transformers import pipeline  
        analyzer = pipeline("sentiment-analysis")  
        return analyzer(text[:1000])  # Limita tamanho para análise  
    except Exception as e:  
        self.logger.error(f"Erro na análise de sentimento: {str(e)}")  
        return None  

def detect_language(self, text):  
    """Identifica idioma quando não especificado"""  
def heuristic_language_from_url(self, url):
    for l in self.config.LANGUAGES:
        if f"/{l}/" in url.lower():
            return l
    return 'unknown'

def detect_language(self, text, url=None):
    try:
        lang = detect(text[:500])
        if lang not in self.config.LANGUAGES and url:
            return self.heuristic_language_from_url(url)
        return lang
    except:
        if url:
            return self.heuristic_language_from_url(url)
        return 'unknown'

def summarize_text(self, text: str) -> Optional[str]:  
    """Sumariza o texto usando modelo com fallback"""  
    if not text or len(text.split()) < 30:  
        return None  
          
    # Tenta sumarização com modelo  
    if self.summarizer:  
        try:  
            # Divide texto em chunks para evitar overflow  
            chunks = [text[i:i+2000] for i in range(0, len(text), 2000)]  
            summaries = []  
              
            for chunk in chunks:  
                result = self.summarizer(  
                    chunk,  
                    max_length=self.config.SUMMARY_MAX_LENGTH,  
                    min_length=self.config.SUMMARY_MIN_LENGTH,  
                    do_sample=False  
                )  
                summaries.append(result[0]['summary_text'])  
              
            return ' '.join(summaries)  
        except Exception as e:  
            self.logger.error(f"Erro no sumarizador: {str(e)}")  
      
    # Fallback: pega as primeiras frases  
    sentences = re.split(r'(?<=[.!?])\s+', text)  
    return ' '.join(sentences[:3]) if len(sentences) > 3 else text[:200] + '...'  

def convert_to_text(self, doc_info: Dict) -> Optional[str]:  
    """Converte o documento para texto formatado"""  
    if not doc_info or 'local_path' not in doc_info:  
        return None  

    raw_path = doc_info['local_path']  
    txt_path = os.path.join(  
        self.base_dir, "documents", "processed", doc_info["type"],  
        f"{doc_info['id']}.txt"  
    )  

    # Se já existe e não foi modificado, retorna o caminho  
    if os.path.exists(txt_path):  
        if not self.is_document_modified(doc_info['url'], doc_info['timestamp']):  
            return txt_path  

    try:  
        text = ""  
       if raw_path.endswith('.pdf'):
    try:
        with pdfplumber.open(raw_path) as pdf:
            text = "\n".join(
                page.extract_text() or ""   
                for page in pdf.pages   
                if page.extract_text() is not None  
            )
    except Exception as e:
        self.logger.error(f"Erro PDF corrompido/protegido: {raw_path} - {e}")
        with open(os.path.join(self.base_dir, "logs", "erro_pdf.txt"), "a", encoding="utf-8") as f:
            f.write(f"{raw_path}\n")
        return None
    
        elif raw_path.endswith(('.html', '.htm')):  
            with open(raw_path, 'r', encoding='utf-8', errors='ignore') as f:  
                html_content = f.read()  
            text = html2text.html2text(html_content)  
        else:  
            return None  

        # Processamento avançado do texto  
        cleaned_text = self.clean_text(text)  
          
        # Validação teológica  
        validation_warnings = self.validator.validate_content(cleaned_text)  
        if validation_warnings:  
            self.logger.warning(f"Alertas teológicos em {doc_info['filename']}: {validation_warnings}")  
          
        # Enriquecimento de metadados  
        doc_info.update({  
            'bible_refs': self.extract_bible_references(cleaned_text),  
            'doctrine_tags': self.tag_document(cleaned_text),  
            'sacred_entities': self.identify_sacred_entities(cleaned_text),  
            'structure': self.extract_document_structure(cleaned_text, doc_info['type']),  
            'summary': self.summarize_text(cleaned_text),  
            'word_count': len(cleaned_text.split()),  
            'char_count': len(cleaned_text),  
            'processed_at': datetime.utcnow().isoformat() + "Z",  
            'sentiment': self.analyze_sentiment(cleaned_text),  
            'detected_lang': self.detect_language(cleaned_text),  
            'validation_warnings': validation_warnings  
        })  

        # Processa plugins  
        doc_info = self.plugin_system.process_document(doc_info)  

        # Salva texto processado  
        with open(txt_path, 'w', encoding='utf-8') as f:  
            f.write(cleaned_text)  

        # Indexa o documento  
        self.indexer.index_document(doc_info)  

        return txt_path  
    except Exception as e:  
        self.logger.error(f"Erro na conversão de {raw_path}: {str(e)}")  
        return None  

def version_document(self, doc_id):  
    """Gerencia histórico de versões"""  
    vdir = os.path.join(self.base_dir, "versions", doc_id)  
    os.makedirs(vdir, exist_ok=True)  
    version = len(os.listdir(vdir)) + 1  
    return version  

def compress_document(self, doc_id):  
    """Compacta documentos antigos"""  
    path = os.path.join(self.base_dir, "documents", "processed", doc_id + ".txt")  
    if os.path.exists(path):  
        with zipfile.ZipFile(f"{path}.zip", 'w') as zf:  
            zf.write(path, compress_type=zipfile.ZIP_DEFLATED)  
        os.remove(path)  
        return True  
    return False  

def export_document(self, doc, formats=('json', 'xml', 'txt')):  
    """Exporta documento em múltiplos formatos"""  
    exporters = {  
        'json': lambda d: json.dumps(d, ensure_ascii=False),  
        'xml': self._export_xml,  
        'txt': lambda d: d.get('content', ''),  
        'liturgical': self.exporter.export_to_liturgical,  
        'catechism': self.exporter.export_to_catechism_format  
    }  
    return {fmt: exporters[fmt](doc) for fmt in formats}  

def _export_xml(self, doc):  
    """Exporta para XML"""  
    root = Element('document')  
    for key, val in doc.items():  
        child = Element(key)  
        child.text = str(val)  
        root.append(child)  
    return tostring(root, encoding='unicode')  

def crawl_page(self, url: str, visited: Set[str], depth: int = 0) -> List[Dict]:  
    """Crawler recursivo com controle de profundidade"""  
    if depth > self.config.MAX_DEPTH or url in visited:  
        return []  

    visited.add(url)  
    self.logger.debug(f"Processando: {url} (profundidade {depth})")  
    documents = []  

    try:  
        response = self.session.get(url, timeout=self.config.TIMEOUT)
    if not response.encoding or response.encoding.lower() == 'iso-8859-1':
        response.encoding = response.apparent_encoding
    soup = BeautifulSoup(response.text, 'html.parser')

        # Processa a página atual se for um documento  
        if any(url.endswith(ext) for ext in ('.html', '.htm', '.pdf')):  
            doc_info = self.download_document(url)  
            if doc_info:  
                txt_path = self.convert_to_text(doc_info)  
                if txt_path:  
                    doc_info['txt_path'] = os.path.relpath(txt_path, self.base_dir)  
                    documents.append(doc_info)  

        # Limite de profundidade atingido  
        if depth >= self.config.MAX_DEPTH:  
            return documents  

        # Processa links na página  
        for link in soup.find_all('a', href=True):  
            if self.file_count >= self.config.MAX_FILES:  
                self.logger.warning(f"Limite de {self.config.MAX_FILES} arquivos atingido durante crawling")  
                return documents  

            href = link['href'].strip()  
            if not href or href.startswith(('#', 'javascript:', 'mailto:')):  
                continue  

            full_url = urljoin(url, href)  
            parsed = urlparse(full_url)  

            if not re.search(r'\.(html?|pdf)$', full_url, re.IGNORECASE):
            continue

            # Filtra URLs externas e bloqueadas  
            if parsed.netloc != urlparse(self.config.BASE_URL).netloc:  
                continue  
            if full_url in visited or any(blocked in full_url for blocked in self.config.BLOCKED_PATHS):  
                continue  

            # Rate limiting  
            time.sleep(random.uniform(0.5, 2.0))

            # Processa documentos diretamente  
            if any(full_url.endswith(ext) for ext in ('.html', '.htm', '.pdf')):  
                doc_info = self.download_document(full_url)  
                if doc_info:  
                    txt_path = self.convert_to_text(doc_info)  
                    if txt_path:  
                        doc_info['txt_path'] = os.path.relpath(txt_path, self.base_dir)  
                        documents.append(doc_info)  

            # Continua crawling para páginas HTML  
            if full_url.endswith(('.html', '.htm')) and full_url not in visited:  
                documents.extend(self.crawl_page(full_url, visited, depth + 1))  

            visited.add(full_url)  

    except Exception as e:  
        self.logger.error(f"Erro ao processar {url}: {str(e)}")  

    return documents  

def process_batch(self, urls):  
    """Processamento em lote com progresso"""  
    documents = []  
    with ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:  
        futures = {executor.submit(self.download_document, url): url for url in urls}  
        for future in tqdm(as_completed(futures), total=len(urls), desc="Processando URLs"):  
            url = futures[future]  
            try:  
                doc = future.result()  
                if doc:  
                    documents.append(doc)  
            except Exception as e:  
                self.logger.error(f"Erro ao processar {url}: {str(e)}")  
    return documents  

def parallel_crawl(self, start_urls: List[str]) -> List[Dict]:  
    """Executa crawling paralelo com gerenciamento de threads"""  
    all_documents = []  
    visited = set()  
      
    with ThreadPoolExecutor(  
        max_workers=self.config.MAX_WORKERS,  
        thread_name_prefix='crawler'  
    ) as executor:  
        self._executor = executor  # Para acesso na recuperação  
          
        future_to_url = {  
            executor.submit(  
                self.safe_crawl_page,  
                url,  
                visited.copy(),  
                0  
            ): url for url in start_urls  
        }  

        for future in as_completed(future_to_url):  
            try:  
                documents = future.result(timeout=self.config.TIMEOUT * 2)  
                all_documents.extend(documents)  
                  
                # Verifica limite de arquivos após cada thread  
                if self.file_count >= self.config.MAX_FILES:  
                    self.logger.warning(f"Limite de arquivos atingido durante processamento paralelo")  
                    break  
                      
            except concurrent.futures.TimeoutError:  
                self.logger.error("Timeout em thread de crawling")  
            except Exception as e:  
                url = future_to_url[future]  
                self.logger.error(f"Erro ao processar {url}: {str(e)}")  

    return all_documents  

def safe_crawl_page(self, url: str, visited: Set[str], depth: int) -> List[Dict]:  
    """Wrapper seguro para o crawler com timeout total"""  
    try:  
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as single_executor:  
            future = single_executor.submit(  
                self.crawl_page,  
                url,  
                visited,  
                depth  
            )  
            return future.result(timeout=self.config.TIMEOUT * 3)  
    except concurrent.futures.TimeoutError:  
        self.logger.warning(f"Timeout no processamento de {url}")  
        return []  
    except Exception as e:  
        self.logger.error(f"Erro seguro em {url}: {str(e)}")  
        return []  

def save_metadata(self, metadata: List[Dict]):  
    """Salva metadados em múltiplos formatos"""  
    if not metadata:  
        self.logger.warning("Nenhum metadado para salvar")  
        return  

    # JSON completo  
    json_path = os.path.join(self.base_dir, "metadata", "documents.json")  
    try:  
        existing_data = []  
        if os.path.exists(json_path):  
            with open(json_path, 'r', encoding='utf-8') as f:  
                existing_data = json.load(f)  
          
        # Atualiza apenas documentos novos/modificados  
        existing_ids = {doc['id'] for doc in existing_data}  
        updated_data = [doc for doc in existing_data if doc['id'] not in {m['id'] for m in metadata}]  
        updated_data.extend(metadata)  
          
        with open(json_path, 'w', encoding='utf-8') as f:  
            json.dump(updated_data, f, indent=2, ensure_ascii=False)  
    except Exception as e:  
        self.logger.error(f"Erro ao salvar JSON: {str(e)}")  

    # CSV simplificado  
    csv_path = os.path.join(self.base_dir, "metadata", "documents.csv")  
    fieldnames = ["id", "url", "pope", "type", "lang", "filename", "pub_date", "timestamp", "batch_id"]  
      
    try:  
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:  
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)  
            writer.writeheader()  
            for doc in metadata:  
                row = {k: v for k, v in doc.items() if k in fieldnames}  
                writer.writerow(row)  
    except Exception as e:  
        self.logger.error(f"Erro ao salvar CSV: {str(e)}")  

def save_enriched_metadata(self, metadata: List[Dict]):  
    """Salva metadados enriquecidos com análise de conteúdo"""  
    enriched_path = os.path.join(self.base_dir, "metadata", "enriched_metadata.json")  
      
    try:  
        existing_data = []  
        if os.path.exists(enriched_path):  
            with open(enriched_path, 'r', encoding='utf-8') as f:  
                existing_data = json.load(f)  
          
        # Mescla dados existentes com novos  
        existing_ids = {doc['id'] for doc in existing_data}  
        updated_data = [doc for doc in existing_data if doc['id'] not in {m['id'] for m in metadata}]  
          
        for doc in metadata:  
            if 'txt_path' in doc:  
                try:  
                    with open(os.path.join(self.base_dir, doc['txt_path']), 'r', encoding='utf-8') as f:  
                        text = f.read()  
                          
                    enriched_doc = {  
                        **doc,  
                        'word_count': len(text.split()),  
                        'bible_refs_count': len(doc.get('bible_refs', [])),  
                        'main_topics': [t['tag'] for t in doc.get('doctrine_tags', [])],  
                        'entities_mentioned': list(doc.get('sacred_entities', {}).keys()),  
                        'structure_types': list({s['type'] for s in doc.get('structure', [])}),  
                        'analysis_complete': True  
                    }  
                    updated_data.append(enriched_doc)  
                except Exception as e:  
                    self.logger.error(f"Erro ao processar {doc['id']}: {str(e)}")  
                    updated_data.append({**doc, 'analysis_complete': False})  
          
        with open(enriched_path, 'w', encoding='utf-8') as f:  
            json.dump(updated_data, f, indent=2, ensure_ascii=False)  
    except Exception as e:  
        self.logger.error(f"Erro ao salvar metadados enriquecidos: {str(e)}")  

def save_batch_metadata(self):  
    """Salva metadados sobre o lote atual de coleta"""  
    batch_metadata = {  
        "batch_id": self.current_batch_id,  
        "start_time": self.start_time,  
        "end_time": time.time(),  
        "duration_seconds": time.time() - self.start_time,  
        "documents_collected": self.file_count,  
        "errors": self.error_count,  
        "max_files": self.config.MAX_FILES,  
        "config": {  
            "base_url": self.config.BASE_URL,  
            "max_depth": self.config.MAX_DEPTH  
        }  
    }  
      
    batch_path = os.path.join(self.base_dir, "metadata", "batches", f"{self.current_batch_id}.json")  
    os.makedirs(os.path.dirname(batch_path), exist_ok=True)  
      
    try:  
        with open(batch_path, 'w', encoding='utf-8') as f:  
            json.dump(batch_metadata, f, indent=2)  
    except Exception as e:  
        self.logger.error(f"Erro ao salvar metadados do lote: {str(e)}")  

def collect_feedback(self, doc_id, rating, comments=None):  
    """Coleta avaliações de usuários"""  
    feedback_path = os.path.join(self.base_dir, "feedback", "feedback.csv")  
    os.makedirs(os.path.dirname(feedback_path), exist_ok=True)  
    with open(feedback_path, 'a', newline='', encoding='utf-8') as f:  
        writer = csv.writer(f)  
        writer.writerow([doc_id, rating, comments, datetime.now().isoformat()])  

def git_push(self) -> bool:  
    """Envia os documentos para o GitHub com tratamento robusto"""  
    if self.file_count < self.config.MIN_FILES_TO_PUSH:  
        self.logger.info(f"Não enviando para GitHub - abaixo do mínimo de {self.config.MIN_FILES_TO_PUSH} arquivos")  
        return False  

    try:  
        os.chdir(self.base_dir)  
        self.logger.info("Preparando para enviar ao GitHub...")  

        # Configuração inicial do repositório  
        if not os.path.exists(os.path.join(self.base_dir, ".git")):  
            subprocess.run(["git", "init"], check=True, capture_output=True, text=True)  
            subprocess.run(["git", "config", "user.name", self.config.GIT_USER], check=True)  
            subprocess.run(["git", "config", "user.email", self.config.GIT_EMAIL], check=True)  
            subprocess.run(["git", "branch", "-M", "main"], check=True)  

        # Verifica remotes  
        remotes = subprocess.run(["git", "remote", "-v"], capture_output=True, text=True).stdout  
        if "origin" not in remotes:  
            subprocess.run(["git", "remote", "add", "origin", self.repo_url], check=True)  
        else:  
            subprocess.run(["git", "remote", "set-url", "origin", self.repo_url], check=True)  

        # Atualiza antes de enviar  
        subprocess.run(["git", "pull", "origin", "main"], check=True)  

        # Commit e push  
        subprocess.run(["git", "add", "."], check=True)  
          
        commit_msg = (  
            f"Atualização automática - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"  
            f"Documentos: {self.file_count}\n"  
            f"Batch ID: {self.current_batch_id}"  
        )  
          
        if self.file_count >= self.config.MAX_FILES:  
            commit_msg += "\n[COLETA PARCIAL - LIMITE ATINGIDO]"  
          
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)  
        push_result = subprocess.run(["git", "push", "-u", "origin", "main"], capture_output=True, text=True)  
          
        if push_result.returncode != 0:  
            self.logger.error(f"Erro no push: {push_result.stderr}")  
            return False  
              
        self.logger.info("Push para GitHub realizado com sucesso!")  
        return True  

    except subprocess.CalledProcessError as e:  
        self.logger.error(f"Erro no Git (code {e.returncode}): {e.stderr if e.stderr else str(e)}")  
        return False  
    except Exception as e:  
        self.logger.error(f"Erro inesperado no Git: {str(e)}")  
        return False  

def run_ci_pipeline(self):  
    """Pipeline de testes automatizados"""  
    tests = [  
        self.test_metadata_integrity,  
        self.test_document_parsing,  
        self.test_network_connectivity  
    ]  
    return all(t() for t in tests)  

def test_metadata_integrity(self):  
    """Testa integridade dos metadados"""  
    try:  
        metadata_path = os.path.join(self.base_dir, "metadata", "documents.json")  
        if os.path.exists(metadata_path):  
            with open(metadata_path, 'r', encoding='utf-8') as f:  
                metadata = json.load(f)  
                for doc in metadata:  
                    self.validate_document(doc)  
        return True  
    except Exception as e:  
        self.logger.error(f"Teste de metadados falhou: {str(e)}")  
        return False  

def test_document_parsing(self):  
    """Testa análise de documentos"""  
    try:  
        test_doc = os.path.join(self.base_dir, "documents", "raw", "test.txt")  
        with open(test_doc, 'w') as f:  
            f.write("Test document")  
        result = self.convert_to_text({'local_path': test_doc, 'type': 'test'})  
        os.remove(test_doc)  
        return result is not None  
    except Exception as e:  
        self.logger.error(f"Teste de análise falhou: {str(e)}")  
        return False  

def test_network_connectivity(self):  
    """Testa conectividade de rede"""  
    return self.check_network()  

def run_crawler(self):  
    """Executa o fluxo completo de coleta com tratamento de erros"""  
    self.logger.info(f"Iniciando coleta - Batch ID: {self.current_batch_id}")  
    self.start_time = time.time()  
      
    try:  
        # URLs iniciais  
        start_urls = [  
            f"{self.config.BASE_URL}/content/francesco/pt/index.html",  
            *self.config.SPECIAL_DOCUMENTS.values()  
        ]  
          
        # Coleta paralela  
        all_documents = self.parallel_crawl(start_urls)  
          
        # Processamento pós-coleta  
        if all_documents:  
            self.logger.info(f"Coleta concluída - {len(all_documents)} documentos processados")  
              
            # Salva metadados  
            self.save_metadata(all_documents)  
            self.save_enriched_metadata(all_documents)  
            self.save_batch_metadata()  
              
            # Envia para GitHub (mesmo se for coleta parcial)  
            if all_documents:  
                self.git_push()  
              
            # Executa limpeza automática  
            self.cleaner.run_cleanup()  
              
            # Log de resumo  
            elapsed = time.time() - self.start_time  
            docs_per_sec = len(all_documents) / elapsed if elapsed > 0 else 0  
            self.logger.info(  
                f"Resumo final:\n"  
                f"- Documentos: {len(all_documents)}/{self.config.MAX_FILES}\n"  
                f"- Erros: {self.error_count}\n"  
                f"- Tempo: {elapsed:.2f}s ({docs_per_sec:.2f} docs/s)\n"  
                f"- Enviado: {'Sim' if len(all_documents) >= self.config.MIN_FILES_TO_PUSH else 'Não'}"  
            )  
        else:  
            self.logger.warning("Nenhum documento novo foi coletado")  
              
    except KeyboardInterrupt:  
        self.logger.info("Coleta interrompida pelo usuário. Salvando progresso...")  
        if 'all_documents' in locals() and all_documents:  
            self.save_metadata(all_documents)  
            self.git_push()  # Tenta enviar o que foi coletado  
    except Exception as e:  
        self.logger.error(f"Erro fatal na coleta: {str(e)}", exc_info=True)  
        self.notifier.send_notification("Erro fatal na coleta", str(e), "error")  
        raise  
    finally:  
        # Garante que os metadados são salvos mesmo em caso de erro  
        if 'all_documents' in locals() and all_documents:  
            self.save_metadata(all_documents)  
            self.save_batch_metadata()  
              
        # Cria backup após coleta  
        backup_file = self.backup_system.create_backup()  
        self.logger.info(f"Backup criado: {backup_file}")  

def analyze_trends(self, timeframe='year'):  
    """Analisa tendências nos documentos coletados"""  
    metadata_path = os.path.join(self.base_dir, "metadata", "enriched_metadata.json")  
    if not os.path.exists(metadata_path):  
        return {}  
          
    try:  
        with open(metadata_path, 'r') as f:  
            docs = json.load(f)  
              
        return self.trend_analyzer.analyze_trends(docs, timeframe)  
    except Exception as e:  
        self.logger.error(f"Erro na análise de tendências: {str(e)}")  
        return {}  

def export_document(self, doc_id, format='liturgical'):  
    """Exporta documento em formatos eclesiásticos"""  
    metadata_path = os.path.join(self.base_dir, "metadata", "enriched_metadata.json")  
    if not os.path.exists(metadata_path):  
        return None  
          
    try:  
        with open(metadata_path, 'r') as f:  
            docs = json.load(f)  
              
        doc = next((d for d in docs if d['id'] == doc_id), None)  
        if not doc:  
            return None  
              
        if format == 'liturgical':  
            return self.exporter.export_to_liturgical(doc)  
        elif format == 'catechism':  
            return self.exporter.export_to_catechism_format(doc)  
        else:  
            raise ValueError(f"Formato não suportado: {format}")  
    except Exception as e:  
        self.logger.error(f"Erro ao exportar documento: {str(e)}")  
        return None

==================== FILA DE PRIORIDADE ====================

class PriorityQueue:
def init(self):
self.queue = []

def add_url(self, url, priority=0):  
    heapq.heappush(self.queue, (-priority, url))  
      
def get_next_url(self):  
    if self.queue:  
        return heapq.heappop(self.queue)[1]  
    return None

==================== API OTIMIZADA ====================

class DocumentAPI:
def init(self, crawler: VaticanCrawler):
self.crawler = crawler
self.app = FastAPI(
title="API de Documentos Católicos",
description="API para consulta de documentos do Vaticano",
version="2.0.0",
docs_url="/docs",
redoc_url=None
)
self.setup_routes()
self.documents_cache = None
self.last_cache_update = 0
self.cache_ttl = crawler.config.CACHE_TTL

class DocumentResponse(BaseModel):  
    id: str  
    url: str  
    pope: str  
    type: str  
    lang: str  
    pub_date: Optional[str] = None  
    summary: Optional[str] = None  
    bible_refs: List[Dict] = []  
    doctrine_tags: List[Dict] = []  
    sacred_entities: Dict[str, Dict] = {}  
    word_count: Optional[int] = None  
    batch_id: str  

class SearchQuery(BaseModel):  
    text: str  
    pope: Optional[str] = None  
    doc_type: Optional[str] = None  
    lang: Optional[str] = None  
    min_words: Optional[int] = None  
    max_words: Optional[int] = None  
    limit: int = 10  
    offset: int = 0  

def load_documents(self, force_refresh: bool = False) -> List[Dict]:  
    """Carrega documentos com cache"""  
    current_time = time.time()  
      
    if not force_refresh and self.documents_cache and (current_time - self.last_cache_update) < self.cache_ttl:  
        return self.documents_cache  
          
    metadata_path = os.path.join(self.crawler.base_dir, "metadata", "enriched_metadata.json")  
    if not os.path.exists(metadata_path):  
        raise HTTPException(status_code=404, detail="Dados não disponíveis")  
          
    try:  
        with open(metadata_path, 'r', encoding='utf-8') as f:  
            self.documents_cache = json.load(f)  
            self.last_cache_update = current_time  
            return self.documents_cache  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=f"Erro ao carregar dados: {str(e)}")  

def setup_routes(self):  
    @self.app.get("/documents/{doc_id}", response_model=self.DocumentResponse)  
    async def get_document(doc_id: str):  
        """Obtém um documento pelo ID"""  
        documents = self.load_documents()  
        for doc in documents:  
            if doc.get('id') == doc_id:  
                return doc  
        raise HTTPException(status_code=404, detail="Documento não encontrado")  

    @self.app.post("/search", response_model=List[self.DocumentResponse])  
    async def search_documents(query: self.SearchQuery):  
        """Busca avançada em documentos"""  
        documents = self.load_documents()  
        results = []  
          
        for doc in documents[query.offset:query.offset + query.limit]:  
            # Filtros básicos  
            if query.pope and doc.get('pope', '').lower() != query.pope.lower():  
                continue  
            if query.doc_type and doc.get('type', '').lower() != query.doc_type.lower():  
                continue  
            if query.lang and doc.get('lang', '').lower() != query.lang.lower():  
                continue  
            if query.min_words and doc.get('word_count', 0) < query.min_words:  
                continue  
            if query.max_words and doc.get('word_count', 0) > query.max_words:  
                continue  
              
            # Busca de texto  
            if query.text:  
                txt_path = os.path.join(self.crawler.base_dir, doc.get('txt_path', ''))  
                if os.path.exists(txt_path):  
                    try:  
                        with open(txt_path, 'r', encoding='utf-8') as f:  
                            text = f.read()  
                            if query.text.lower() in text.lower():  
                                results.append(doc)  
                    except:  
                        continue  
            else:  
                results.append(doc)  
              
            if len(results) >= query.limit:  
                break  
          
        return results  

    @self.app.get("/stats")  
    async def get_stats():  
        """Estatísticas dos documentos"""  
        documents = self.load_documents()  
          
        if not documents:  
            return {}  
          
        stats = {  
            "total": len(documents),  
            "by_pope": Counter(doc.get('pope', 'Desconhecido') for doc in documents),  
            "by_type": Counter(doc.get('type', 'outros') for doc in documents),  
            "by_lang": Counter(doc.get('lang', 'outros') for doc in documents),  
            "word_counts": {  
                "min": min(doc.get('word_count', 0) for doc in documents),  
                "max": max(doc.get('word_count', 0) for doc in documents),  
                "avg": sum(doc.get('word_count', 0) for doc in documents) / len(documents)  
            },  
            "last_update": max(doc.get('timestamp', '') for doc in documents)  
        }  
          
        return stats  

    @self.app.post("/feedback/{doc_id}")  
    async def submit_feedback(doc_id: str, rating: int, comments: Optional[str] = None):  
        """Envia feedback sobre um documento"""  
        try:  
            self.crawler.collect_feedback(doc_id, rating, comments)  
            return {"status": "success", "message": "Feedback registrado"}  
        except Exception as e:  
            raise HTTPException(status_code=500, detail=f"Erro ao registrar feedback: {str(e)}")  

    @self.app.get("/trends")  
    async def get_trends(timeframe: str = 'year'):  
        """Obtém tendências de tópicos ao longo do tempo"""  
        try:  
            trends = self.crawler.analyze_trends(timeframe)  
            return trends  
        except Exception as e:  
            raise HTTPException(status_code=500, detail=f"Erro ao analisar tendências: {str(e)}")  

    @self.app.get("/export/{doc_id}")  
    async def export_document(doc_id: str, format: str = 'liturgical'):  
        """Exporta documento em formato eclesiástico"""  
        try:  
            content = self.crawler.export_document(doc_id, format)  
            if not content:  
                raise HTTPException(status_code=404, detail="Documento não encontrado")  
            return PlainTextResponse(content)  
        except Exception as e:  
            raise HTTPException(status_code=500, detail=f"Erro ao exportar documento: {str(e)}")  

def run(self, host: str = "0.0.0.0", port: int = 8000):  
    """Inicia o servidor API"""  
    uvicorn.run(self.app, host=host, port=port)

==================== MAIN ====================

if name == "main":
import argparse

parser = argparse.ArgumentParser(description="Vatican Document Crawler and API")  
parser.add_argument('--crawl', action='store_true', help='Executar o crawler')  
parser.add_argument('--api', action='store_true', help='Iniciar a API')  
parser.add_argument('--host', type=str, default="0.0.0.0", help='Host da API')  
parser.add_argument('--port', type=int, default=8000, help='Porta da API')  
parser.add_argument('--max-files', type=int, help='Limite máximo de arquivos para coletar')  
parser.add_argument('--min-files-push', type=int, help='Mínimo de arquivos para enviar ao GitHub')  
parser.add_argument('--from-env', action='store_true', help='Carregar configurações de variáveis de ambiente')  
parser.add_argument('--monitor-port', type=int, help='Porta do Prometheus')
...
if args.monitor_port:
    crawler.config.MONITOR_PORT = args.monitor_port

args = parser.parse_args()  

# Verifica dependências  
try:  
    import torch  
except ImportError:  
    print("Aviso: PyTorch não está instalado. Sumarização estará desativada.")  

# Configura e executa  
config = Config.from_env() if args.from_env else Config()  
crawler = VaticanCrawler()  
  
# Sobrescreve configurações se especificado  
if args.max_files:  
    crawler.config.MAX_FILES = args.max_files  
if args.min_files_push:  
    crawler.config.MIN_FILES_TO_PUSH = args.min_files_push  

if args.crawl:  
    crawler.run_crawler()  
elif args.api:  
    api = DocumentAPI(crawler)  
    api.run(host=args.host, port=args.port)  
else:  
    print("Opções:\n  --crawl  Executar coleta\n  --api    Iniciar API")