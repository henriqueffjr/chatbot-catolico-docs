#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Vatican Document Crawler - Versão Otimizada e Estável"""

# ==================== IMPORTAÇÕES ====================
import spacy
import pytextrank
import structlog
import os
import sys
import time
import json
import uuid
import signal
import logging
import argparse
import tempfile
import datetime
import hashlib
import zipfile
import threading
import subprocess
import importlib
from functools import lru_cache, partial, wraps
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Optional, Tuple, Any
from logging.handlers import RotatingFileHandler

# Bibliotecas de terceiros
import requests
import pdfplumber
import html2text
import psutil
import redis
import numpy as np
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
import uvicorn
from pydantic import BaseModel
from prometheus_client import start_http_server, Counter, Gauge, Histogram, Summary
from langdetect import detect
from sentence_transformers import SentenceTransformer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from ratelimit import limits, sleep_and_retry
from jinja2 import Template
from requests_cache import CachedSession
from sentence_transformers import util

# ==================== CONFIGURAÇÕES ====================
class Config:
    # Configurações básicas
    MONITOR_PORT = int(os.getenv("MONITOR_PORT", 8001))
    BASE_URL = "https://www.vatican.va"
    LANGUAGES = ["pt", "en", "la", "it", "es", "fr"]
    MAX_DEPTH = 3
    TIMEOUT = 30
    MAX_RETRIES = 3
    MAX_FILES = 50000
    MIN_FILES_TO_PUSH = 1
    MAX_WORKERS = 2
    RATE_LIMIT_DELAY = 1.0
    CACHE_TTL = 300
    PLUGINS = ["metadata_enricher", "content_validator", "bible_reference"]
    
    # Limites de processamento
    MAX_PDF_PAGES = 100
    CHUNK_SIZE = 8192
    MAX_DOC_SIZE_MB = 25
    
    # Diretórios
    BASE_DIR = os.path.abspath("vatican_docs")
    CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints")
    RAW_DIR = os.path.join(BASE_DIR, "raw")
    TXT_DIR = os.path.join(BASE_DIR, "txt")
    METADATA_DIR = os.path.join(BASE_DIR, "metadata")
    LOG_DIR = os.path.join(BASE_DIR, "logs")
    BACKUP_DIR = os.path.join(BASE_DIR, "backups")
    PLUGIN_DIR = os.path.join(BASE_DIR, "plugins")
    EMBEDDINGS_DIR = os.path.join(BASE_DIR, "embeddings")
    
    # GitHub
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
    GITHUB_REPO = "henriqueffjr/chatbot-catolico-docs"
    GITHUB_BRANCH = "main"
    
    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB = int(os.getenv("REDIS_DB", 0))
    
    # Modelos de IA
    EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
    SUMMARY_SENTENCES = 3

    @property
    def DOCUMENT_QUEUE_NAME(self):
        return f"{self.GITHUB_REPO.replace('/', '_')}_queue"

# Verificação de segurança
if os.getenv("ENVIRONMENT") == "production" and not Config.GITHUB_TOKEN:
    print("ERRO: TOKEN DO GITHUB NÃO CONFIGURADO EM AMBIENTE DE PRODUÇÃO!")
    sys.exit(1)

# ==================== LOGGING ====================
def configurar_logging():
    """Configura logging estruturado e tradicional"""
    os.makedirs(Config.LOG_DIR, exist_ok=True)
    
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    file_handler = RotatingFileHandler(
        os.path.join(Config.LOG_DIR, 'crawler.log'),
        maxBytes=2_000_000,
        backupCount=2
    )
    file_handler.setFormatter(log_formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    structlog.configure(
        processors=[
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory()
    )
    
    return logger, structlog.get_logger()

logger, slog = configurar_logging()

# ==================== INICIALIZAÇÃO DE MODELOS ====================
try:
    nlp = spacy.load("pt_core_news_lg", exclude=["ner", "parser"])
    nlp.add_pipe("textrank")
    logger.info("Modelo spaCy carregado com sucesso")
except Exception as e:
    logger.error(f"Falha ao carregar modelo spaCy: {e}")
    sys.exit(1)

# ==================== MÉTRICAS PROMETHEUS ====================
FILES_PROCESSED = Counter('files_processed_total', 'Total de arquivos processados')
CRAWL_ERRORS = Counter('crawl_errors_total', 'Total de erros no crawler')
BATCH_DURATION = Histogram('batch_duration_seconds', 'Duração de cada batch de coleta')
CRAWLER_STATE = Gauge('crawler_state', 'Estado atual do crawler (0=ocioso, 1=ativo)')
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
DOC_SIZE = Gauge('document_size_bytes', 'Size of processed documents')
QUEUE_SIZE = Gauge('document_queue_size', 'Number of documents in processing queue')
BIBLE_REFERENCES = Counter('bible_references_found', 'Total de referências bíblicas encontradas')
SEMANTIC_SEARCHES = Counter('semantic_searches_performed', 'Total de buscas semânticas realizadas')

# Locks para operações thread-safe
file_lock = threading.Lock()
db_lock = threading.Lock()

# ==================== CLASSES AUXILIARES ====================
class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException()

class PluginManager:
    def __init__(self):
        self.plugins = []
        
    def carregar_plugins(self):
        sys.path.append(Config.PLUGIN_DIR)
        for plugin in Config.PLUGINS:
            try:
                module = importlib.import_module(f"plugins.{plugin}")
                self.plugins.append(module.Plugin())
                logger.info(f"Plugin {plugin} carregado com sucesso")
            except Exception as e:
                logger.error(f"Falha ao carregar plugin {plugin}: {e}")

    def executar_hook(self, hook_name, *args, **kwargs):
        for plugin in self.plugins:
            if hasattr(plugin, hook_name):
                try:
                    getattr(plugin, hook_name)(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Erro no plugin {plugin.__class__.__name__}.{hook_name}: {e}")

class DocumentQueue:
    def __init__(self):
        self.conn = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=Config.REDIS_DB,
            decode_responses=True
        )
    
    def adicionar_documento(self, url: str, prioridade: int = 0) -> bool:
        try:
            return self.conn.zadd(Config.DOCUMENT_QUEUE_NAME, {url: prioridade}) == 1
        except Exception as e:
            logger.error(f"Erro ao adicionar documento na fila: {e}")
            return False
    
    def obter_proximo_documento(self) -> Optional[str]:
        try:
            result = self.conn.zpopmax(Config.DOCUMENT_QUEUE_NAME)
            return result[0][0] if result else None
        except Exception as e:
            logger.error(f"Erro ao obter próximo documento: {e}")
            return None
    
    def tamanho_fila(self) -> int:
        try:
            return self.conn.zcard(Config.DOCUMENT_QUEUE_NAME)
        except Exception as e:
            logger.error(f"Erro ao verificar tamanho da fila: {e}")
            return 0

class RateLimiter:
    def __init__(self, calls=10, period=60):
        self.calls = calls
        self.period = period
    
    @sleep_and_retry
    @limits(calls=10, period=60)
    def fazer_requisicao(self, url: str) -> Optional[requests.Response]:
        """Faz uma requisição HTTP com rate limiting"""
        try:
            response = requests.get(url, timeout=Config.TIMEOUT)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"Erro na requisição para {url}: {e}")
            return None

class BackupManager:
    @staticmethod
    def criar_backup() -> Optional[str]:
        """Cria um backup compactado de todos os documentos"""
        try:
            os.makedirs(Config.BACKUP_DIR, exist_ok=True)
            backup_file = os.path.join(
                Config.BACKUP_DIR,
                f'backup_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
            )
            
            with zipfile.ZipFile(backup_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(Config.BASE_DIR):
                    for file in files:
                        if not file.endswith('.zip'):
                            full_path = os.path.join(root, file)
                            arcname = os.path.relpath(full_path, Config.BASE_DIR)
                            zipf.write(full_path, arcname)
            
            logger.info(f"Backup criado: {backup_file}")
            return backup_file
        except Exception as e:
            logger.error(f"Erro ao criar backup: {e}")
            return None
    
    @staticmethod
    def restaurar_backup(backup_file: str) -> bool:
        """Restaura um backup existente"""
        try:
            with zipfile.ZipFile(backup_file, 'r') as zipf:
                zipf.extractall(Config.BASE_DIR)
            logger.info(f"Backup restaurado: {backup_file}")
            return True
        except Exception as e:
            logger.error(f"Erro ao restaurar backup: {e}")
            return False

class SemanticSearch:
    def __init__(self):
        self.model = SentenceTransformer(Config.EMBEDDING_MODEL)
        self.embeddings = {}
        self.load_embeddings()
    
    def load_embeddings(self):
        """Carrega embeddings existentes do disco"""
        os.makedirs(Config.EMBEDDINGS_DIR, exist_ok=True)
        
        for filename in os.listdir(Config.EMBEDDINGS_DIR):
            if filename.endswith('.npy'):
                doc_id = filename[:-4]
                embedding = np.load(os.path.join(Config.EMBEDDINGS_DIR, filename))
                self.embeddings[doc_id] = embedding
    
    def add_document(self, doc_id: str, text: str):
        """Adiciona um novo documento ao índice de busca semântica"""
        embedding = self.model.encode(text)
        np.save(os.path.join(Config.EMBEDDINGS_DIR, f"{doc_id}.npy"), embedding)
        self.embeddings[doc_id] = embedding
    
    def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Realiza uma busca semântica"""
        SEMANTIC_SEARCHES.inc()
        query_embedding = self.model.encode(query)
        
        similarities = {}
        for doc_id, doc_embedding in self.embeddings.items():
            sim = util.pytorch_cos_sim(query_embedding, doc_embedding).item()
            similarities[doc_id] = sim
        
        # Ordenar por similaridade e retornar os top_k
        sorted_docs = sorted(similarities.items(), key=lambda x: x[1], reverse=True)[:top_k]
        
        return [{"doc_id": doc_id, "similarity": score} for doc_id, score in sorted_docs]
    # ==================== FUNÇÕES UTILITÁRIAS ====================

def criar_diretorios() -> None:
    """Cria todos os diretórios necessários para o funcionamento do crawler."""
    dirs = [
        Config.BASE_DIR,
        Config.CHECKPOINT_DIR,
        Config.RAW_DIR,
        Config.TXT_DIR,
        Config.METADATA_DIR,
        Config.LOG_DIR,
        Config.BACKUP_DIR,
        Config.PLUGIN_DIR,
        Config.EMBEDDINGS_DIR
    ]
    
    for dir_path in dirs:
        try:
            os.makedirs(dir_path, exist_ok=True)
            logger.debug(f"Diretório criado/verificado: {dir_path}")
        except Exception as e:
            logger.error(f"Erro ao criar diretório {dir_path}: {e}")
            raise

def verificar_espaco_disco(min_mb: int = 100) -> bool:
    """Verifica se há espaço suficiente em disco para operações."""
    try:
        stat = os.statvfs(Config.BASE_DIR)
        free_space = stat.f_bavail * stat.f_frsize
        return free_space > min_mb * 1024 * 1024
    except Exception as e:
        logger.error(f"Erro ao verificar espaço em disco: {e}")
        return False

def verificar_saude_sistema() -> Dict[str, Any]:
    """Realiza verificações de saúde do sistema."""
    checks = {
        'espaco_disco': verificar_espaco_disco(),
        'memoria_disponivel': psutil.virtual_memory().available > 100 * 1024 * 1024,
        'conexao_internet': testar_conexao_internet(),
        'acesso_api_vaticano': testar_acesso_api(),
        'redis_disponivel': testar_conexao_redis(),
        'modelos_nlp_carregados': True
    }
    
    status = {
        'status': 'healthy' if all(checks.values()) else 'unhealthy',
        'checks': checks,
        'timestamp': datetime.datetime.now().isoformat(),
        'version': '2.1.0'
    }
    
    return status

def testar_conexao_internet() -> bool:
    """Testa a conexão com a internet."""
    try:
        response = requests.get("https://www.google.com", timeout=10)
        return response.status_code == 200
    except Exception:
        return False

def testar_acesso_api() -> bool:
    """Testa o acesso à API do Vaticano."""
    try:
        response = requests.get(f"{Config.BASE_URL}/content/vatican/it.html", timeout=10)
        return response.status_code == 200
    except Exception:
        return False

def testar_conexao_redis() -> bool:
    """Testa a conexão com o Redis."""
    try:
        conn = redis.Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT)
        return conn.ping()
    except Exception:
        return False

def gerar_nome_arquivo(url: str, extensao: str = "html") -> str:
    """Gera um nome de arquivo único baseado no hash da URL."""
    hash_nome = hashlib.md5(url.encode()).hexdigest()
    return f"{hash_nome}.{extensao}"

def is_valid_url(url: str) -> bool:
    """Valida se a URL está no formato correto."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.netloc.endswith("vatican.va")
    except ValueError:
        return False

def detectar_idioma(texto: str) -> str:
    """Detecta o idioma do texto usando langdetect."""
    try:
        lang = detect(texto)
        return lang if lang in Config.LANGUAGES else "desconhecido"
    except Exception as e:
        logger.warning(f"Erro ao detectar idioma: {e}")
        return "desconhecido"

def salvar_arquivo(caminho: str, conteudo, modo: str = "w", binario: bool = False) -> bool:
    """Salva conteúdo em arquivo com tratamento de erros."""
    if not verificar_espaco_disco():
        logger.error("Espaço em disco insuficiente para salvar arquivo")
        return False
        
    try:
        with file_lock:
            os.makedirs(os.path.dirname(caminho), exist_ok=True)
            with open(caminho, "wb" if binario else "w", 
                    encoding=None if binario else "utf-8") as f:
                f.write(conteudo)
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo {caminho}: {e}")
        return False

def carregar_arquivo(caminho: str, binario: bool = False):
    """Carrega conteúdo de arquivo com tratamento de erros."""
    try:
        with file_lock:
            with open(caminho, "rb" if binario else "r", 
                    encoding=None if binario else "utf-8") as f:
                return f.read()
    except Exception as e:
        logger.error(f"Erro ao carregar arquivo {caminho}: {e}")
        return None

def salvar_json(caminho: str, dados: Dict) -> bool:
    """Salva dados em formato JSON com formatação legível."""
    try:
        with file_lock:
            os.makedirs(os.path.dirname(caminho), exist_ok=True)
            with open(caminho, "w", encoding="utf-8") as f:
                json.dump(dados, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar JSON {caminho}: {e}")
        return False

def carregar_json(caminho: str) -> Dict:
    """Carrega dados de um arquivo JSON."""
    try:
        with file_lock:
            with open(caminho, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Erro ao carregar JSON {caminho}: {e}")
        return {}

# ==================== PROCESSAMENTO DE DOCUMENTOS ====================

def criar_sessao_cacheada() -> CachedSession:
    """Cria sessão com cache para evitar requisições repetidas."""
    return CachedSession(
        'vatican_cache',
        backend='sqlite',
        expire_after=datetime.timedelta(days=1),
        allowable_methods=['GET', 'POST'],
        stale_if_error=True
    )

@REQUEST_TIME.time()
def baixar_documento(url: str) -> Optional[bytes]:
    """Baixa o conteúdo de uma URL com tratamento de erros."""
    session = criar_sessao_cacheada()
    
    retry = Retry(
        total=Config.MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    try:
        logger.info(f"Baixando documento: {url}")
        response = session.get(url, timeout=Config.TIMEOUT)
        response.raise_for_status()
        
        if len(response.content) == 0:
            logger.warning(f"Conteúdo vazio recebido de {url}")
            return None
            
        DOC_SIZE.set(len(response.content))
        logger.info(f"Documento baixado com sucesso. Tamanho: {len(response.content)} bytes")
        return response.content
    except requests.RequestException as e:
        logger.error(f"Erro ao baixar {url}: {e}")
        CRAWL_ERRORS.inc()
        return None

def processar_pdf_em_blocos(pdf_path: str, bloco_size: int = 10) -> str:
    """Processa PDF em blocos para evitar sobrecarga de memória."""
    texto = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if len(pdf.pages) > Config.MAX_PDF_PAGES:
                logger.warning(f"PDF muito grande ({len(pdf.pages)} páginas): {pdf_path}")
                return ""
                
            for i in range(0, len(pdf.pages), bloco_size):
                bloco = pdf.pages[i:i+bloco_size]
                texto.extend(p.extract_text() or "" for p in bloco)
        return "".join(texto)
    except Exception as e:
        logger.error(f"Erro ao processar PDF em blocos: {e}")
        return ""

def extrair_referencias_biblicas(texto: str) -> List[str]:
    """Extrai referências bíblicas do texto usando NLP."""
    doc = nlp(texto)
    referencias = []
    
    # Padrões comuns de referências bíblicas
    padroes = [
        [{"LOWER": {"IN": ["gn", "ex", "lv", "nm", "dt", "js", "jz", "rt", "1sm", "2sm", "1rs", "2rs", "1cr", "2cr", "ed", "ne", "tb", "jt", "et", "1mc", "2mc", "job", "sl", "pv", "ec", "ct", "sb", "eclo", "is", "jr", "lm", "br", "ez", "dn", "os", "jl", "am", "ab", "jn", "mq", "na", "hc", "sf", "ag", "zc", "ml", "mt", "mc", "lc", "jo", "atos", "rm", "1cor", "2cor", "gl", "ef", "fl", "cl", "1ts", "2ts", "1tm", "2tm", "tt", "fm", "hb", "tg", "1pe", "2pe", "1jo", "2jo", "3jo", "jd", "ap"]}}, 
         {"ORTH": {"REGEX": r"^\d+$"}}, 
         {"ORTH": ":", "OP": "?"}, 
         {"ORTH": {"REGEX": r"^\d+"}, "OP": "?"}, 
         {"ORTH": {"REGEX": r"^[-–]\d+"}, "OP": "?"}]
    ]
    
    matcher = spacy.matcher.Matcher(nlp.vocab)
    matcher.add("BIBLE_REF", padroes)
    
    matches = matcher(doc)
    for match_id, start, end in matches:
        span = doc[start:end]
        referencias.append(span.text)
        BIBLE_REFERENCES.inc()
    
    return list(set(referencias))  # Remove duplicatas

def converter_para_texto(content: bytes, url: str) -> str:
    try:
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(Config.TIMEOUT)

        try:
            if url.endswith(".pdf"):
                with tempfile.NamedTemporaryFile(suffix=".pdf") as temp_pdf:
                    temp_pdf.write(content)
                    temp_pdf.flush()
                    texto = processar_pdf_em_blocos(temp_pdf.name)
                    logger.info(f"PDF convertido para texto. Tamanho: {len(texto)} caracteres")
                    return texto

            elif url.endswith((".html", ".htm")):
                soup = BeautifulSoup(content, "html.parser")
                texto = soup.get_text(separator="\n", strip=True)
                logger.info(f"HTML convertido para texto. Tamanho: {len(texto)} caracteres")
                return texto

            else:
                texto = html2text.html2text(content.decode("utf-8", errors="ignore"))
                logger.info(f"Conteúdo convertido para texto. Tamanho: {len(texto)} caracteres")
                return texto

        except TimeoutException:
            logger.warning(f"Timeout ao converter {url}")
            return ""

        finally:
            if threading.current_thread() is threading.main_thread():
                signal.alarm(0)

    except Exception as e:
        logger.error(f"Erro ao converter {url} para texto: {e}")
        return ""

def identificar_categoria_por_url(url: str) -> str:
    """Identifica a categoria do documento baseado na URL."""
    url = url.lower()
    if "encyclicals" in url:
        return "enciclica"
    elif "councils" in url:
        return "concilio"
    elif "ccc" in url or "catechism" in url:
        return "catecismo"
    elif "cdc" in url:
        return "codigo_direito_canonico"
    elif "justpeace" in url:
        return "doutrina_social"
    elif "speeches" in url:
        return "discurso"
    elif "homilies" in url:
        return "homilia"
    elif "letters" in url:
        return "carta"
    elif "messages" in url:
        return "mensagem"
    else:
        return "outros"

def priorizar_urls(urls: List[str]) -> List[str]:
    """Prioriza URLs especiais por importância/categoria"""
    prioridades = {
        'catecismo': 10,
        'codigo_direito_canonico': 10,
        'compendio_doutrina': 9,
        'concilios': 9,
        'enciclicas': 8,
        'doutrina_social': 7,
        'atos_oficiais': 7,
        'liturgia': 6,
        'documentos_papas': 5,
        'outros': 1
    }
    
    urls_priorizadas = []
    for url in urls:
        prioridade = 1
        for key, value in prioridades.items():
            if key in url:
                prioridade = value
                break
        urls_priorizadas.append((url, prioridade))
    
    return [url for url, _ in sorted(urls_priorizadas, key=lambda x: x[1], reverse=True)]

def priorizar_urls(urls: List[str]) -> List[str]:
    """Prioriza URLs por importância/categoria"""
    prioridades = {
        'enciclica': 5,
        'concilio': 5,
        'catecismo': 4,
        'codigo_direito_canonico': 4,
        'doutrina_social': 3,
        'discurso': 2,
        'homilia': 2,
        'carta': 2,
        'mensagem': 2,
        'outros': 1
    }
    return sorted(urls, key=lambda x: prioridades[identificar_categoria_por_url(x)], reverse=True)

# ==================== METADADOS E CHECKPOINT ====================

CHECKPOINT_FILE = os.path.join(Config.CHECKPOINT_DIR, "download_checkpoint.json")

class GeradorMetadados:
    def gerar(self, url: str, texto: str, formato: str) -> Dict:
        """Gera metadados completos para um documento."""
        
        # Identificar papa baseado na URL
        papa = "Desconhecido"
        if "/francesco/" in url.lower():
            papa = "Papa Francisco"
        elif "/benedict-xvi/" in url.lower():
            papa = "Papa Bento XVI"
        elif "/john-paul-ii/" in url.lower():
            papa = "Papa João Paulo II"
        try:
            categoria = identificar_categoria_por_url(url)
            idioma = detectar_idioma(texto)
            tamanho = len(texto.encode('utf-8'))
            hash_conteudo = hashlib.sha256(texto.encode('utf-8')).hexdigest()
            referencias = extrair_referencias_biblicas(texto)
            resumo = gerar_resumo(texto, idioma if idioma != "desconhecido" else "portuguese")
            
            return {
                "id": str(uuid.uuid4()),
                "titulo": os.path.basename(url),
                "url": url,
                "idioma": idioma,
                "papa": papa,
                "categoria": categoria,
                "formato": formato,
                "tamanho_bytes": tamanho,
                "data_coleta": datetime.datetime.utcnow().isoformat(),
                "hash_conteudo": hash_conteudo,
                "referencias_biblicas": referencias,
                "resumo": resumo,
                "texto_amostra": texto[:500] + "..." if len(texto) > 500 else texto
            }
        except Exception as e:
            logger.error(f"Erro ao gerar metadados: {e}")
            return {}

def carregar_checkpoint() -> Set[str]:
    """Carrega os URLs já processados do arquivo de checkpoint."""
    try:
        if os.path.exists(CHECKPOINT_FILE):
            dados = carregar_json(CHECKPOINT_FILE)
            return set(dados.get("urls_processadas", []))
        return set()
    except Exception as e:
        logger.error(f"Erro ao carregar checkpoint: {e}")
        return set()

def salvar_checkpoint(urls_processadas: Set[str]) -> bool:
    """Salva o estado atual do checkpoint."""
    try:
        dados = {
            "urls_processadas": list(urls_processadas),
            "ultima_atualizacao": datetime.datetime.utcnow().isoformat(),
            "total_documentos": len(urls_processadas)
        }
        return salvar_json(CHECKPOINT_FILE, dados)
    except Exception as e:
        logger.error(f"Erro ao salvar checkpoint: {e}")
        return False

def atualizar_checkpoint(url: str) -> bool:
    """Atualiza o checkpoint com uma nova URL processada."""
    try:
        with file_lock:
            urls = carregar_checkpoint()
            urls.add(url)
            return salvar_checkpoint(urls)
    except Exception as e:
        logger.error(f"Erro ao atualizar checkpoint com {url}: {e}")
        return False
    # ==================== FUNÇÕES PRINCIPAIS ====================

def gerar_resumo(texto: str, idioma: str = "portuguese") -> str:
    """Gera um resumo do texto usando TextRank."""
    try:
        doc = nlp(texto)
        resumo = []
        
        for sent in doc._.textrank.summary(limit_phrases=15, limit_sentences=Config.SUMMARY_SENTENCES):
            resumo.append(str(sent))
        
        return " ".join(resumo)
    except Exception as e:
        logger.error(f"Erro ao gerar resumo: {e}")
        return ""

def processar_documento(url: str, semantic_search: Optional[SemanticSearch] = None) -> bool:
    """Processa um documento completo (download, conversão e metadados)."""
    try:
        logger.info(f"Iniciando processamento do documento: {url}")
        
        if not is_valid_url(url):
            logger.warning(f"URL inválida: {url}")
            return False

        if not verificar_espaco_disco():
            logger.error("Espaço em disco insuficiente para processar documento")
            return False

        # Baixar documento
        logger.info(f"Baixando documento: {url}")
        content = baixar_documento(url)
        if not content:
            logger.warning(f"Falha no download de {url}")
            return False

        logger.info(f"Documento baixado com sucesso. Tamanho: {len(content)} bytes")

        # Verificar tamanho do documento
        if len(content) > Config.MAX_DOC_SIZE_MB * 1024 * 1024:
            logger.warning(f"Documento muito grande ({len(content)/1024/1024:.2f}MB): {url}")
            return False

        # Salvar documento bruto
        nome_arquivo = gerar_nome_arquivo(url)
        caminho_original = os.path.join(Config.RAW_DIR, nome_arquivo)
        logger.info(f"Salvando documento bruto em: {caminho_original}")
        if not salvar_arquivo(caminho_original, content, binario=True):
            logger.error("Falha ao salvar documento bruto")
            return False

        # Converter para texto
        logger.info("Convertendo documento para texto")
        texto_extraido = converter_para_texto(content, url)
        if not texto_extraido:
            logger.warning(f"Não foi possível extrair texto de {url}")
            return False

        logger.info(f"Texto extraído com sucesso. Tamanho: {len(texto_extraido)} caracteres")

        # Salvar texto convertido
        caminho_texto = os.path.join(Config.TXT_DIR, nome_arquivo.replace(".html", ".txt"))
        logger.info(f"Salvando texto convertido em: {caminho_texto}")
        if not salvar_arquivo(caminho_texto, texto_extraido):
            logger.error("Falha ao salvar texto convertido")
            return False

        # Gerar resumo
        logger.info("Gerando resumo do documento")
        try:
            resumo = gerar_resumo(texto_extraido)
        except Exception as e:
            logger.error(f"Erro ao gerar resumo: {e}")
            resumo = ""

        # Gerar e salvar metadados
        formato = "pdf" if url.endswith(".pdf") else "html"
        logger.info("Gerando metadados do documento")
        metadados = GeradorMetadados().gerar(url, texto_extraido, formato)
        caminho_metadados = os.path.join(Config.METADATA_DIR, nome_arquivo.replace(".html", ".json"))
        logger.info(f"Salvando metadados em: {caminho_metadados}")
        if not salvar_json(caminho_metadados, metadados):
            logger.error("Falha ao salvar metadados")
            return False

        # Adicionar ao índice de busca semântica
        if semantic_search:
            logger.info("Adicionando documento ao índice de busca semântica")
            try:
                semantic_search.add_document(metadados["id"], texto_extraido)
            except Exception as e:
                logger.error(f"Erro ao adicionar documento ao índice semântico: {e}")

        # Registrar como processado
        logger.info("Atualizando checkpoint")
        if not atualizar_checkpoint(url):
            logger.error("Falha ao atualizar checkpoint")
            return False

        FILES_PROCESSED.inc()
        logger.info(f"Documento processado com sucesso: {url}")
        return True

    except Exception as e:
        logger.error(f"Erro ao processar {url}: {e}", exc_info=True)
        CRAWL_ERRORS.inc()
        return False

def obter_lista_urls() -> List[str]:
    """Carrega e prioriza a lista de URLs a serem processadas."""
    lista_path = os.path.join(Config.BASE_DIR, "urls.txt")
    
    # Cria o arquivo padrão se não existir
    if not os.path.exists(lista_path):
        urls_padrao = [
            "https://www.vatican.va/content/vatican/pt.html",
            "https://www.vatican.va/archive/cdc/index_po.htm",
            "https://www.vatican.va/archive/ccc/index_po.htm",
            "https://www.vatican.va/roman_curia/pontifical_councils/justpeace/documents/rc_pc_justpeace_doc_20060526_compendio-dott-soc_po.html",
            "https://www.vatican.va/archive/atti-ufficiali-santa-sede/index_po.htm",
            "https://www.vatican.va/archive/hist_councils/index_po.htm",
            "https://www.vatican.va/roman_curia/pontifical_councils/justpeace/documents/index_po.htm",
            "https://www.vatican.va/news_services/liturgy/index_po.htm",
            "https://www.vatican.va/content/francesco/pt/index.html"
        ]
        with open(lista_path, "w", encoding="utf-8") as f:
            f.write("\n".join(urls_padrao))
        logger.info(f"Arquivo de URLs criado com {len(urls_padrao)} entradas")

    with open(lista_path, "r", encoding="utf-8") as f:
        urls = [linha.strip() for linha in f if linha.strip() and is_valid_url(linha.strip())]
    
    urls_priorizadas = priorizar_urls(urls)
    logger.info(f"{len(urls_priorizadas)} URLs válidas carregadas (após priorização)")
    return urls_priorizadas

@BATCH_DURATION.time()
def iniciar_coleta_em_lote(lista_urls: List[str], max_threads: int = Config.MAX_WORKERS) -> bool:
    """Processa múltiplos documentos em paralelo."""
    try:
        if not verificar_espaco_disco():
            logger.error("Espaço em disco insuficiente para iniciar coleta")
            return False

        logger.info(f"Iniciando coleta - {len(lista_urls)} documentos")
        urls_processadas = carregar_checkpoint()
        total_processados = 0
        
        # Inicializar busca semântica
        semantic_search = SemanticSearch()
        
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {
                executor.submit(processar_documento, url, semantic_search): url
                for url in lista_urls
                if url not in urls_processadas
            }
            
            for future in as_completed(futures):
                url = futures[future]
                try:
                    if future.result():
                        total_processados += 1
                except Exception as e:
                    logger.error(f"Erro durante processamento de {url}: {e}")
                    
        logger.info(f"Coleta concluída - {total_processados}/{len(lista_urls)} documentos processados")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao iniciar coleta em lote: {e}", exc_info=True)
        return False

def executar_crawl() -> None:
    """Função principal para execução do crawler."""
    logger.info("Iniciando modo de coleta")
    criar_diretorios()
    
    # Verificar saúde do sistema antes de começar
    status_sistema = verificar_saude_sistema()
    logger.info(f"Status do sistema: {json.dumps(status_sistema, indent=2)}")
    
    if not status_sistema['checks']['espaco_disco']:
        logger.error("Abortando: espaço em disco insuficiente")
        return
    if not status_sistema['checks']['memoria_disponivel']:
        logger.error("Abortando: memória insuficiente")
        return
    
    urls = obter_lista_urls()
    if not urls:
        logger.warning("Nenhuma URL válida para processar")
        return
    
    CRAWLER_STATE.set(1)
    try:
        if not iniciar_coleta_em_lote(urls):
            logger.error("Falha durante a coleta em lote")
    finally:
        CRAWLER_STATE.set(0)
        # Criar backup após cada execução
        logger.info("Criando backup dos dados")
        BackupManager.criar_backup()

# ==================== API E MONITORAMENTO ====================

class DocumentRequest(BaseModel):
    url: str

class SearchRequest(BaseModel):
    query: str
    top_k: int = 5

def executar_api() -> None:
    """Inicia o servidor API."""
    logger.info("Iniciando servidor API")
    app = FastAPI()
    plugin_manager = PluginManager()
    plugin_manager.carregar_plugins()
    semantic_search = SemanticSearch()
    
    @app.get("/")
    def status():
        return {
            "status": "ativo",
            "timestamp": datetime.datetime.now().isoformat(),
            "versao": "2.1.0"
        }
    
    @app.get("/saude")
    def saude():
        return verificar_saude_sistema()
    
    @app.get("/estatisticas")
    def estatisticas():
        return {
            "documentos_processados": FILES_PROCESSED._value.get(),
            "erros": CRAWL_ERRORS._value.get(),
            "estado": CRAWLER_STATE._value.get(),
            "tamanho_fila": DocumentQueue().tamanho_fila(),
            "referencias_biblicas": BIBLE_REFERENCES._value.get(),
            "buscas_semanticas": SEMANTIC_SEARCHES._value.get()
        }
    
    @app.post("/adicionar_url")
    def adicionar_url(document: DocumentRequest):
        if not is_valid_url(document.url):
            raise HTTPException(status_code=400, detail="URL inválida")
        
        if DocumentQueue().adicionar_documento(document.url):
            return {"status": "sucesso", "mensagem": "URL adicionada à fila"}
        else:
            raise HTTPException(status_code=500, detail="Falha ao adicionar URL")
    
    @app.post("/buscar_semantica")
    def buscar_semantica(search: SearchRequest):
        try:
            results = semantic_search.search(search.query, search.top_k)
            return {
                "status": "sucesso",
                "resultados": results
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/documento/{doc_id}")
    def obter_documento(doc_id: str):
        metadata_path = os.path.join(Config.METADATA_DIR, f"{doc_id}.json")
        if not os.path.exists(metadata_path):
            raise HTTPException(status_code=404, detail="Documento não encontrado")
        
        metadata = carregar_json(metadata_path)
        return metadata
    
    @app.get("/documentos/{categoria}")
    def listar_documentos(categoria: str):
        documentos = []
        for filename in os.listdir(Config.METADATA_DIR):
            if filename.endswith('.json'):
                metadata = carregar_json(os.path.join(Config.METADATA_DIR, filename))
                if metadata.get('categoria', '').lower() == categoria.lower():
                    documentos.append({
                        "id": metadata.get("id"),
                        "titulo": metadata.get("titulo"),
                        "url": metadata.get("url"),
                        "data_coleta": metadata.get("data_coleta")
                    })
        return documentos
    
    uvicorn.run(app, host="0.0.0.0", port=8000)

def executar_monitor() -> None:
    """Inicia o servidor de monitoramento Prometheus."""
    logger.info(f"Iniciando monitor Prometheus na porta {Config.MONITOR_PORT}")
    start_http_server(Config.MONITOR_PORT)
    
    # Manter o serviço rodando
    while True:
        time.sleep(1)

def executar_worker() -> None:
    """Inicia o worker para processar a fila de documentos."""
    logger.info("Iniciando worker para processar fila de documentos")
    semantic_search = SemanticSearch()
    fila = DocumentQueue()
    
    while True:
        try:
            url = fila.obter_proximo_documento()
            if url:
                logger.info(f"Processando documento da fila: {url}")
                processar_documento(url, semantic_search)
            else:
                time.sleep(5)
        except Exception as e:
            logger.error(f"Erro no worker: {e}")
            time.sleep(10)

def verificar_memoria():
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    logger.info(f"Memória: {mem.percent}% usado | Swap: {swap.percent}% usado")
    return mem.percent < 85 and swap.percent < 80

def main():
    parser = argparse.ArgumentParser(description="Crawler de Documentos do Vaticano")
    parser.add_argument("--crawl", action="store_true", help="Executa o processo de coleta")
    parser.add_argument("--api", action="store_true", help="Inicia a API para consulta")
    parser.add_argument("--monitor", action="store_true", help="Inicia o monitor Prometheus")
    parser.add_argument("--worker", action="store_true", help="Inicia como worker processando a fila")
    parser.add_argument("--max_workers", type=int, default=Config.MAX_WORKERS, help="Número máximo de workers")
    
    args = parser.parse_args()
    Config.MAX_WORKERS = args.max_workers
    
    criar_diretorios()
    
    try:
        if args.monitor:
            executar_monitor()
        elif args.api:
            executar_api()
        elif args.crawl:
            executar_crawl()
        elif args.worker:
            logger.info(f"Iniciando worker com {Config.MAX_WORKERS} threads")
            executar_worker()
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()