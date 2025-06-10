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
import random
import dns.resolver
import aiofiles
import asyncio
from typing import Optional, List, Set, Dict, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from functools import wraps
import unicodedata
from typing import Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from threading import Thread, Lock
import tarfile
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from fastapi import FastAPI, Query, Path, HTTPException
from pydantic import BaseModel
import uvicorn
import spacy
from elasticsearch import Elasticsearch
import redis

def log_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logging.debug(f"{func.__name__} executado em {end - start:.2f}s")
        return result
    return wrapper

class CrawlerException(Exception):
    """Exceção base para o crawler"""
    pass

class URLInvalidaException(CrawlerException):
    """Exceção para URLs inválidas"""
    pass

class ConteudoInvalidoException(CrawlerException):
    """Exceção para conteúdo inválido"""
    pass

class LimiteExcedidoException(CrawlerException):
    """Exceção para quando limites são excedidos"""
    pass

class ProcessamentoException(CrawlerException):
    """Exceção para erros de processamento"""
    pass

class CacheException(CrawlerException):
    """Exceção para erros de cache"""
    pass

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class CachingDNSResolver:
    def __init__(self):
        self._cache = {}
    
    def resolve(self, host):
        if host not in self._cache:
            answers = dns.resolver.resolve(host, 'A')
            self._cache[host] = [answer.address for answer in answers]
        return self._cache[host]

dns_resolver = CachingDNSResolver()

class CacheLocal:
    """Sistema de cache local para URLs já processadas"""
    def __init__(self, cache_dir: str = None):
        self.cache_dir = cache_dir or os.path.join(Config.BASE_DIR, 'cache')
        self.memory_cache = {}
        self.cache_size = 1000
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def _get_cache_path(self, key: str) -> str:
        """Gera caminho do arquivo de cache"""
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hash_key}.cache")
    
    def get(self, key: str) -> Optional[dict]:
        """Recupera item do cache"""
        try:
            if key in self.memory_cache:
                return self.memory_cache[key]
            
            cache_path = self._get_cache_path(key)
            if os.path.exists(cache_path):
                with open(cache_path, 'r') as f:
                    data = json.load(f)
                    self.memory_cache[key] = data
                    return data
        except Exception as e:
            logging.debug(f"Erro ao ler cache: {e}")
        return None
    
    def set(self, key: str, value: dict):
        """Armazena item no cache"""
        try:
            self.memory_cache[key] = value
            if len(self.memory_cache) > self.cache_size:
                self.memory_cache.pop(next(iter(self.memory_cache)))
            
            cache_path = self._get_cache_path(key)
            with open(cache_path, 'w') as f:
                json.dump(value, f)
        except Exception as e:
            logging.debug(f"Erro ao salvar cache: {e}")
    
    def clear(self):
        """Limpa o cache"""
        self.memory_cache.clear()
        for file in os.listdir(self.cache_dir):
            os.remove(os.path.join(self.cache_dir, file))

class RateLimiter:
    def __init__(self, requests_per_second: float = 1.0):
        self.delay = 1.0 / requests_per_second
        self.last_request = 0
        self.lock = Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_request = time.time()

class BackupManager:
    def __init__(self, base_dir: str, max_backups: int = 5):
        self.base_dir = base_dir
        self.backup_dir = os.path.join(base_dir, 'backups')
        self.max_backups = max_backups
        os.makedirs(self.backup_dir, exist_ok=True)
    
    def criar_backup(self):
        """Cria backup dos dados coletados"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(self.backup_dir, f'backup_{timestamp}.tar.gz')
            
            # Cria arquivo tar.gz com os dados
            with tarfile.open(backup_path, 'w:gz') as tar:
                tar.add(os.path.join(self.base_dir, 'raw'), arcname='raw')
                tar.add(os.path.join(self.base_dir, 'txt'), arcname='txt')
                tar.add(os.path.join(self.base_dir, 'metadata'), arcname='metadata')
            
            # Remove backups antigos
            self._limpar_backups_antigos()
            
            logging.info(f"Backup criado: {backup_path}")
            return True
        except Exception as e:
            logging.error(f"Erro ao criar backup: {e}")
            return False
    
    def _limpar_backups_antigos(self):
        """Remove backups mais antigos se exceder o limite"""
        backups = sorted(
            [f for f in os.listdir(self.backup_dir) if f.startswith('backup_')],
            reverse=True
        )
        
        for backup in backups[self.max_backups:]:
            os.remove(os.path.join(self.backup_dir, backup))


class ContentAnalyzer:
    def __init__(self):
        self.nlp_cache = {}
        self.importantes_cache = {}
    
    def analisar_documento(self, texto: str, url: str) -> Dict:
        """Análise completa do documento"""
        return {
            'tipo_documento': self._identificar_tipo_documento(texto),
            'idioma': self._detectar_idioma(texto),
            'relevancia': self._calcular_relevancia(texto),
            'entidades': self._extrair_entidades(texto),
            'topicos': self._identificar_topicos(texto),
            'referencias': self._encontrar_referencias(texto),
            'citacoes': self._extrair_citacoes(texto)
        }
    
    def _identificar_tipo_documento(self, texto: str) -> str:
        tipos = {
            'enciclica': r'enc[íi]clica|encyclical',
            'exortacao': r'exorta[çc][ãa]o|exhortation',
            'bula': r'bula papal|papal bull',
            'homilia': r'homilia|homily',
            'discurso': r'discurso|speech',
            'carta': r'carta|letter',
            'motu_proprio': r'motu\s+proprio'
        }
        
        for tipo, padrao in tipos.items():
            if re.search(padrao, texto, re.IGNORECASE):
                return tipo
        return 'outro'
    
    def _detectar_idioma(self, texto: str) -> str:
        # Padrões comuns em português
        padroes_pt = ['ção', 'ções', 'mente', 'ante', 'ando', 'endo', 'indo']
        matches_pt = sum(1 for p in padroes_pt if p in texto.lower())
        return 'pt' if matches_pt >= 2 else 'outro'
    
    def _calcular_relevancia(self, texto: str) -> float:
        palavras_importantes = set(Config.IMPORTANT_KEYWORDS)
        palavras_texto = set(texto.lower().split())
        
        # Calcula interseção com palavras importantes
        relevancia = len(palavras_texto.intersection(palavras_importantes)) / len(palavras_importantes)
        return min(1.0, relevancia)
    
    def _extrair_entidades(self, texto: str) -> Dict[str, List[str]]:
        entidades = {
            'pessoas': [],
            'lugares': [],
            'organizacoes': [],
            'datas': []
        }
        
        # Extrai datas
        entidades['datas'] = re.findall(r'\d{1,2}/\d{1,2}/\d{4}', texto)
        
        # Extrai nomes próprios (simplificado)
        nomes = re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', texto)
        entidades['pessoas'] = [n for n in nomes if len(n.split()) >= 2]
        
        return entidades
    
    def _identificar_topicos(self, texto: str) -> List[str]:
        # Implementação simplificada - identifica tópicos por frequência de palavras
        palavras = texto.lower().split()
        freq = {}
        for palavra in palavras:
            if len(palavra) > 3:  # ignora palavras muito curtas
                freq[palavra] = freq.get(palavra, 0) + 1
        
        # Retorna as 5 palavras mais frequentes como tópicos
        return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:5]
    
    def _encontrar_referencias(self, texto: str) -> List[str]:
        # Procura referências bibliográficas e citações
        refs = []
        
        # Padrões comuns de referência
        padroes = [
            r'\(\d{4}\)',  # Ano entre parênteses
            r'\[\d+\]',    # Número entre colchetes
            r'cf\.',       # Abreviação de "conforme"
            r'vid\.'       # Abreviação de "vide"
        ]
        
        for padrao in padroes:
            refs.extend(re.findall(padrao, texto))
        
        return refs
    
    def _extrair_citacoes(self, texto: str) -> List[str]:
        citacoes = []
        # Padrões de citação comuns em documentos do Vaticano
        padroes = [
            r'"([^"]+)"',          # aspas duplas
            r"'([^']+)'",          # aspas simples
            r'«([^»]+)»',          # aspas angulares
            r'[\d]+\.\s+([A-Z][^.!?]+[.!?])'  # numeração seguida de texto
        ]
        
        for padrao in padroes:
            matches = re.finditer(padrao, texto)
            citacoes.extend(m.group(1) for m in matches)
        
        return citacoes[:Config.MAX_CITATIONS_PER_DOC]


class NotificationSystem:
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.last_notification = {}
    
    def notify(self, tipo: str, mensagem: str, nivel: str = 'info'):
        """Envia notificação baseada no tipo e configuração"""
        if not self._should_notify(tipo):
            return
        
        metodos = {
            'email': self._enviar_email,
            'telegram': self._enviar_telegram,
            'log': self._registrar_log
        }
        
        for metodo in self.config.get('metodos', ['log']):
            if metodo in metodos:
                try:
                    metodos[metodo](mensagem, nivel)
                except Exception as e:
                    logging.error(f"Erro ao enviar notificação via {metodo}: {e}")
    
    def _should_notify(self, tipo: str) -> bool:
        """Verifica se deve enviar notificação baseado em rate limiting"""
        agora = time.time()
        if tipo in self.last_notification:
            if agora - self.last_notification[tipo] < self.config.get('intervalo_min', 3600):
                return False
        self.last_notification[tipo] = agora
        return True
    
    def _enviar_email(self, mensagem: str, nivel: str):
        """Envia notificação por email"""
        if 'email' not in self.config:
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['email']['from_email']
            msg['To'] = self.config['email']['to_email']
            msg['Subject'] = f"Crawler Notification - {nivel.upper()}"
            
            msg.attach(MIMEText(mensagem, 'plain'))
            
            with smtplib.SMTP(self.config['email']['smtp_server'], 
                            self.config['email']['smtp_port']) as server:
                server.starttls()
                server.login(self.config['email']['from_email'],
                           self.config['email'].get('password', ''))
                server.send_message(msg)
        except Exception as e:
            logging.error(f"Erro ao enviar email: {e}")
    
    def _enviar_telegram(self, mensagem: str, nivel: str):
        """Envia notificação via Telegram"""
        # Implementar se necessário
        pass
    
    def _registrar_log(self, mensagem: str, nivel: str):
        """Registra notificação no log"""
        getattr(logging, nivel)(mensagem)

class Config:
    # Diretórios base
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

    # Configurações do crawler
    MAX_WORKERS = 8
    RATE_LIMIT = 2.0
    MIN_TEXT_LENGTH = 100
    MAX_TEXT_LENGTH = 1000000
    TIMEOUT_CONNECT = 10
    TIMEOUT_READ = 30
    USER_AGENT_ROTATE_INTERVAL = 60
    TIMEOUT = 20
    MAX_DOC_SIZE_MB = 25
    MAX_MEMORY_PERCENT = 80
    CHUNK_SIZE = 4096
    MAX_DEPTH = 3
    MAX_URLS = 10000
    MAX_RETRIES = 3
    RETRY_DELAY = 10
    STATS_INTERVAL = 300
    REQUEST_DELAY = 1.0

    # Configurações de backup
    BACKUP_INTERVAL = 3600
    MAX_BACKUPS = 5
    
    # Configurações de análise
    MIN_DOCUMENT_RELEVANCE = 0.5
    MAX_CITATIONS_PER_DOC = 100
    IMPORTANT_KEYWORDS = [
        'papa', 'pontifice', 'vaticano', 'igreja',
        'catolica', 'santo', 'santa', 'beatificacao',
        'canonizacao', 'concilio', 'sinodo'
    ]

    # Configurações de notificação
    NOTIFICATION_CONFIG = {
        'metodos': ['log', 'email'],
        'intervalo_min': 3600,
        'email': {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'from_email': 'seu-email@gmail.com',
            'to_email': 'destino@email.com'
        }
    }

    # User Agents
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/91.0.864.48',
        'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1'
    ]

    # Padrões de detecção
    PORTUGUESE_PATTERNS = [
        '/pt/', '_po.', '/po/', 
        'portugues', 'portuguese',
        '_port.', '.port.',
        '/pt-br/', '/pt-pt/',
        'lang=pt', 'lang=po',
        'portuguese.html', 'port.html'
    ]

    PRIORITY_PATTERNS = [
        '/content/francesco/',
        '/content/vatican/pt/',
        '/content/holy-father/',
        '/archive/hist_councils/',
        '/archive/congregations/',
        '/content/romancuria/',
        '/content/news/',
        '/content/speeches/',
        '/content/homilies/',
        '/content/messages/',
        '/content/letters/',
        '/content/motu_proprio/',
        '/content/encyclicals/',
        '/content/apost_exhortations/',
        '/content/apost_constitutions/',
        '/content/apost_letters/',
        '/content/bulletins/',
        '/content/daily-homilies/',
        '/roman_curia/',
        '/archive/ccc/',
        '/archive/cdc/',
        '/archive/vat-ii/'
    ]

    IGNORE_PATTERNS = [
        '.pdf', '.jpg', '.jpeg', '.png', '.gif',
        'youtube.com', 'twitter.com', 'facebook.com',
        'instagram.com', 'javascript:', 'mailto:',
        '#', 'tel:', 'whatsapp:', '.zip', '.doc',
        '.docx', '.mp3', '.mp4', 'javascript:void'
    ]

    PALAVRAS_IMPORTANTES = [
        'encíclica', 'exortação', 'constituição', 'carta apostólica',
        'homilia', 'catequese', 'discurso', 'mensagem', 'bula',
        'motu proprio', 'audiência', 'angelus', 'decreto',
        'papa', 'vaticano', 'santa sé', 'pontífice',
        'concílio', 'sínodo', 'congregação', 'dicastério',
        'declaração', 'instrução', 'nota doutrinal', 'meditação',
        'regina coeli', 'consistório', 'carta circular',
        'documento pontifício', 'constituição dogmática',
        'magistério', 'santo padre', 'congregação'
    ]

    # Novas configurações para API e NLP
    API_HOST = "0.0.0.0"
    API_PORT = 8000
    ELASTICSEARCH_HOST = "http://localhost:9200"
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    CACHE_TIMEOUT = 3600  # 1 hora

    # Configurações do Elasticsearch
    ES_INDEX_DOCS = "vatican_docs"
    ES_INDEX_MAPPING = {
        "mappings": {
            "properties": {
                "url": {"type": "keyword"},
                "titulo": {"type": "text"},
                "texto": {"type": "text"},
                "categoria": {"type": "keyword"},
                "data_coleta": {"type": "date"},
                "tamanho_html": {"type": "integer"},
                "tamanho_texto": {"type": "integer"},
                "num_links": {"type": "integer"},
                "links": {"type": "keyword"},
                "palavras_chave": {"type": "keyword"},
                "eh_importante": {"type": "boolean"},
                "prioridade": {"type": "integer"},
                "idioma": {"type": "keyword"},
                "entidades": {"type": "object"},
                "tipo_documento": {"type": "keyword"},
                "resumo": {"type": "text"},
                "temas": {"type": "nested"},
                "sentimentos": {"type": "object"},
                "termos_doutrinais": {"type": "object"},
                "estatisticas_linguisticas": {"type": "object"}
            }
        }
    }
    
    @classmethod
    def criar_diretorios(cls):
        """Cria os diretórios necessários"""
        for dir_path in [cls.BASE_DIR, cls.RAW_DIR, cls.TXT_DIR, cls.LINKS_DIR,
                        cls.STATS_DIR, cls.ERROR_DIR, cls.METADATA_DIR]:
            os.makedirs(dir_path, exist_ok=True)

class NLPProcessor:
    def __init__(self):
        """Inicializa o processador de linguagem natural"""
        try:
            self.nlp = spacy.load('pt_core_news_lg')
        except OSError:
            logging.warning("Modelo spaCy não encontrado. Tentando fazer download...")
            os.system('python -m spacy download pt_core_news_lg')
            self.nlp = spacy.load('pt_core_news_lg')
        
        self.cache = {}
        self.termos_doutrinais = {
            'sacramentos': ['batismo', 'crisma', 'eucaristia', 'confissão', 'unção', 'ordem', 'matrimônio'],
            'dogmas': ['trindade', 'encarnação', 'ressurreição', 'ascensão', 'assunção'],
            'virtudes': ['fé', 'esperança', 'caridade', 'prudência', 'justiça', 'fortaleza', 'temperança'],
            'moral': ['pecado', 'graça', 'redenção', 'salvação', 'conversão', 'penitência'],
            'eclesiologia': ['igreja', 'papa', 'bispo', 'sacerdote', 'concílio', 'vaticano']
        }
    
    def processar_documento(self, texto: str, doc_id: str = None) -> Dict:
        """Processa um documento completo retornando análise detalhada"""
        if not texto:
            return {}
            
        # Verifica cache
        if doc_id and doc_id in self.cache:
            return self.cache[doc_id]
            
        try:
            doc = self.nlp(texto)
            
            analise = {
                'resumo': self.gerar_resumo(doc),
                'entidades': self.extrair_entidades(doc),
                'temas': self.identificar_temas(doc),
                'sentimentos': self.analisar_sentimentos(doc),
                'termos_doutrinais': self.encontrar_termos_doutrinais(texto),
                'estatisticas': self.calcular_estatisticas(doc)
            }
            
            # Salva em cache se tiver ID
            if doc_id:
                self.cache[doc_id] = analise
                
            return analise
            
        except Exception as e:
            logging.error(f"Erro ao processar documento: {e}")
            return {}
    
    def gerar_resumo(self, doc) -> str:
        """Gera um resumo do texto usando as frases mais relevantes"""
        frases = list(doc.sents)
        if not frases:
            return ""
            
        # Pontua cada frase baseado em palavras-chave e posição
        pontuacoes = []
        palavras_doc = set(token.text.lower() for token in doc if not token.is_stop)
        
        for i, frase in enumerate(frases):
            pontos = 0
            palavras_frase = set(token.text.lower() for token in frase if not token.is_stop)
            
            # Pontos por palavras importantes
            pontos += len(palavras_frase & palavras_doc) * 0.3
            
            # Pontos por posição (início e fim são mais importantes)
            if i < len(frases) * 0.2:  # Primeiros 20%
                pontos += 2
            elif i > len(frases) * 0.8:  # Últimos 20%
                pontos += 1
                
            pontuacoes.append((pontos, frase.text))
            
        # Seleciona as 3 frases mais relevantes
        melhores_frases = sorted(pontuacoes, reverse=True)[:3]
        return " ".join(frase for _, frase in sorted(melhores_frases, key=lambda x: x[1]))
    
    def extrair_entidades(self, doc) -> Dict[str, List[str]]:
        """Extrai e classifica entidades nomeadas do texto"""
        entidades = {
            'pessoas': [],
            'lugares': [],
            'organizacoes': [],
            'datas': [],
            'eventos': []
        }
        
        for ent in doc.ents:
            if ent.label_ == 'PER':
                entidades['pessoas'].append(ent.text)
            elif ent.label_ == 'LOC':
                entidades['lugares'].append(ent.text)
            elif ent.label_ == 'ORG':
                entidades['organizacoes'].append(ent.text)
            elif ent.label_ == 'DATE':
                entidades['datas'].append(ent.text)
            elif ent.label_ == 'EVENT':
                entidades['eventos'].append(ent.text)
                
        # Remove duplicatas e limita tamanho
        for key in entidades:
            entidades[key] = list(set(entidades[key]))[:10]
            
        return entidades
    
    def identificar_temas(self, doc) -> List[Dict[str, float]]:
        """Identifica os principais temas do documento usando análise de frequência e relevância"""
        # Conta frequência de palavras relevantes
        palavras = Counter()
        for token in doc:
            if (not token.is_stop and not token.is_punct and 
                token.pos_ in ['NOUN', 'PROPN', 'ADJ']):
                palavras[token.lemma_] += 1
                
        # Calcula relevância usando TF-IDF simulado
        total_palavras = sum(palavras.values())
        temas = []
        
        for palavra, freq in palavras.most_common(10):
            relevancia = (freq / total_palavras) * log(2.0)  # IDF simplificado
            temas.append({
                'tema': palavra,
                'relevancia': round(relevancia, 3)
            })
            
        return temas
    
    def analisar_sentimentos(self, doc) -> Dict[str, float]:
        """Analisa o sentimento do texto usando léxico de sentimentos"""
        palavras_positivas = set(['bom', 'ótimo', 'excelente', 'santo', 'sagrado', 'divino'])
        palavras_negativas = set(['mal', 'ruim', 'pecado', 'erro', 'heresia'])
        
        positivos = 0
        negativos = 0
        
        for token in doc:
            if token.lemma_.lower() in palavras_positivas:
                positivos += 1
            elif token.lemma_.lower() in palavras_negativas:
                negativos += 1
                
        total = positivos + negativos or 1
        return {
            'positivo': round(positivos / total, 2),
            'negativo': round(negativos / total, 2),
            'neutro': round(1 - ((positivos + negativos) / len(doc)), 2)
        }
    
    def encontrar_termos_doutrinais(self, texto: str) -> Dict[str, List[str]]:
        """Encontra termos doutrinais no texto"""
        texto_lower = texto.lower()
        termos_encontrados = {}
        
        for categoria, termos in self.termos_doutrinais.items():
            encontrados = [
                termo for termo in termos 
                if termo in texto_lower
            ]
            if encontrados:
                termos_encontrados[categoria] = encontrados
                
        return termos_encontrados
    
    def calcular_estatisticas(self, doc) -> Dict:
        """Calcula estatísticas linguísticas do documento"""
        return {
            'num_tokens': len(doc),
            'num_frases': len(list(doc.sents)),
            'tamanho_medio_frase': round(len(doc) / len(list(doc.sents)) if len(list(doc.sents)) > 0 else 0, 2),
            'diversidade_lexica': round(len(set(token.text.lower() for token in doc)) / len(doc) if len(doc) > 0 else 0, 3)
        }

class DocumentSchema(BaseModel):
    """Schema Pydantic para documentos"""
    url: str
    titulo: str
    texto: str
    categoria: str
    data_coleta: str
    tamanho_html: int = 0
    tamanho_texto: int = 0
    num_links: int = 0
    links: List[str] = []
    palavras_chave: List[str] = []
    eh_importante: bool = False
    prioridade: int = 0
    idioma: str = ""
    entidades: Dict = {}
    tipo_documento: str = ""

class DocumentMetadata:
    def __init__(self, url: str, categoria: str):
        # Campos básicos existentes
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
        self.idioma = self._detectar_idioma(url)
        self.texto = ""
        
        # Novos campos NLP
        self.resumo = ""
        self.entidades = {}
        self.temas = []
        self.sentimentos = {}
        self.termos_doutrinais = {}
        self.estatisticas_linguisticas = {}
        self.tipo_documento = ""
    
    def _detectar_idioma(self, url: str) -> str:
        """Detecta o idioma com base na URL"""
        url_lower = url.lower()
        if any(pattern in url_lower for pattern in Config.PORTUGUESE_PATTERNS):
            return 'pt'
        return 'outro'
    
    def update(self, dados: Dict):
        """Atualiza os metadados do documento com novos dados"""
        for chave, valor in dados.items():
            if hasattr(self, chave):
                setattr(self, chave, valor)
            else:
                # Adiciona novos campos dinamicamente
                self.__dict__[chave] = valor

    def to_dict(self) -> Dict:
        """Converte o objeto para dicionário"""
        return {
            'url': self.url,
            'categoria': self.categoria,
            'titulo': self.titulo,
            'descricao': self.descricao,
            'data_coleta': self.data_coleta,
            'tamanho_html': self.tamanho_html,
            'tamanho_texto': self.tamanho_texto,
            'num_links': self.num_links,
            'links': self.links,
            'palavras_chave': self.palavras_chave,
            'eh_importante': self.eh_importante,
            'prioridade': self.prioridade,
            'idioma': self.idioma,
            'texto': self.texto,
            'resumo': self.resumo,
            'entidades': self.entidades,
            'temas': self.temas,
            'sentimentos': self.sentimentos,
            'termos_doutrinais': self.termos_doutrinais,
            'estatisticas_linguisticas': self.estatisticas_linguisticas,
            'tipo_documento': self.tipo_documento,
            **{k: v for k, v in self.__dict__.items() if k not in {
                'url', 'categoria', 'titulo', 'descricao', 'data_coleta',
                'tamanho_html', 'tamanho_texto', 'num_links', 'links',
                'palavras_chave', 'eh_importante', 'prioridade', 'idioma', 'texto',
                'resumo', 'entidades', 'temas', 'sentimentos', 'termos_doutrinais',
                'estatisticas_linguisticas', 'tipo_documento'
            }}
        }

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
        self.tentativas = {}
        self.carregar_estado()
    
    def carregar_estado(self):
        """Carrega estado anterior do crawler"""
        try:
            if os.path.exists(Config.QUEUE_FILE):
                with open(Config.QUEUE_FILE, 'r') as f:
                    data = json.load(f)
                    for item in data.get('queue', []):
                        if isinstance(item, (list, tuple)):
                            if len(item) == 2:
                                url, depth = item
                                self.queue.append((url, depth, calcular_prioridade(url, depth)))
                            else:
                                self.queue.append(tuple(item))
                    self.processadas.update(data.get('processadas', []))
                    self.tentativas = data.get('tentativas', {})
        except Exception as e:
            logging.error(f"Erro ao carregar estado: {e}")
    
    def salvar_estado(self):
        """Salva estado atual do crawler"""
        try:
            with open(Config.QUEUE_FILE, 'w') as f:
                json.dump({
                    'queue': list(self.queue),
                    'processadas': list(self.processadas),
                    'tentativas': self.tentativas
                }, f, indent=2)
        except Exception as e:
            logging.error(f"Erro ao salvar estado: {e}")
    
    def adicionar(self, url: str, depth: int = 0, prioridade: int = None):
        """Adiciona URL à fila se não foi processada ainda"""
        url_norm = normalizar_url(url)
        if (url_norm not in self.processadas and 
            url_norm not in [normalizar_url(x[0]) for x in self.queue] and 
            depth <= Config.MAX_DEPTH and
            len(self.processadas) < Config.MAX_URLS and
            not any(p in url_norm for p in Config.IGNORE_PATTERNS)):
            
            if prioridade is None:
                prioridade = calcular_prioridade(url_norm, depth)
            
            self.queue.append((url_norm, depth, prioridade))
            self.queue = deque(sorted(self.queue, key=lambda x: x[2], reverse=True))
    
    def obter_proxima(self) -> Optional[Tuple[str, int, int]]:
        """Retorna próxima URL a ser processada"""
        while self.queue:
            item = self.queue.popleft()
            url = item[0] if isinstance(item, (list, tuple)) else item
            
            if self.tentativas.get(url, 0) < Config.MAX_RETRIES:
                return item if len(item) == 3 else (url, 0, calcular_prioridade(url, 0))
        
        return None
    
    def marcar_tentativa(self, url: str):
        """Incrementa contador de tentativas para uma URL"""
        self.tentativas[url] = self.tentativas.get(url, 0) + 1
    
    def marcar_processada(self, url: str):
        """Marca URL como processada"""
        self.processadas.add(url)
        if url in self.tentativas:
            del self.tentativas[url]
        self.salvar_estado()

async def salvar_arquivo_async(caminho: str, conteudo: str, modo: str = 'w'):
    """Salva arquivo de forma assíncrona"""
    async with aiofiles.open(caminho, mode=modo, encoding='utf-8' if modo == 'w' else None) as f:
        await f.write(conteudo)

def salvar_arquivos(metadata, content, caminho_raw, caminho_txt, caminho_links, caminho_meta):
    """Salva arquivos usando um ThreadPoolExecutor"""
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(lambda: open(caminho_raw, 'wb').write(content)),
            executor.submit(lambda: open(caminho_txt, 'w', encoding='utf-8').write(metadata.texto)),
            executor.submit(lambda: open(caminho_links, 'w', encoding='utf-8').write(
                json.dumps(metadata.links, ensure_ascii=False, indent=2))),
            executor.submit(lambda: open(caminho_meta, 'w', encoding='utf-8').write(
                json.dumps(metadata.__dict__, ensure_ascii=False, indent=2)))
        ]
        for future in futures:
            future.result()

def get_random_user_agent():
    """Retorna um User-Agent aleatório"""
    return random.choice(Config.USER_AGENTS)

def verificar_memoria():
    """Verifica o uso de memória"""
    try:
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        logging.info(f"Memória: {mem.percent}% usado | Swap: {swap.percent}% usado")
        
        if mem.percent > Config.MAX_MEMORY_PERCENT:
            logging.warning("Iniciando limpeza de memória...")
            gc.collect()
            
            # Força limpeza mais agressiva
            import sys
            sys.stderr.flush()
            sys.stdout.flush()
            
            # Limpa caches
            gc.collect(2)
            
            mem = psutil.virtual_memory()
            logging.info(f"Após limpeza - Memória: {mem.percent}% usado")
            
            if mem.percent > Config.MAX_MEMORY_PERCENT:
                return False
        
        return True
    except Exception as e:
        logging.error(f"Erro ao verificar memória: {e}")
        return False  

def sanitizar_texto(texto: str) -> str:
    """Remove caracteres inválidos e normaliza espaços"""
    # Remove caracteres de controle
    texto = ''.join(char for char in texto if ord(char) >= 32 or char in '\n\t')
    
    # Normaliza espaços e quebras de linha
    texto = re.sub(r'\s+', ' ', texto)
    texto = re.sub(r'\n\s*\n', '\n\n', texto)
    
    # Remove espaços no início e fim de cada linha
    texto = '\n'.join(line.strip() for line in texto.splitlines())
    
    return texto.strip()

def limpar_html(html: str) -> str:
    """Remove tags e scripts indesejados"""
    # Remove scripts e estilos
    html = re.sub(r'<script.*?</script>', '', html, flags=re.DOTALL)
    html = re.sub(r'<style.*?</style>', '', html, flags=re.DOTALL)
    
    # Remove comentários
    html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
    
    # Remove tags vazias
    html = re.sub(r'<[^>]*?/\s*>', '', html)
    
    return html

def normalizar_texto(texto: str) -> str:
    """Normaliza texto para processamento"""
    # Remove acentos
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    
    # Converte para minúsculo
    texto = texto.lower()
    
    # Remove caracteres especiais
    texto = re.sub(r'[^\w\s]', ' ', texto)
    
    # Normaliza espaços
    texto = re.sub(r'\s+', ' ', texto).strip()
    
    return texto

def normalizar_url(url: str) -> str:
    """Normaliza URLs para evitar duplicatas"""
    parsed = urlparse(url)
    path = re.sub(r'/+', '/', parsed.path)  # Remove barras duplicadas
    path = path.rstrip('/')  # Remove barra final
    
    # Remove fragmentos e parâmetros desnecessários
    query = '&'.join(sorted(
        param for param in parsed.query.split('&')
        if param and not param.startswith(('utm_', 'fb_', 'source='))
    ))
    
    return parsed._replace(
        path=path,
        query=query,
        fragment=''
    ).geturl()

def identificar_categoria(url: str) -> str:
    """Identifica a categoria do documento baseado na URL"""
    url_lower = url.lower()
    if "/holy-father/" in url_lower or "/papa/" in url_lower:
        return "papas"
    elif "/content/francesco/" in url_lower:
        return "francisco"
    elif "/content/benedict-xvi/" in url_lower:
        return "bento-xvi"
    elif "/content/john-paul-ii/" in url_lower:
        return "joao-paulo-ii"
    elif "/documents/" in url_lower:
        return "documentos"
    elif "/archive/ccc/" in url_lower or "catechism" in url_lower:
        return "catecismo"
    elif "/archive/cdc/" in url_lower or "canon-law" in url_lower:
        return "direito-canonico"
    elif "/roman_curia/" in url_lower or "/content/romancuria/" in url_lower:
        return "curia-romana"
    elif "/content/speeches/" in url_lower or "/content/messages/" in url_lower:
        return "discursos-mensagens"
    elif "/content/homilies/" in url_lower or "/content/angelus/" in url_lower:
        return "homilias-angelus"
    elif "/archive/" in url_lower:
        return "arquivo"
    else:
        return "outros"

def calcular_prioridade(url: str, depth: int) -> int:
    """Calcula prioridade da URL baseado em regras"""
    prioridade = 100
    
    # Penalidade pela profundidade
    prioridade -= depth * 10
    
    # Bônus para conteúdo em português
    if '/pt/' in url.lower():
        prioridade += 100
    elif '/pt-br/' in url.lower():
        prioridade += 90
    elif any(pattern in url.lower() for pattern in Config.PORTUGUESE_PATTERNS):
        prioridade += 60
    
    # Bônus para documentos importantes
    if any(pattern in url for pattern in Config.PRIORITY_PATTERNS):
        prioridade += 50
    
    # Bônus específicos por tipo de conteúdo
    if '/content/francesco/' in url:
        prioridade += 45
    elif '/documents/' in url:
        prioridade += 40
    elif '/content/speeches/' in url:
        prioridade += 35
    elif '/holy-father/' in url:
        prioridade += 30
    elif '/content/homilies/' in url:
        prioridade += 25
    elif '/archive/' in url:
        prioridade += 20
        
    return max(0, min(prioridade, 200))

def validar_html(html: str) -> Tuple[bool, List[str]]:
    """Valida estrutura do HTML"""
    erros = []
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Verifica tags não fechadas
        tags_abertas = []
        for tag in soup.find_all(True):
            if tag.name not in ['br', 'img', 'input', 'hr', 'meta', 'link']:
                if not tag.contents:
                    erros.append(f"Tag vazia encontrada: {tag.name}")
        
        # Verifica encoding
        if not soup.original_encoding:
            erros.append("Encoding não detectado")
        
        # Verifica estrutura básica
        if not soup.find('html'):
            erros.append("Tag <html> não encontrada")
        if not soup.find('body'):
            erros.append("Tag <body> não encontrada")
        
        return len(erros) == 0, erros
    except Exception as e:
        return False, [f"Erro ao validar HTML: {e}"]

def verificar_encoding(response) -> str:
    """Verifica e retorna o encoding correto"""
    # Tenta pegar do header Content-Type
    content_type = response.headers.get('content-type', '')
    match = re.search(r'charset=(\S+)', content_type)
    if match:
        return match.group(1)
    
    # Tenta detectar do conteúdo
    return response.apparent_encoding or 'utf-8'

def eh_tipo_valido(response):
    """Verifica se o tipo de conteúdo é HTML"""
    content_type = response.headers.get('content-type', '').lower()
    return 'text/html' in content_type

def calcular_relevancia_semantica(texto: str, palavras_chave: List[str]) -> float:
    """Calcula relevância semântica do texto"""
    texto_norm = normalizar_texto(texto)
    palavras_texto = set(texto_norm.split())
    palavras_chave_norm = {normalizar_texto(p) for p in palavras_chave}
    
    # Calcula intersecção
    matches = palavras_texto.intersection(palavras_chave_norm)
    
    # Calcula score baseado em TF-IDF simplificado
    score = len(matches) / len(palavras_chave_norm) if palavras_chave_norm else 0
    
    return min(1.0, score)

def identificar_idioma_texto(texto: str) -> str:
    """Identifica idioma do texto usando n-grams"""
    # Implementação simplificada - pode ser expandida com biblioteca como langdetect
    texto_norm = normalizar_texto(texto.lower())
    
    # Padrões comuns em português
    padroes_pt = ['ção', 'ções', 'mente', 'ante', 'ando', 'endo', 'indo']
    matches_pt = sum(1 for p in padroes_pt if p in texto_norm)
    
    return 'pt' if matches_pt >= 2 else 'outro'

def extrair_entidades(texto: str) -> Dict[str, List[str]]:
    """Extrai entidades nomeadas do texto"""
    entidades = {
        'pessoas': [],
        'lugares': [],
        'organizacoes': [],
        'datas': []
    }
    
    # Aqui você pode integrar com spaCy ou NLTK para melhor extração
    # Implementação básica com regex
    entidades['datas'] = re.findall(r'\d{1,2}/\d{1,2}/\d{4}', texto)
    
    return entidades

def eh_conteudo_importante(texto: str) -> bool:
    """Detecta se o conteúdo é importante baseado em palavras-chave e padrões"""
    texto_lower = texto.lower()
    
    # Padrões expandidos para capturar mais conteúdo relevante
    padroes_importantes = [
        r'carta\s+apost[óo]lica',
        r'exorta[çc][ãa]o\s+apost[óo]lica',
        r'constitui[çc][ãa]o\s+apost[óo]lica',
        r'enc[íi]clica',
        r'motu\s+proprio',
        r'bula\s+papal',
        r'homilia\s+do\s+papa',
        r'audiência\s+geral',
        r'angelus',
        r'regina\s+c[œoe]li',
        r'pontif[íi]c[ie]',
        r'magist[ée]rio',
        r'concílio',
        r'santo\s+padre',
        r'santa\s+s[ée]',
        r'congreg[aç][ãa]o',
        r'dicast[ée]rio'
    ]
    
    # Verifica presença de palavras-chave
    if any(palavra in texto_lower for palavra in Config.PALAVRAS_IMPORTANTES):
        return True
            
    # Verifica padrões
    if any(re.search(padrao, texto_lower) for padrao in padroes_importantes):
        return True
    
    # Verifica comprimento mínimo do texto
    if len(texto.split()) > 200:  # textos com mais de 200 palavras
        return True
    
    return False

def extrair_titulo(soup: BeautifulSoup) -> str:
    """Extrai título da página"""
    for selector in [
        lambda s: s.title,
        lambda s: s.find('meta', {'property': 'og:title'}),
        lambda s: s.find('h1'),
        lambda s: s.find('h2')
    ]:
        element = selector(soup)
        if element:
            if hasattr(element, 'get'):
                content = element.get('content')
                if content:
                    return content.strip()
            else:
                return element.get_text().strip()
    return ""

def extrair_descricao(soup: BeautifulSoup) -> str:
    """Extrai descrição da página"""
    for selector in [
        lambda s: s.find('meta', {'name': 'description'}),
        lambda s: s.find('meta', {'property': 'og:description'}),
        lambda s: s.find('div', {'class': 'description'}),
        lambda s: s.find('div', {'id': 'description'})
    ]:
        element = selector(soup)
        if element:
            if hasattr(element, 'get'):
                content = element.get('content')
                if content:
                    return content.strip()
            else:
                return element.get_text().strip()
    return ""

def extrair_palavras_chave(texto: str, max_palavras: int = 15) -> List[str]:
    """Extrai palavras-chave do texto"""
    palavras = re.findall(r'\b\w+\b', texto.lower())
    freq = {}
    stop_words = {'de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas', 
                 'por', 'para', 'com', 'e', 'ou', 'que', 'se', 'um', 'uma', 'os', 'as'}
    
    for palavra in palavras:
        if len(palavra) > 3 and palavra not in stop_words:
            freq[palavra] = freq.get(palavra, 0) + 1
            
    return sorted(freq.keys(), key=lambda x: freq[x], reverse=True)[:max_palavras]

@log_execution_time
def extrair_texto_principal(soup: BeautifulSoup) -> str:
    """Extrai o texto principal do documento"""
    # Remove elementos indesejados
    for element in soup(['script', 'style', 'nav', 'footer', 'iframe', 'header']):
        element.decompose()
    
    # Procura o conteúdo principal
    main_content = (
        soup.find('main') or 
        soup.find('div', {'id': 'content'}) or 
        soup.find('div', {'class': 'content'}) or 
        soup.find('article') or 
        soup
    )
    
    return main_content.get_text(separator='\n', strip=True)

def extrair_links(html_content: str, base_url: str) -> List[str]:
    """Extrai links relevantes da página"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        links = set()
        
        main_content = (
            soup.find('main') or 
            soup.find('div', {'id': 'content'}) or 
            soup.find('div', {'class': 'content'}) or 
            soup
        )
        
        for a in main_content.find_all('a', href=True):
            href = a['href']
            if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                continue
                
            try:
                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)
                
                if ('vatican.va' in parsed.netloc and 
                    parsed.scheme in ('http', 'https') and
                    not any(p in full_url for p in Config.IGNORE_PATTERNS)):
                    
                    normalized_url = normalizar_url(full_url)
                    if normalized_url:
                        links.add(normalized_url)
                        
            except Exception as e:
                logging.debug(f"Erro ao processar link {href}: {e}")
                continue
        
        return list(links)
    except Exception as e:
        logging.error(f"Erro ao extrair links: {e}")
        return []

class RetryManager:
    """Gerenciador de tentativas de reconexão"""
    def __init__(self, max_retries: int = 3, delay: int = 5):
        self.max_retries = max_retries
        self.delay = delay
        self.retry_count = 0
    
    def should_retry(self, exception: Exception) -> bool:
        """Verifica se deve tentar novamente"""
        if self.retry_count >= self.max_retries:
            return False
        
        retry_exceptions = (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.RequestException
        )
        
        if isinstance(exception, retry_exceptions):
            self.retry_count += 1
            time.sleep(self.delay * self.retry_count)
            return True
        
        return False
    
    def reset(self):
        """Reseta o contador de tentativas"""
        self.retry_count = 0

def conexao_resiliente(func):
    """Decorator para adicionar resiliência às conexões"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        retry_manager = RetryManager()
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if not retry_manager.should_retry(e):
                    raise
                logging.warning(f"Tentando reconexão... ({retry_manager.retry_count}/{retry_manager.max_retries})")
    return wrapper

def verificar_estado_periodico():
    """Verifica estado do crawler periodicamente"""
    while True:
        try:
            verificar_memoria()
            time.sleep(300)  # 5 minutos
        except Exception as e:
            logging.error(f"Erro na verificação periódica: {e}")

@log_execution_time
def baixar_e_processar(url: str, depth: int, stats: CrawlerStats) -> Optional[DocumentMetadata]:
    """Baixa e processa uma página"""
    try:
        # Configuração da sessão com retries e conexões persistentes
        session = requests.Session()
        session.headers.update({
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=30,
            pool_maxsize=30,
            pool_block=False
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        
        categoria = identificar_categoria(url)
        metadata = DocumentMetadata(url, categoria)
        
        nome_arquivo = hashlib.md5(url.encode()).hexdigest()
        caminho_raw = os.path.join(Config.RAW_DIR, f"{nome_arquivo}.html")
        caminho_txt = os.path.join(Config.TXT_DIR, f"{nome_arquivo}.txt")
        caminho_links = os.path.join(Config.LINKS_DIR, f"{nome_arquivo}.json")
        caminho_meta = os.path.join(Config.METADATA_DIR, f"{nome_arquivo}.json")
        
        with session.get(url, stream=True, timeout=Config.TIMEOUT) as response:
            response.raise_for_status()
            
            if not eh_tipo_valido(response):
                logging.warning(f"Tipo de conteúdo inválido para {url}")
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
            
            stats.bytes_baixados += total_size
            logging.info(f"Arquivo raw salvo: {caminho_raw}")
            
            # Processa o conteúdo
            html_content = content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extrai texto principal e metadados básicos
            metadata.texto = extrair_texto_principal(soup)
            metadata.tamanho_texto = len(metadata.texto)
            metadata.titulo = extrair_titulo(soup)
            metadata.descricao = extrair_descricao(soup)
            metadata.tamanho_html = total_size
            metadata.palavras_chave = extrair_palavras_chave(metadata.texto)
            metadata.eh_importante = eh_conteudo_importante(metadata.texto)
            metadata.prioridade = calcular_prioridade(url, depth)
            
            # Extrai e processa links
            links = extrair_links(html_content, url)
            metadata.links = links
            metadata.num_links = len(links)
            stats.links_encontrados += len(links)
            
            # Processa o texto com NLP
            nlp_processor = NLPProcessor()
            analise_nlp = nlp_processor.processar_documento(
                metadata.texto,
                doc_id=nome_arquivo
            )
            
            # Atualiza metadata com resultados NLP
            metadata.update({
                'resumo': analise_nlp.get('resumo', ''),
                'entidades': analise_nlp.get('entidades', {}),
                'temas': analise_nlp.get('temas', []),
                'sentimentos': analise_nlp.get('sentimentos', {}),
                'termos_doutrinais': analise_nlp.get('termos_doutrinais', {}),
                'estatisticas_linguisticas': analise_nlp.get('estatisticas', {})
            })
            
            # Identifica o tipo de documento baseado nos termos doutrinais
            if metadata.termos_doutrinais:
                categorias = list(metadata.termos_doutrinais.keys())
                metadata.tipo_documento = categorias[0] if categorias else "outro"
            
            # Indexa no Elasticsearch
            es = Elasticsearch([Config.ELASTICSEARCH_HOST])
            doc_id = hashlib.md5(url.encode()).hexdigest()
            es.index(
                index=Config.ES_INDEX_DOCS,
                id=doc_id,
                body=metadata.to_dict()
            )
            
            # Adiciona ao cache do Redis
            redis_client = redis.Redis(
                host=Config.REDIS_HOST,
                port=Config.REDIS_PORT,
                db=0
            )
            redis_client.setex(
                f"doc:{doc_id}",
                Config.CACHE_TIMEOUT,
                json.dumps(metadata.to_dict())
            )
            
            # Salva arquivos de forma assíncrona
            asyncio.run(salvar_arquivo_async(caminho_txt, metadata.texto))
            asyncio.run(salvar_arquivo_async(caminho_links, 
                json.dumps(metadata.links, ensure_ascii=False, indent=2)))
            asyncio.run(salvar_arquivo_async(caminho_meta, 
                json.dumps(metadata.to_dict(), ensure_ascii=False, indent=2)))
            
            return metadata
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao baixar {url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro ao processar {url}: {e}")
        return None

def main():
    """Função principal do programa"""
    # Configura logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Inicializa estatísticas
    stats = CrawlerStats()
    inicio = datetime.now()
    
    try:
        # Cria diretórios necessários
        os.makedirs(Config.RAW_DIR, exist_ok=True)
        os.makedirs(Config.TXT_DIR, exist_ok=True)
        os.makedirs(Config.LINKS_DIR, exist_ok=True)
        os.makedirs(Config.METADATA_DIR, exist_ok=True)
        
        # Inicializa conexões com ElasticSearch e Redis
        es = Elasticsearch([Config.ELASTICSEARCH_HOST])
        redis_client = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=0
        )
        
        # Cria índice no ElasticSearch se não existir
        if not es.indices.exists(index=Config.ES_INDEX_DOCS):
            es.indices.create(
                index=Config.ES_INDEX_DOCS,
                body=Config.ES_INDEX_MAPPING
            )
        
        # Inicia o crawler
        urls_iniciais = [
            "https://www.vatican.va/content/vatican/pt.html",
            "https://www.vatican.va/content/francesco/pt.html"
        ]
        
        for url in urls_iniciais:
            metadata = baixar_e_processar(url, depth=0, stats=stats)
            if metadata and metadata.links:
                # Indexa o documento no ElasticSearch
                doc_id = hashlib.md5(url.encode()).hexdigest()
                es.index(
                    index=Config.ES_INDEX_DOCS,
                    id=doc_id,
                    body=metadata.to_dict()
                )
                # Adiciona ao cache do Redis
                redis_client.setex(
                    f"doc:{doc_id}",
                    Config.CACHE_TIMEOUT,
                    json.dumps(metadata.to_dict())
                )
                
                # Processa links encontrados
                for link in metadata.links:
                    metadata_link = baixar_e_processar(link, depth=1, stats=stats)
                    if metadata_link:
                        link_id = hashlib.md5(link.encode()).hexdigest()
                        es.index(
                            index=Config.ES_INDEX_DOCS,
                            id=link_id,
                            body=metadata_link.to_dict()
                        )
        
        # Inicia a API
        api = VaticanAPI()
        api.iniciar()
        
    except KeyboardInterrupt:
        logging.info("\nInterrompido pelo usuário")
    except Exception as e:
        logging.error(f"Erro: {e}")
    finally:
        # Calcula tempo total
        tempo_total = datetime.now() - inicio
        horas = tempo_total.seconds // 3600
        minutos = (tempo_total.seconds % 3600) // 60
        
        # Exibe relatório
        logging.info("\n" + "="*70)
        logging.info("RELATÓRIO DETALHADO DO CRAWLER")
        logging.info("="*70)
        logging.info(f"Tempo de execução: {horas}h {minutos}m")
        logging.info(f"URLs processadas: {stats.urls_processadas}")
        if stats.urls_processadas > 0:
            taxa_sucesso = ((stats.urls_processadas - stats.urls_falhas) / 
                           stats.urls_processadas) * 100
            logging.info(f"Taxa de sucesso: {taxa_sucesso:.1f}%")
            
            velocidade = stats.bytes_baixados / (tempo_total.seconds or 1) / 1024
            logging.info(f"Velocidade média: {velocidade:.2f} KB/s")
            
            media_links = (stats.links_encontrados / 
                         stats.urls_processadas if stats.urls_processadas > 0 else 0)
            logging.info(f"Média de links por página: {media_links:.1f}")
        
        logging.info(f"Documentos importantes: {stats.docs_importantes}")
        logging.info(f"\nErros:")
        logging.info(f"  404: {stats.erros_404}")
        logging.info(f"  Timeout: {stats.erros_timeout}")
        logging.info(f"  Conexão: {stats.erros_conexao}")
        logging.info(f"  Rate Limit: {stats.erros_rate_limit}")
        
        # Lista as categorias mais comuns
        if stats.categorias:
            logging.info(f"\nTop Categorias:")
            for cat, count in sorted(stats.categorias.items(), 
                                   key=lambda x: x[1], reverse=True):
                logging.info(f"  {cat}: {count} documentos")
        logging.info("="*70)

if __name__ == "__main__":
    main()