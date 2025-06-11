import os

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