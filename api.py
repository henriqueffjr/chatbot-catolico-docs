from fastapi import FastAPI, HTTPException
from typing import List, Optional
from pydantic import BaseModel
import hashlib
import json
import redis
from elasticsearch import Elasticsearch, exceptions as es_exceptions

from config import Config

app = FastAPI(
    title="Vatican Docs API",
    description="API para consulta de documentos católicos indexados pelo crawler",
    version="1.0.0"
)

# Conexões globais (simples - produção recomenda usar por request)
es = Elasticsearch([Config.ELASTICSEARCH_HOST])
redis_client = redis.Redis(
    host=Config.REDIS_HOST,
    port=Config.REDIS_PORT,
    db=0
)

class DocumentOut(BaseModel):
    url: str
    titulo: str
    texto: str
    categoria: str
    data_coleta: Optional[str] = None
    tamanho_html: Optional[int] = 0
    tamanho_texto: Optional[int] = 0
    num_links: Optional[int] = 0
    links: Optional[List[str]] = []
    palavras_chave: Optional[List[str]] = []
    eh_importante: Optional[bool] = False
    prioridade: Optional[int] = 0
    idioma: Optional[str] = ""
    entidades: Optional[dict] = {}
    tipo_documento: Optional[str] = ""
    resumo: Optional[str] = ""
    temas: Optional[list] = []
    sentimentos: Optional[dict] = {}
    termos_doutrinais: Optional[dict] = {}
    estatisticas_linguisticas: Optional[dict] = {}

def doc_id_from_url(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/doc/by-url/")
def get_doc_by_url(url: str):
    """Busca documento por URL (hash MD5 da URL)"""
    doc_id = doc_id_from_url(url)
    return get_doc(doc_id)

@app.get("/doc/{doc_id}", response_model=DocumentOut)
def get_doc(doc_id: str):
    """Busca documento pelo doc_id (md5 da url)"""
    # Primeiro tenta Redis (cache)
    redis_key = f"doc:{doc_id}"
    cached = redis_client.get(redis_key)
    if cached:
        return json.loads(cached)
    # Depois tenta Elasticsearch
    try:
        res = es.get(index=Config.ES_INDEX_DOCS, id=doc_id)
        doc = res["_source"]
        # Atualiza cache
        redis_client.setex(redis_key, Config.CACHE_TIMEOUT, json.dumps(doc))
        return doc
    except es_exceptions.NotFoundError:
        raise HTTPException(status_code=404, detail="Documento não encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/docs/search/", response_model=List[DocumentOut])
def search_docs(q: str, max_results: int = 10):
    """Busca documentos no Elasticsearch por texto livre"""
    try:
        resp = es.search(
            index=Config.ES_INDEX_DOCS,
            query={
                "multi_match": {
                    "query": q,
                    "fields": ["titulo^2", "texto", "resumo", "palavras_chave"]
                }
            },
            size=max_results
        )
        results = [hit["_source"] for hit in resp["hits"]["hits"]]
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/docs/recentes/", response_model=List[DocumentOut])
def docs_recentes(n: int = 5):
    """Lista últimos n documentos indexados"""
    try:
        resp = es.search(
            index=Config.ES_INDEX_DOCS,
            sort=[{"data_coleta": "desc"}],
            size=n
        )
        results = [hit["_source"] for hit in resp["hits"]["hits"]]
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Para rodar: uvicorn api:app --reload