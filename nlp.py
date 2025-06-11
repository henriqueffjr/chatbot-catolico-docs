import spacy
import logging
from collections import Counter
from typing import Dict, List

class NLPProcessor:
    def __init__(self):
        """Inicializa o processador de linguagem natural"""
        try:
            self.nlp = spacy.load("pt_core_news_lg")
        except OSError:
            logging.warning("Modelo spaCy pt_core_news_lg não encontrado. Baixando...")
            import os
            os.system("python -m spacy download pt_core_news_lg")
            self.nlp = spacy.load("pt_core_news_lg")
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
            if doc_id:
                self.cache[doc_id] = analise
            return analise
        except Exception as e:
            logging.error(f"Erro ao processar documento NLP: {e}")
            return {}

    def gerar_resumo(self, doc) -> str:
        """Gera um resumo do texto usando as frases mais relevantes"""
        frases = list(doc.sents)
        if not frases:
            return ""
        pontuacoes = []
        palavras_doc = set(token.text.lower() for token in doc if not token.is_stop)
        for i, frase in enumerate(frases):
            pontos = 0
            palavras_frase = set(token.text.lower() for token in frase if not token.is_stop)
            pontos += len(palavras_frase & palavras_doc) * 0.3
            if i < len(frases) * 0.2:
                pontos += 2
            elif i > len(frases) * 0.8:
                pontos += 1
            pontuacoes.append((pontos, frase.text))
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
        for key in entidades:
            entidades[key] = list(set(entidades[key]))[:10]
        return entidades

    def identificar_temas(self, doc) -> List[Dict[str, float]]:
        """Identifica os principais temas do documento usando frequência e relevância"""
        palavras = Counter()
        for token in doc:
            if (not token.is_stop and not token.is_punct and token.pos_ in ['NOUN', 'PROPN', 'ADJ']):
                palavras[token.lemma_] += 1
        total_palavras = sum(palavras.values())
        temas = []
        for palavra, freq in palavras.most_common(10):
            relevancia = (freq / total_palavras) if total_palavras else 0
            temas.append({'tema': palavra, 'relevancia': round(relevancia, 3)})
        return temas

    def analisar_sentimentos(self, doc) -> Dict[str, float]:
        """Analisa o sentimento do texto usando léxico básico de sentimentos"""
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
            encontrados = [termo for termo in termos if termo in texto_lower]
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