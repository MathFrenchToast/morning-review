from __future__ import annotations

import hashlib
import logging
import math
import re
from pathlib import Path

import numpy as np

from morning_review.core.config import settings

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service d'embeddings CPU léger (FastEmbed ONNX 384d avec chargement lazy).
    Bascule automatiquement sur un hachage vectoriel déterministe 384d si le modèle
    ONNX n'est pas encore téléchargé ou en mode test hors-ligne.
    """

    def __init__(self) -> None:
        self._model = None
        self._load_attempted = False
        self.dim = settings.embedding_dimensions
        self.model_version = settings.embedding_model_name

    def _get_model(self):
        if self._load_attempted:
            return self._model
        self._load_attempted = True
        try:
            from fastembed import TextEmbedding

            cache_dir = Path(settings.fastembed_cache_path)
            cache_dir.mkdir(parents=True, exist_ok=True)
            self._model = TextEmbedding(
                model_name=settings.embedding_model_name,
                cache_dir=str(cache_dir),
                threads=settings.omp_num_threads,
            )
            logger.info("FastEmbed ONNX model loaded: %s", settings.embedding_model_name)
        except Exception as exc:
            logger.warning(
                "FastEmbed ONNX unavailable (%s), using deterministic lexical-hash 384d fallback.",
                exc,
            )
            self._model = None
        return self._model

    def _deterministic_fallback_embed(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        tokens = re.findall(r"[a-zA-ZÀ-ÿ0-9]{3,}", (text or "").lower())
        if not tokens:
            vec[0] = 1.0
            return vec
        for tok in tokens:
            # Unigram + character 4-gram projection for multilingual fuzzy similarity
            features = [tok] + [tok[i : i + 4] for i in range(max(0, len(tok) - 3))]
            for feat in features:
                digest = hashlib.blake2b(feat.encode("utf-8"), digest_size=8).digest()
                idx = int.from_bytes(digest[:4], "little") % self.dim
                sign = 1.0 if (digest[4] & 1) == 0 else -1.0
                vec[idx] += sign
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [round(v / norm, 6) for v in vec]

    def embed_one(self, text: str) -> list[float]:
        return self.embed_batch([text])[0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        cleaned = [(t or "").strip()[:4000] or "empty" for t in texts]
        model = self._get_model()
        if model is not None:
            try:
                raw_vecs = list(model.embed(cleaned))
                results: list[list[float]] = []
                for arr in raw_vecs:
                    np_arr = np.asarray(arr, dtype=np.float32)
                    norm = float(np.linalg.norm(np_arr)) or 1.0
                    results.append((np_arr / norm).astype(float).tolist())
                return results
            except Exception as exc:
                logger.warning("FastEmbed inference fallback: %s", exc)
        return [self._deterministic_fallback_embed(t) for t in cleaned]


embedding_service = EmbeddingService()
