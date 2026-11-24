"""Text embedding service (document side and query side)."""
from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from collections import OrderedDict
from typing import Sequence

import numpy as np

log = logging.getLogger(__name__)

DEFAULTS = {
    "model": "BAAI/bge-large-en-v1.5",
    "backend": "sentence_transformers",
    "device": "cpu",
    "dimensions": 1024,
    "max_seq_length": 512,
    "batch_size": 16,
    "threads": 4,
    "cache_size": 50000,
    # Prompts are the ONLY model-specific setting. None = use the model family default below.
    "query_prompt": None,
    "document_prompt": None,
}

# Query / document prefixes each model family was trained with.
PROMPTS = {
    "bge":     ("Represent this sentence for searching relevant passages: ", ""),
    "qwen3":   ("Instruct: Given a user question about business records, retrieve the "
                "graph entities that answer it\nQuery:", ""),
    "gemma":   ("task: search result | query: ", "title: none | text: "),
    "minilm":  ("", ""),
}


def default_prompts(model: str) -> tuple[str, str]:
    m = model.lower()
    for family, prompts in PROMPTS.items():
        if family in m or (family == "qwen3" and "qwen" in m) or (family == "gemma" and "embeddinggemma" in m):
            return prompts
    return ("", "")


def build_embed_text(label: str, fields, get) -> str:
    """The ONE place embedding text is built (router and worker both call it)."""
    parts = []
    for f in fields:
        v = get(f)
        if isinstance(v, (list, tuple)):
            v = ", ".join(str(x) for x in v if x is not None)
        if v is not None and str(v).strip():
            parts.append(f"{f.replace('_', ' ')}: {str(v).strip()}")
    return f"{label}. " + ". ".join(parts) if parts else ""


_SINGLETON: "EmbeddingService | None" = None
_SINGLETON_LOCK = threading.Lock()


def embedding_config(loader=None) -> dict:
    """Merge defaults <- connections.yaml ``embedding:`` <- EMBEDDING_* env vars."""
    cfg = dict(DEFAULTS)
    if loader is not None:
        try:
            cfg.update(loader.connections.get("embedding") or {})
        except Exception:                                   # pragma: no cover
            pass
    env_map = {
        "EMBEDDING_MODEL": ("model", str),
        "EMBEDDING_BACKEND": ("backend", str),
        "EMBEDDING_DEVICE": ("device", str),
        "EMBEDDING_DIMENSIONS": ("dimensions", int),
        "EMBEDDING_MAX_SEQ_LENGTH": ("max_seq_length", int),
        "EMBEDDING_BATCH_SIZE": ("batch_size", int),
        "EMBEDDING_THREADS": ("threads", int),
    }
    for env, (key, cast) in env_map.items():
        if os.getenv(env):
            cfg[key] = cast(os.environ[env])
    return cfg


def model_tag(cfg: dict) -> str:
    """Written next to every vector so a later model/dimension change is detectable."""
    return f"{cfg['model']}@{int(cfg['dimensions'])}"


class EmbeddingService:
    def __init__(self, cfg: dict | None = None):
        self.cfg = {**DEFAULTS, **(cfg or {})}
        self.model_name = self.cfg["model"]
        self.backend = self.cfg["backend"]
        self.dimension = int(self.cfg["dimensions"])
        self.max_seq_length = int(self.cfg["max_seq_length"])
        self.batch_size = int(self.cfg["batch_size"])
        self.threads = int(self.cfg["threads"])
        self._max_chars = self.max_seq_length * 6
        self._cache: OrderedDict[str, np.ndarray] = OrderedDict()
        self._cache_size = int(self.cfg["cache_size"])
        q, d = default_prompts(self.model_name)
        self.query_prompt = self.cfg.get("query_prompt") if self.cfg.get("query_prompt") is not None else q
        self.document_prompt = self.cfg.get("document_prompt") if self.cfg.get("document_prompt") is not None else d
        self._lock = threading.Lock()

        t0 = time.time()
        log.info("Loading embedding model %s (backend=%s, threads=%d, max_seq_length=%d)",
                 self.model_name, self.backend, self.threads, self.max_seq_length)
        if self.backend == "sentence_transformers":
            self._load_st()
        elif self.backend == "fastembed":
            self._load_fastembed()
        else:
            raise ValueError(f"unknown embedding backend {self.backend!r}")

        probe = self._encode_raw(["dimension probe"])
        native = probe.shape[1]
        if self.dimension > native:
            raise ValueError(f"embedding.dimensions={self.dimension} but {self.model_name} "
                             f"only produces {native}")
        self.native_dimension = native
        log.info("Embedding model loaded in %.1fs: native_dim=%d, output_dim=%d",
                 time.time() - t0, native, self.dimension)


    def _load_st(self) -> None:
        import torch
        from sentence_transformers import SentenceTransformer
        device = str(self.cfg.get("device") or "cpu")
        kwargs = {}
        if device.startswith("cuda"):
            kwargs["model_kwargs"] = {"torch_dtype": torch.float16}
        else:
            torch.set_num_threads(self.threads)
        self._st = SentenceTransformer(
            self.model_name, device=device,
            cache_folder=os.getenv("EMBEDDING_CACHE_DIR") or None, **kwargs,
        )
        self._st.max_seq_length = self.max_seq_length

    def _load_fastembed(self) -> None:
        from fastembed import TextEmbedding
        self._fe = TextEmbedding(
            model_name=self.model_name, threads=self.threads,
            cache_dir=os.getenv("EMBEDDING_CACHE_DIR") or None,
        )

    def _encode_raw(self, texts: list[str]) -> np.ndarray:
        texts = [t[: self._max_chars] for t in texts]
        if self.backend == "sentence_transformers":
            out = self._st.encode(texts, batch_size=self.batch_size, show_progress_bar=False,
                                  normalize_embeddings=True, convert_to_numpy=True)
        else:
            out = np.vstack(list(self._fe.embed(texts, batch_size=self.batch_size)))
        return np.asarray(out, dtype=np.float32)

    def _finish(self, vecs: np.ndarray) -> np.ndarray:
        """MRL cut + re-normalise. Also rejects NaN/zero vectors loudly."""
        vecs = vecs[:, : self.dimension]
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        if not np.all(np.isfinite(vecs)) or np.any(norms == 0):
            raise ValueError("embedding model returned NaN/zero vectors")
        return vecs / norms


    def encode_documents(self, texts: Sequence[str]) -> list[list[float]]:
        """Embed stored records. No instruction prefix (Qwen3 convention)."""
        if not texts:
            return []
        keys = [hashlib.sha1(t.encode("utf-8")).hexdigest() for t in texts]
        result: dict[str, np.ndarray] = {}
        todo: dict[str, str] = {}
        with self._lock:
            for k, t in zip(keys, texts):
                hit = self._cache.get(k)
                if hit is not None:
                    self._cache.move_to_end(k)
                    result[k] = hit
                elif k not in todo:
                    todo[k] = t
        if todo:
            vecs = self._finish(self._encode_raw([self.document_prompt + t for t in todo.values()]))
            with self._lock:
                for k, v in zip(todo.keys(), vecs):
                    result[k] = v
                    self._cache[k] = v
                    if len(self._cache) > self._cache_size:
                        self._cache.popitem(last=False)
        return [result[k].tolist() for k in keys]

    def encode_query(self, text: str) -> list[float]:
        """Embed a search question. Use this on the retrieval side, never for records."""
        return self._finish(self._encode_raw([self.query_prompt + text]))[0].tolist()

    # Backwards-compatible names used by older scripts.
    def encode_batch(self, texts: Sequence[str]) -> list[list[float]]:
        return self.encode_documents(texts)

    def encode_one(self, text: str) -> list[float]:
        return self.encode_documents([text])[0]


def get_embedder(cfg: dict | None = None) -> EmbeddingService:
    """
    Process-wide singleton. Only the embedding worker should call this; the CDC consumer never loads the model.
    """
    global _SINGLETON
    with _SINGLETON_LOCK:
        if _SINGLETON is None:
            _SINGLETON = EmbeddingService(cfg or embedding_config())
        return _SINGLETON
