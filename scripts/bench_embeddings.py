#!/usr/bin/env python3
"""Measure embedding latency, throughput and RAM on THIS machine."""
from __future__ import annotations

import argparse
import json
import os
import resource
import subprocess
import sys
import tempfile
import time

CONFIGS = {
    "minilm":    ("sentence_transformers", "sentence-transformers/all-MiniLM-L6-v2"),
    "bge-large": ("sentence_transformers", "BAAI/bge-large-en-v1.5"),
    "qwen-st":   ("sentence_transformers", "Qwen/Qwen3-Embedding-0.6B"),
    "qwen-fe":   ("fastembed", "Qwen/Qwen3-Embedding-0.6B"),
    "qwen-fe-q": ("fastembed", "Qwen/Qwen3-Embedding-0.6B-Q"),
}

# Shaped like the pipeline's real embed texts (see core.yaml `embed.fields`).
SHORT = [  # Customer / Event / Alert / Recommendation: ~15-40 tokens
    "Priya | Raman | Acme Retail | Chennai | India | lead | organic_search",
    "page_view | engagement | web | Pricing - Enterprise plans",
    "high | error_rate > 5% for 10m | Checkout API error rate is above threshold",
    "approved | Customer asked twice about delayed refund; route to billing queue",
] * 8
MEDIUM = [  # Ticket / Deal / Campaign: ~120-200 tokens
    ("Unable to log in after password reset | After resetting my password yesterday the "
     "portal keeps returning me to the sign-in page. I cleared cookies, tried Chrome and "
     "Firefox, and asked a colleague to try from another network with the same result. "
     "We have a payroll run tomorrow and three people on my team are blocked. | open | "
     "high | authentication | User cannot sign in after a password reset; likely a session "
     "cookie or SSO redirect issue affecting several users in one tenant.")
] * 16
LONG = [  # KnowledgeArticle content: truncated at max_seq_length
    ("How to configure SLA policies | " + ("An SLA policy defines first-response and "
     "resolution targets per priority. Targets pause outside business hours when the "
     "calendar is attached, and breaches raise an escalation to the queue owner. ") * 20
     + " | ticketing")
] * 8


def worker(name, threads, batch, repeats, max_seq, out_npy):
    os.environ["OMP_NUM_THREADS"] = str(threads)
    os.environ["MKL_NUM_THREADS"] = str(threads)
    import numpy as np
    backend, model = CONFIGS[name]
    t0 = time.time()
    if backend == "sentence_transformers":
        import torch
        from sentence_transformers import SentenceTransformer
        torch.set_num_threads(threads)
        m = SentenceTransformer(model, device="cpu")
        m.max_seq_length = max_seq
        enc = lambda xs: m.encode(xs, batch_size=batch, normalize_embeddings=True,
                                  show_progress_bar=False, convert_to_numpy=True)
        tok = m.tokenizer
    else:
        from fastembed import TextEmbedding
        m = TextEmbedding(model_name=model, threads=threads)
        enc = lambda xs: np.vstack(list(m.embed(xs, batch_size=batch)))
        tok = None
    load_s = time.time() - t0
    res = {"config": name, "threads": threads, "batch": batch, "load_s": round(load_s, 1)}
    enc(SHORT[:2])                                   # warm-up
    for label, texts in (("short", SHORT), ("medium", MEDIUM), ("long", LONG)):
        ntok = (sum(len(tok(t, truncation=True, max_length=max_seq)["input_ids"]) for t in texts)
                if tok else None)
        best = 1e9
        for _ in range(repeats):
            t = time.time(); v = enc(texts); best = min(best, time.time() - t)
        res[label] = {"ms_per_text": round(1000 * best / len(texts), 1),
                      "texts_per_s": round(len(texts) / best, 1),
                      "tokens_per_s": round(ntok / best) if ntok else None}
        if label == "short":
            np.save(out_npy, np.asarray(v, dtype="float32"))
    t = time.time(); enc([SHORT[0]]); res["single_query_ms"] = round(1000 * (time.time() - t), 1)
    res["peak_rss_gb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 ** 2, 2)
    res["dim"] = int(np.asarray(v).shape[1])
    print("RESULT " + json.dumps(res))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--configs", default="minilm,bge-large,qwen-st")
    ap.add_argument("--threads", default="1,4")
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--max-seq", type=int, default=512)
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--_worker")
    ap.add_argument("--_npy")
    a = ap.parse_args()
    repeats = 1 if a.quick else 3
    if a._worker:
        worker(a._worker, int(a.threads), a.batch, repeats, a.max_seq, a._npy)
        return

    tmp = tempfile.mkdtemp()
    results, vecs = [], {}
    for name in a.configs.split(","):
        for th in [int(x) for x in a.threads.split(",")]:
            npy = os.path.join(tmp, f"{name}_{th}.npy")
            cmd = [sys.executable, __file__, "--_worker", name, "--threads", str(th),
                   "--batch", str(a.batch), "--max-seq", str(a.max_seq), "--_npy", npy]
            if a.quick:
                cmd.append("--quick")
            print(f"... {name} threads={th}", flush=True)
            p = subprocess.run(cmd, capture_output=True, text=True)
            line = next((l for l in p.stdout.splitlines() if l.startswith("RESULT ")), None)
            if not line:
                print(f"    FAILED:\n{p.stderr[-1500:]}")
                continue
            r = json.loads(line[7:]); results.append(r); vecs[name] = npy
            print(f"    load={r['load_s']}s ram={r['peak_rss_gb']}GB dim={r['dim']} "
                  f"short={r['short']['ms_per_text']}ms medium={r['medium']['ms_per_text']}ms "
                  f"long={r['long']['ms_per_text']}ms query={r['single_query_ms']}ms")

    print("\n| config | threads | RAM GB | short ms/text | medium ms/text | long ms/text | "
          "1 query ms | hours per 1M short texts |")
    print("|---|---|---|---|---|---|---|---|")
    for r in results:
        hrs = 1e6 / r["short"]["texts_per_s"] / 3600
        print(f"| {r['config']} | {r['threads']} | {r['peak_rss_gb']} | {r['short']['ms_per_text']} | "
              f"{r['medium']['ms_per_text']} | {r['long']['ms_per_text']} | "
              f"{r['single_query_ms']} | {hrs:.1f} |")

    import numpy as np
    if "qwen-st" in vecs:
        ref = np.load(vecs["qwen-st"])
        for other in ("qwen-fe", "qwen-fe-q"):
            if other in vecs:
                o = np.load(vecs[other])
                cos = (ref * o).sum(1) / np.linalg.norm(ref, axis=1) / np.linalg.norm(o, axis=1)
                print(f"\nagreement qwen-st vs {other}: mean cosine={cos.mean():.4f} "
                      f"min={cos.min():.4f}  (>0.99 good, <0.97 investigate)")


if __name__ == "__main__":
    main()
