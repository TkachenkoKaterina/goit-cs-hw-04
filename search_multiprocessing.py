#!/usr/bin/env python3
"""
Багатопроцесорний пошук ключових слів у .txt файлах каталогу.

Повертає dict: {keyword: [file_paths...]}, вимірює час.
— Використовує multiprocessing.Process + Queue для збору часткових результатів
— Без спільної пам'яті (щоб уникнути блокувань), потім мерджимо в головному процесі
— Обробка помилок читання/кодування
"""

from __future__ import annotations
import argparse
import json
from multiprocessing import Process, Queue, cpu_count
from pathlib import Path
from time import perf_counter
from typing import Dict, List, Iterable

def chunked(seq: List[Path], n_proc: int) -> Iterable[List[Path]]:
    if n_proc <= 0:
        n_proc = 1
    k = max(1, len(seq) // n_proc + (1 if len(seq) % n_proc else 0))
    for i in range(0, len(seq), k):
        yield seq[i:i+k]

def scan_files(files: List[Path], keywords: List[str]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {kw: [] for kw in keywords}
    lower_keywords = [kw.lower() for kw in keywords]
    for p in files:
        try:
            text = p.read_text(encoding="utf-8", errors="ignore").lower()
        except Exception:
            continue
        for kw, lkw in zip(keywords, lower_keywords):
            if lkw in text:
                out[kw].append(str(p))
    return out

def worker(files: List[Path], keywords: List[str], q: Queue):
    q.put(scan_files(files, keywords))

def merge(a: Dict[str, List[str]], b: Dict[str, List[str]]) -> Dict[str, List[str]]:
    for k, v in b.items():
        a.setdefault(k, []).extend(v)
    return a

def main():
    ap = argparse.ArgumentParser(description="Multiprocessing: пошук ключових слів у текстових файлах")
    ap.add_argument("--path", required=True, help="Каталог з файлами")
    ap.add_argument("--keywords", nargs="+", required=True, help="Ключові слова для пошуку")
    ap.add_argument("--procs", type=int, default=0, help="К-сть процесів (0 = кількість CPU)")
    ap.add_argument("--glob", default="**/*.txt", help="Маска пошуку файлів (за замовч. **/*.txt)")
    args = ap.parse_args()

    root = Path(args.path)
    files = sorted(root.glob(args.glob))
    keywords = args.keywords
    n_proc = args.procs or cpu_count()

    q: Queue = Queue()
    procs: List[Process] = []

    t0 = perf_counter()
    for chunk in chunked(files, n_proc):
        p = Process(target=worker, args=(chunk, keywords, q), daemon=True)
        p.start()
        procs.append(p)

    # зібрати результати
    result: Dict[str, List[str]] = {kw: [] for kw in keywords}
    for _ in procs:
        part = q.get()  # блокуюче очікування
        merge(result, part)

    for p in procs:
        p.join()
    elapsed = perf_counter() - t0

    print(json.dumps({"results": result, "elapsed_seconds": elapsed}, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()