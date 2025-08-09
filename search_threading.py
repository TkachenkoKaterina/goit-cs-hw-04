#!/usr/bin/env python3
"""
Багатопотоковий пошук ключових слів у .txt файлах каталогу.

Повертає dict: {keyword: [file_paths...]}, вимірює час.
— Безпечне оновлення спільної структури за допомогою Lock
— Обробка помилок читання/кодування
— Параметри CLI: шлях, слова, кількість потоків, маска файлів
"""

from __future__ import annotations
import argparse
import json
import threading
from pathlib import Path
from time import perf_counter
from typing import Dict, List, Iterable, Tuple

def chunked(seq: List[Path], n: int) -> Iterable[List[Path]]:
    if n <= 0:
        n = 1
    k = max(1, len(seq) // n + (1 if len(seq) % n else 0))
    for i in range(0, len(seq), k):
        yield seq[i:i+k]

def scan_files(files: List[Path], keywords: List[str]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {kw: [] for kw in keywords}
    lower_keywords = [kw.lower() for kw in keywords]
    for p in files:
        try:
            text = p.read_text(encoding="utf-8", errors="ignore").lower()
        except Exception:
            # пропускаємо файл, якщо не вдалось прочитати
            continue
        for kw, lkw in zip(keywords, lower_keywords):
            if lkw in text:
                out[kw].append(str(p))
    return out

def merge_into(dst: Dict[str, List[str]], src: Dict[str, List[str]]):
    for k, v in src.items():
        dst[k].extend(v)

def worker(files: List[Path], keywords: List[str], shared: Dict[str, List[str]], lock: threading.Lock):
    part = scan_files(files, keywords)
    with lock:
        merge_into(shared, part)

def main():
    ap = argparse.ArgumentParser(description="Threading: пошук ключових слів у текстових файлах")
    ap.add_argument("--path", required=True, help="Каталог з файлами")
    ap.add_argument("--keywords", nargs="+", required=True, help="Ключові слова для пошуку")
    ap.add_argument("--threads", type=int, default=4, help="К-сть потоків (за замовч. 4)")
    ap.add_argument("--glob", default="**/*.txt", help="Маска пошуку файлів (за замовч. **/*.txt)")
    args = ap.parse_args()

    root = Path(args.path)
    files = sorted(root.glob(args.glob))
    keywords = args.keywords

    # спільний результат + синхронізація
    shared: Dict[str, List[str]] = {kw: [] for kw in keywords}
    lock = threading.Lock()

    t0 = perf_counter()
    threads: List[threading.Thread] = []
    for chunk in chunked(files, args.threads):
        th = threading.Thread(target=worker, args=(chunk, keywords, shared, lock), daemon=True)
        th.start()
        threads.append(th)

    for th in threads:
        th.join()
    elapsed = perf_counter() - t0

    print(json.dumps({"results": shared, "elapsed_seconds": elapsed}, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()