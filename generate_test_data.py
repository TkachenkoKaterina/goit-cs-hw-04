#!/usr/bin/env python3
"""
Генератор тестових .txt файлів для пошуку ключових слів.

Створює структуру папок і файлів з випадковим текстом та вкрапленнями
заданих keywords (різні регістри, повторення). Підійде під **/*.txt.

Приклад:
  python generate_test_data.py --out data --files 200 --subdirs 3 \
      --min-words 80 --max-words 300 --keywords квітка троянда love python
"""

from __future__ import annotations
import argparse
import random
from pathlib import Path
from typing import List

BASIC_VOCAB = """
сад місто небо ріка сонце вітер ліс камінь вода день ніч час людина книга музика
code data system thread process memory network service python java csharp golang rust
flower love rose tulip lily garden spring summer autumn winter happy joy bright light
""".split()

def random_text(num_words: int, vocab: List[str]) -> List[str]:
    return [random.choice(vocab) for _ in range(num_words)]

def sprinkle_keywords(words: List[str], keywords: List[str], density: float = 0.02):
    """
    Додає ключові слова у випадкові позиції.
    density ~ ймовірність вставки ключа в кожне слово (очікувано ~2%).
    Також варіюємо регістр: lower/upper/Title.
    """
    if not keywords or density <= 0:
        return words

    def mutate_case(w: str) -> str:
        r = random.random()
        if r < 0.7:
            return w.lower()
        elif r < 0.85:
            return w.upper()
        else:
            return w.title()

    i = 0
    out = []
    while i < len(words):
        out.append(words[i])
        if random.random() < density:
            kw = mutate_case(random.choice(keywords))
            out.append(kw)
            # інколи дублюємо ключ кілька разів поспіль
            if random.random() < 0.2:
                out.append(mutate_case(random.choice(keywords)))
        i += 1
    return out

def make_files(
    out_dir: Path,
    total_files: int,
    subdirs: int,
    min_words: int,
    max_words: int,
    keywords: List[str],
    seed: int | None,
):
    if seed is not None:
        random.seed(seed)

    out_dir.mkdir(parents=True, exist_ok=True)
    # створимо підпапки: shard_00, shard_01, ...
    shard_paths = [out_dir / f"shard_{i:02d}" for i in range(max(1, subdirs))]
    for sp in shard_paths:
        sp.mkdir(parents=True, exist_ok=True)

    for idx in range(total_files):
        shard = shard_paths[idx % len(shard_paths)]
        path = shard / f"doc_{idx:05d}.txt"

        n = random.randint(max(5, min_words), max(min_words, max_words))
        words = random_text(n, BASIC_VOCAB)
        # зробимо так, щоб частина файлів точно містила keywords
        if random.random() < 0.75:
            density = random.uniform(0.01, 0.05)  # 1..5% ключів
            words = sprinkle_keywords(words, keywords, density=density)

        text = " ".join(words)
        try:
            path.write_text(text, encoding="utf-8")
        except Exception as e:
            print(f"[WARN] Не вдалося записати {path}: {e}")

def parse_args():
    ap = argparse.ArgumentParser(description="Генератор .txt файлів з ключовими словами")
    ap.add_argument("--out", default="data", help="Каталог призначення (за замовч. data)")
    ap.add_argument("--files", type=int, default=200, help="Кількість файлів (за замовч. 200)")
    ap.add_argument("--subdirs", type=int, default=3, help="К-сть підпапок (за замовч. 3)")
    ap.add_argument("--min-words", type=int, default=80, help="Мін. слів у файлі (за замовч. 80)")
    ap.add_argument("--max-words", type=int, default=300, help="Макс. слів у файлі (за замовч. 300)")
    ap.add_argument("--keywords", nargs="+", default=["квітка", "троянда", "love", "python"],
                    help="Ключові слова для вкраплення")
    ap.add_argument("--seed", type=int, default=42, help="Seed для відтворюваності (або приберіть)")
    return ap.parse_args()

def main():
    args = parse_args()
    make_files(
        out_dir=Path(args.out),
        total_files=max(1, args.files),
        subdirs=max(1, args.subdirs),
        min_words=max(5, args.min_words),
        max_words=max(5, args.max_words),
        keywords=args.keywords,
        seed=args.seed,
    )
    print(f"✔ Згенеровано {args.files} файлів у '{args.out}' (підпапок: {args.subdirs}).")

if __name__ == "__main__":
    main()