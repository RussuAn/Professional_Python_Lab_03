import argparse
import csv
from itertools import count, islice
from pathlib import Path
from random import Random
from stream_processor.parsers import FIELDS


def generate_file(path: Path, size: int, seed: int = 1) -> None:
    if size < 0:
        raise ValueError("Кількість записів не може бути від’ємною.")
    random = Random(seed)
    names = ("Марія", "Данило", "Соломія", "Остап", "Олена", "Тарас")
    surnames = ("Дорош", "Левицький", "Климчук", "Мазур", "Романюк", "Дорош")
    groups = ("ФЕП-31", "ФЕП-32", "ФЕП-33", "ФЕП-34")
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(FIELDS)
        for number in islice(count(1), size):
            index = (number - 1) % len(names)
            writer.writerow((number, names[index], surnames[index],
                             groups[(number - 1) % len(groups)],
                             round(random.uniform(40, 100), 2)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, default=Path("data/students.csv"))
    parser.add_argument("--count", type=int, default=100_000)
    args = parser.parse_args()
    if args.count < 0:
        parser.error("Кількість записів не може бути від’ємною.")
    generate_file(args.path, args.count)
    print(f"Створено {args.count} записів у {args.path}")


if __name__ == "__main__":
    main()
