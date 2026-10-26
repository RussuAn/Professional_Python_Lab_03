from collections.abc import Iterator
from pathlib import Path


def read_lines(path: Path) -> Iterator[str]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        yield from file
