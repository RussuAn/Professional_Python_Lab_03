from collections.abc import Iterable, Iterator
from itertools import count, islice
from stream_processor.parsers import Student


class BatchIterator:
    def __init__(self, students: Iterable[Student], size: int):
        if size <= 0:
            raise ValueError("Розмір пакета має бути додатним.")
        self.iterator = iter(students)
        self.size = size

    def __iter__(self):
        return self

    def __next__(self) -> list[Student]:
        batch = list(islice(self.iterator, self.size))
        if not batch:
            raise StopIteration
        return batch


def batched_students(students: Iterable[Student], size: int) -> Iterator[list[Student]]:
    yield from BatchIterator(students, size)


def infinite_ids() -> Iterator[int]:
    yield from count(1)
