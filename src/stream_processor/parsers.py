import csv
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from math import isfinite
from typing import NamedTuple

FIELDS = ("student_id", "first_name", "last_name", "group", "average_grade")


class Student(NamedTuple):
    student_id: int
    first_name: str
    last_name: str
    group: str
    average_grade: float


@dataclass
class RecordCounts:
    total: int = 0
    valid: int = 0
    invalid: int = 0


def parse_rows(lines: Iterable[str]) -> Iterator[dict]:
    reader = csv.DictReader(lines)
    if tuple(reader.fieldnames or ()) != FIELDS:
        raise ValueError("Неправильний заголовок CSV.")
    yield from reader


def clean_rows(rows: Iterable[dict]) -> Iterator[dict]:
    for row in rows:
        yield {key: value.strip() if isinstance(value, str) else value
               for key, value in row.items()}


def validate_students(rows: Iterable[dict],
                      counts: RecordCounts | None = None) -> Iterator[Student]:
    if counts is None:
        counts = RecordCounts()
    for row in rows:
        counts.total += 1
        try:
            if None in row:
                raise ValueError("Зайві поля")
            student = Student(int(row["student_id"]), row["first_name"],
                              row["last_name"], row["group"],
                              float(row["average_grade"]))
            if (student.student_id <= 0 or not student.first_name
                    or not student.last_name or not student.group
                    or not isfinite(student.average_grade)
                    or not 0 <= student.average_grade <= 100):
                raise ValueError("Некоректні дані")
        except (ValueError, TypeError, KeyError):
            counts.invalid += 1
            continue
        counts.valid += 1
        yield student
