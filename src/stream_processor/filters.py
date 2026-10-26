from collections.abc import Iterable, Iterator
from stream_processor.parsers import Student


def filter_students(students: Iterable[Student], minimum: float = 0,
                    group: str | None = None) -> Iterator[Student]:
    for student in students:
        if student.average_grade >= minimum and (group is None or student.group == group):
            yield student


def normalize_names(students: Iterable[Student]) -> Iterator[Student]:
    for student in students:
        yield student._replace(first_name=student.first_name.title(),
                               last_name=student.last_name.title())


def search_students(students: Iterable[Student], student_id: int | None = None,
                    last_name: str | None = None) -> Iterator[Student]:
    for student in students:
        if student_id is not None and student.student_id != student_id:
            continue
        if last_name is not None and student.last_name.casefold() != last_name.casefold():
            continue
        yield student
