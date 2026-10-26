from collections import Counter
from collections.abc import Iterable
from heapq import nlargest
from stream_processor.parsers import Student


def calculate_statistics(students: Iterable[Student]) -> dict:
    count = 0
    total = 0.0
    minimum = maximum = None
    groups = Counter()
    for student in students:
        grade = student.average_grade
        count += 1
        total += grade
        groups[student.group] += 1
        minimum = grade if minimum is None else min(minimum, grade)
        maximum = grade if maximum is None else max(maximum, grade)
    return {"count": count, "average": total / count if count else None,
            "minimum": minimum, "maximum": maximum, "groups": dict(groups)}


def top_students(students: Iterable[Student], limit: int) -> list[Student]:
    if limit < 0:
        raise ValueError("Кількість студентів не може бути від’ємною.")
    return nlargest(limit, students, key=lambda student: student.average_grade)
