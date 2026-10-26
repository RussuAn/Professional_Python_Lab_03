from contextlib import closing
from pathlib import Path
from stream_processor.readers import read_lines
from stream_processor.parsers import parse_rows, clean_rows, validate_students, RecordCounts
from stream_processor.filters import filter_students, normalize_names


def build_pipeline(path: Path, minimum: float = 0, group: str | None = None,
                   counts: RecordCounts | None = None):
    with closing(read_lines(path)) as lines:
        rows = parse_rows(lines)
        cleaned = clean_rows(rows)
        valid = validate_students(cleaned, counts)
        selected = filter_students(valid, minimum, group)
        yield from normalize_names(selected)


def read_eager(path: Path, minimum: float = 0, group: str | None = None,
               counts: RecordCounts | None = None):
    with closing(read_lines(path)) as source:
        lines = list(source)
    rows = list(parse_rows(lines))
    cleaned = list(clean_rows(rows))
    valid = list(validate_students(cleaned, counts))
    selected = list(filter_students(valid, minimum, group))
    return list(normalize_names(selected))
