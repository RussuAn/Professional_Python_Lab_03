from contextlib import closing
from itertools import islice
from pathlib import Path
import pytest
from stream_processor.analytics import calculate_statistics, top_students
from stream_processor.filters import search_students
from stream_processor.iterators import BatchIterator, batched_students, infinite_ids
from stream_processor.parsers import RecordCounts, parse_rows, clean_rows, validate_students
from stream_processor.pipeline import build_pipeline, read_eager
from stream_processor.data import generate_file

SAMPLE = Path(__file__).resolve().parents[1] / 'data' / 'sample.csv'


def test_validation_and_statistics():
    counts = RecordCounts()
    result = calculate_statistics(build_pipeline(SAMPLE, counts=counts))
    assert (counts.total, counts.valid, counts.invalid) == (10, 6, 4)
    assert result['count'] == 6
    assert result['average'] == pytest.approx(508 / 6)
    assert (result['minimum'], result['maximum']) == (72, 95)
    assert result['groups'] == {'ФЕП-32': 2, 'ФЕП-31': 2, 'ФЕП-33': 2}


def test_filtering_normalization_and_top():
    students = list(build_pipeline(SAMPLE, 85, 'ФЕП-32'))
    assert [s.student_id for s in students] == [1, 3]
    assert students[0].first_name == 'Марія'
    assert [s.student_id for s in top_students(iter(students), 2)] == [3, 1]
    assert [s.student_id for s in build_pipeline(SAMPLE, 85, 'ФЕП-33')] == [5]
    assert top_students(iter(students), 0) == []


def test_search_is_lazy_and_case_insensitive():
    counts = RecordCounts()
    with closing(build_pipeline(SAMPLE, counts=counts)) as stream:
        found = next(search_students(stream, student_id=2), None)
    assert found.student_id == 2
    assert counts.total == 2
    assert [s.student_id for s in search_students(build_pipeline(SAMPLE), last_name='дОрОш')] == [1, 6]
    assert next(search_students(build_pipeline(SAMPLE), student_id=999), None) is None


def test_iterator_batches_and_exhaustion():
    iterator = BatchIterator(build_pipeline(SAMPLE), 4)
    assert iter(iterator) is iterator
    assert len(next(iterator)) == 4
    assert len(next(iterator)) == 2
    with pytest.raises(StopIteration):
        next(iterator)
    with pytest.raises(StopIteration):
        next(iterator)
    assert [len(b) for b in batched_students(build_pipeline(SAMPLE), 2)] == [2, 2, 2]
    with pytest.raises(ValueError):
        BatchIterator([], 0)
    assert list(islice(infinite_ids(), 3)) == [1, 2, 3]


def test_empty_and_missing_header(tmp_path):
    path = tmp_path / 'empty.csv'
    generate_file(path, 0)
    result = calculate_statistics(build_pipeline(path))
    assert result == {'count': 0, 'average': None, 'minimum': None, 'maximum': None, 'groups': {}}
    assert list(batched_students([], 2)) == []
    assert top_students([], 4) == []
    path.write_text('id,grade\n1,90\n', encoding='utf-8')
    with pytest.raises(ValueError):
        list(build_pipeline(path))


def test_csv_quotes_missing_extra_and_nonfinite():
    lines = ['student_id,first_name,last_name,group,average_grade\n',
             '1,"Марія, Анна",Дорош,ФЕП-32,100\n',
             '2,Остап,Мазур,ФЕП-32,0\n',
             '3,Остап,Мазур,ФЕП-32\n',
             '4,Остап,Мазур,ФЕП-32,80,extra\n',
             '5,Остап,Мазур,ФЕП-32,inf\n',
             '0,Остап,Мазур,ФЕП-32,80\n']
    counts = RecordCounts()
    result = list(validate_students(clean_rows(parse_rows(lines)), counts))
    assert [s.average_grade for s in result] == [100, 0]
    assert result[0].first_name == 'Марія, Анна'
    assert counts.invalid == 4


def test_file_closed_after_early_exit(monkeypatch):
    opened = []
    original = Path.open
    def tracked(path, *args, **kwargs):
        file = original(path, *args, **kwargs)
        opened.append(file)
        return file
    monkeypatch.setattr(Path, 'open', tracked)
    with closing(build_pipeline(SAMPLE)) as stream:
        assert next(stream).student_id == 1
        assert not opened[0].closed
    assert opened[0].closed


def test_eager_lazy_equivalence_and_dataset(tmp_path):
    path = tmp_path / 'students.csv'
    generate_file(path, 10_000)
    eager = calculate_statistics(read_eager(path, 80, 'ФЕП-32'))
    lazy = calculate_statistics(build_pipeline(path, 80, 'ФЕП-32'))
    assert eager == lazy
    assert calculate_statistics(build_pipeline(path))['count'] == 10_000
