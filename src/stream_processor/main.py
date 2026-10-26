import argparse
from contextlib import closing
from itertools import islice, pairwise
from math import isfinite
from pathlib import Path
from stream_processor.analytics import calculate_statistics, top_students
from stream_processor.filters import search_students
from stream_processor.iterators import BatchIterator, batched_students, infinite_ids
from stream_processor.parsers import RecordCounts
from stream_processor.pipeline import build_pipeline


def print_students(students):
    print(f"{'ID':<8} {'Прізвище та ім’я':<25} {'Група':<10} {'Бал':>7}")
    print('-' * 53)
    found = False
    for student in students:
        name = f'{student.last_name} {student.first_name}'
        print(f'{student.student_id:<8} {name:<25} {student.group:<10} {student.average_grade:>7.2f}')
        found = True
    if not found:
        print('Записів немає')


def main():
    parser = argparse.ArgumentParser(description='Потокова обробка даних студентів')
    parser.add_argument('--path', type=Path, default=Path('data/students.csv'))
    parser.add_argument('--minimum', type=float, default=85)
    parser.add_argument('--group')
    parser.add_argument('--limit', type=int, default=5)
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--student-id', type=int, default=2)
    parser.add_argument('--last-name', default='Дорош')
    args = parser.parse_args()
    if not isfinite(args.minimum) or not 0 <= args.minimum <= 100:
        parser.error('Мінімальний бал має належати діапазону 0–100.')
    if args.limit <= 0 or args.batch_size <= 0:
        parser.error('Кількість результатів і розмір пакета мають бути додатними.')

    counts = RecordCounts()
    statistics = calculate_statistics(build_pipeline(args.path, counts=counts))
    print('ПЕРЕВІРКА ВХІДНИХ ДАНИХ')
    print(f'Усього записів: {counts.total}; коректних: {counts.valid}; некоректних: {counts.invalid}')
    for label, key in [('Середній бал', 'average'), ('Мінімальний бал', 'minimum'), ('Максимальний бал', 'maximum')]:
        value = statistics[key]
        print(f'{label}: {value:.4f}' if value is not None else f'{label}: немає даних')
    print('Кількість за групами:', statistics['groups'])

    print(f'\nПЕРШІ {args.limit} ЗАПИСІВ ЗА УМОВОЮ: бал >= {args.minimum}; група {args.group or "усі"}')
    with closing(build_pipeline(args.path, args.minimum, args.group)) as stream:
        print_students(islice(stream, args.limit))
    selected = calculate_statistics(build_pipeline(args.path, args.minimum, args.group))
    print('Відібрано:', selected['count'])

    print(f'\nНАЙКРАЩІ {args.limit} СТУДЕНТІВ ЗА ЦІЄЮ УМОВОЮ')
    print_students(top_students(build_pipeline(args.path, args.minimum, args.group), args.limit))

    search_counts = RecordCounts()
    print(f'\nПОШУК ID {args.student_id}')
    with closing(build_pipeline(args.path, counts=search_counts)) as stream:
        found = next(search_students(stream, student_id=args.student_id), None)
    print_students([] if found is None else [found])
    print('Прочитано записів до результату:', search_counts.total)
    print(f'\nПОШУК ПРІЗВИЩА {args.last_name}, до {args.limit} результатів')
    with closing(build_pipeline(args.path)) as stream:
        print_students(islice(search_students(stream, last_name=args.last_name), args.limit))

    sizes = (len(batch) for batch in batched_students(build_pipeline(args.path), args.batch_size))
    batch_count = record_count = last_size = 0
    for last_size in sizes:
        batch_count += 1
        record_count += last_size
    print(f'\nПАКЕТИ: {batch_count}; записів: {record_count}; останній пакет: {last_size}')
    with closing(build_pipeline(args.path)) as stream:
        iterator = BatchIterator(stream, args.batch_size)
        print('iter(iterator) is iterator:', iter(iterator) is iterator)
        print('Розмір першого пакета через next():', len(next(iterator, [])))
        print('Решта записів:', sum(len(batch) for batch in iterator))
        print('Після вичерпання next(iterator, None):', next(iterator, None))
    print('Перші 5 ID нескінченного потоку:', list(islice(infinite_ids(), 5)))
    with closing(build_pipeline(args.path)) as stream:
        grades = (student.average_grade for student in islice(stream, 5))
        print('Різниці сусідніх балів:', [round(b - a, 2) for a, b in pairwise(grades)])


if __name__ == '__main__':
    main()
