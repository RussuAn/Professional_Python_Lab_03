import argparse
import gc
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
import tracemalloc
from stream_processor.analytics import calculate_statistics
from stream_processor.data import generate_file
from stream_processor.filters import search_students
from stream_processor.parsers import RecordCounts
from stream_processor.pipeline import build_pipeline, read_eager


def measure(function, *args):
    gc.collect()
    start = perf_counter()
    result = function(*args)
    elapsed = perf_counter() - start
    gc.collect()
    tracemalloc.start()
    try:
        checked = function(*args)
        peak = tracemalloc.get_traced_memory()[1] / (1024 * 1024)
    finally:
        tracemalloc.stop()
    assert checked == result
    return result, elapsed, peak


def aggregate_file(path, eager):
    students = read_eager(path, 85) if eager else build_pipeline(path, 85)
    return calculate_statistics(students)


def first_result(path, eager):
    start = perf_counter()
    if eager:
        students = read_eager(path, 85)
        result = next(iter(students), None)
    else:
        with closing(build_pipeline(path, 85)) as students:
            result = next(students, None)
    return result, perf_counter() - start


def early_search(path, eager):
    counts = RecordCounts()
    if eager:
        students = read_eager(path, counts=counts)
        selected = list(search_students(students, student_id=2))
        result = next(iter(selected), None)
    else:
        with closing(build_pipeline(path, counts=counts)) as students:
            result = next(search_students(students, student_id=2), None)
    return result, counts.total


def main():
    parser = argparse.ArgumentParser(description='Порівняння eager та lazy')
    parser.add_argument('--sizes', type=int, nargs='+', default=[10_000, 100_000, 500_000])
    parser.add_argument('--early-size', type=int, default=1_000_000)
    args = parser.parse_args()
    if min(args.sizes) <= 0 or args.early_size < 2:
        parser.error('Розміри мають бути додатними, early-size — не менше 2.')
    print('Час і пам’ять вимірюються окремими запусками. Умова: бал >= 85.')
    print(f"{'Записів':>9} {'Eager, с':>11} {'Lazy, с':>11} {'Eager, MiB':>12} {'Lazy, MiB':>11} {'Перший E, с':>13} {'Перший L, с':>13}")
    with TemporaryDirectory() as folder:
        path = Path(folder) / 'students.csv'
        for size in args.sizes:
            generate_file(path, size)
            eager, eager_time, eager_peak = measure(aggregate_file, path, True)
            lazy, lazy_time, lazy_peak = measure(aggregate_file, path, False)
            assert eager == lazy
            first_e, time_e = first_result(path, True)
            first_l, time_l = first_result(path, False)
            assert first_e == first_l
            print(f'{size:>9} {eager_time:>11.4f} {lazy_time:>11.4f} {eager_peak:>12.4f} {lazy_peak:>11.4f} {time_e:>13.6f} {time_l:>13.6f}', flush=True)
        print(f'\nДОСТРОКОВИЙ ПОШУК ID 2 У {args.early_size} ЗАПИСАХ', flush=True)
        generate_file(path, args.early_size)
        eager, time_e, peak_e = measure(early_search, path, True)
        lazy, time_l, peak_l = measure(early_search, path, False)
        assert eager[0] == lazy[0]
        print(f"{'Спосіб':<10} {'Час, с':>12} {'Пік, MiB':>14} {'Прочитано':>12}")
        print(f'Eager      {time_e:>12.6f} {peak_e:>14.4f} {eager[1]:>12}')
        print(f'Lazy       {time_l:>12.6f} {peak_l:>14.4f} {lazy[1]:>12}')
        print('Результати eager і lazy збігаються.')


if __name__ == '__main__':
    main()
