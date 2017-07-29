#!/usr/bin/env python3

from time import monotonic as monotime
import multiprocessing
from collections import Counter, defaultdict
import json
from random import choice, seed, random
from os import getpid, nice
from datetime import datetime
from zlib import compress, decompress
from pickle import dumps, loads


class HyperloglogWrapper:

    def __init__(self, precision=0.01):
        from hyperloglog import HyperLogLog
        self._prec = precision
        self._hll = HyperLogLog(precision)

    def add(self, item):
        self._hll.add(item)

    def count(self):
        return len(self._hll)

    def merge(self, other):
        assert isinstance(other, HyperloglogWrapper)
        self._hll.update(other._hll)

    def name(self):
        return 'hyperloglog:{}'.format(self._prec)

    def serialize(self):
        return compress(dumps(self._hll), 1)

    def deserialize(self, data):
        return loads(decompress(data))


class HLLWrapper:

    def __init__(self, precision=15):
        from HLL import HyperLogLog
        self._prec = precision
        self._hll = HyperLogLog(precision)

    def add(self, item):
        self._hll.add(item)

    def count(self):
        return int(round(self._hll.cardinality()))

    def merge(self, other):
        assert isinstance(other, HLLWrapper)
        self._hll.merge(other._hll)

    def name(self):
        return 'HLL:{}'.format(self._prec)

    def serialize(self):
        from pickle import dumps
        from zlib import compress
        return compress(dumps(self._hll), 1)

    def deserialize(self, data):
        return loads(decompress(data))


def get_hlpp(precision):
    from pyhllpp import HLL, HLL11, HLL12, HLL13, HLL14, HLL15, HLL16, HLL17, HLL18
    variants = {10: HLL, 11: HLL11, 12: HLL12, 13: HLL13, 14: HLL14, 15: HLL15,
                16: HLL16, 17: HLL17, 18: HLL18}
    return variants[precision]


class HLLPPWrapper:

    def __init__(self, precision=15):
        self._prec = precision
        self._hll = get_hlpp(precision)(1301)

    def add(self, item):
        self._hll.insert(item)

    def count(self):
        return self._hll.count()

    def merge(self, other):
        assert isinstance(other, HLLPPWrapper)
        self._hll.merge(other._hll)

    def name(self):
        return 'hllpp:{}'.format(self._prec)

    def serialize(self):
        from zlib import compress
        return compress(self._hll.serialize(), 1)

    def deserialize(self, data):
        return self._hll.deserialize(decompress(data))


Wrappers = [
    #lambda: HyperloglogWrapper(precision=0.1),
    #lambda: HyperloglogWrapper(precision=0.05),
    #lambda: HyperloglogWrapper(precision=0.03),
    lambda: HyperloglogWrapper(precision=0.01),
    #lambda: HLLWrapper(precision=10),
    #lambda: HLLWrapper(precision=11),
    lambda: HLLWrapper(precision=13),
    #lambda: HLLWrapper(precision=14),
    lambda: HLLWrapper(precision=15),
    #lambda: HLLPPWrapper(precision=10),
    #lambda: HLLPPWrapper(precision=11),
    lambda: HLLPPWrapper(precision=13),
    lambda: HLLPPWrapper(precision=14),
    lambda: HLLPPWrapper(precision=15),
    lambda: HLLPPWrapper(precision=18),
    lambda: HLLPPWrapper(precision=17),
]


_letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

def generate_item(length=6):
    return ''.join(choice(_letters) for i in range(length))


assert generate_item()


def benchmark(n=0, run_count=10, item_count=5000):
    t0 = monotime()
    seed()
    error_points = Counter()
    sample_sets = defaultdict(list)
    for run_number in range(run_count):
        print('.', end='', flush=True)

        real_set = set()
        w_objs = [W() for W in Wrappers]
        w_pairs = [(w_obj, w_obj.name()) for w_obj in w_objs]

        for item_number in range(item_count):
            item = generate_item()
            real_set.add(item)
            for w_obj, w_name in w_pairs:
                w_obj.add(item)

                error = (w_obj.count() - len(real_set)) / len(real_set)

                coordinates = (
                    w_name,
                    ((item_number + 1) // 10) * 10,
                    round(error * 1000) / 1000,
                )
                error_points[coordinates] += 1

            if random() < 0.001:
                real_set_data = compress(dumps(real_set), 1)
                for w_obj, w_name in w_pairs:
                    sample_sets[w_name].append((w_obj.serialize(), real_set_data))

        real_set_data = compress(dumps(real_set), 1)
        for w_obj, w_name in w_pairs:
            sample_sets[w_name].append((w_obj.serialize(), real_set_data))

    print('{} benchmark run {:3} done (pid {}) in {:.3f} s'.format(datetime.utcnow(), n, getpid(), monotime() - t0))
    return error_points, dict(sample_sets)


def main():

    all_error_points = Counter()
    all_sample_sets = defaultdict(list)

    nice(10)
    with multiprocessing.Pool() as pool:
        for error_points, sample_sets in pool.imap_unordered(benchmark, range(200)):
            all_error_points += error_points
            for w_name, items in sample_sets.items():
                all_sample_sets[w_name].extend(items)

    with open('errors.jsonl', 'a') as f:
        for (w_name, item_count, error), occurrence_count in all_error_points.items():
            row = {
                'add': {
                    'w_name': w_name,
                    'item_count': item_count,
                    'error': error,
                    'occurrences': occurrence_count,
                },
            }
            f.write(json.dumps(row, sort_keys=True) + '\n')

    for w_name, w_samples in sorted(all_sample_sets.items()):
        assert isinstance(w_samples, list)
        print('Test merging:', w_name, len(w_samples))
        for set_count in range(2, 51):
            for run_number in range(1000):
                print('-', set_count, run_number, end=' ', flush=True)
                real_set = set()
                w_objs = [W() for W in Wrappers]
                w_obj, = [x for x in w_objs if x.name() == w_name] # :)
                for set_number in range(set_count):
                    w_serialized, set_serialized = choice(w_samples)
                    part_obj = w_obj.deserialize(w_serialized)
                    part_set = loads(decompress(set_serialized))
                    assert part_obj is not w_obj
                    w_obj.merge(part_obj)
                    real_set.update(part_set)

                    est_count = w_obj.count()
                    error = (est_count - len(real_set)) / (len(real_set) or 1)
                    error = round(error * 1000) / 1000

                    row = {
                        'merge': {
                            'w_name': w_name,
                            'set_count': set_number + 1,
                            'error': error,
                            'occurrences': 1,
                        },
                    }
                    #print('>', row)
                    with open('errors.jsonl', 'a') as f:
                        f.write(json.dumps(row, sort_keys=True) + '\n')

                print(error)
