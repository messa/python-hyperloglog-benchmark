#!/usr/bin/env python3

from helpers import Wrappers
from time import monotonic as monotime


def test_wrapper(W):
    x = W()
    print('-', x.name().ljust(18), end=' ')
    assert x.count() == 0
    t0 = monotime()
    add_count = 1000 * 1000
    for i in range(add_count):
        x.add(hex(i))
    t1 = monotime()
    assert x.count()
    xs = x.serialize()
    from zlib import decompress
    print('{}k additions took {:.3f} s;'.format(add_count // 1000, t1-t0), end=' ')
    print('difference: {:6} ({:4.2f} %)'.format(x.count() - add_count, 100 * abs(x.count() - add_count) / add_count), end=' ')
    print('serialized: {:6} B compressed, {:6} B uncompressed'.format(len(xs), len(decompress(xs))))


def main():
    for W in Wrappers:
        test_wrapper(W)


if __name__ == '__main__':
    main()
