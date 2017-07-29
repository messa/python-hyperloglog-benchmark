#!/usr/bin/env python3

from os import getpid
import gzip

from helpers import Wrappers, generate_item


def main():
    item_count = 100000
    name = 'data/add.{pid}.txt.gz'.format(pid=getpid())
    with gzip.open(name, 'at') as f:
        iterations = 0
        while True:
            for key_length in [4, 5, 6]:
                items = [generate_item()[:key_length] for i in range(item_count)]
                for W in Wrappers:
                    w = W()
                    w_name = w.name()
                    s = set()
                    add_items = items[:1000] if w_name.startswith('hyperloglog:') else items
                    for n, item in enumerate(add_items):
                        w.add(item)
                        s.add(item)
                        if n % 10 == 0:
                            error = (w.count() - len(s)) / len(s)
                            f.write('{} {} {:.3f}\n'.format(w_name, n+1, error))
                    f.flush()
                iterations += 1
                print(iterations, flush=True)


if __name__ == '__main__':
    main()
