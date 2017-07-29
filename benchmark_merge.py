#!/usr/bin/env python3

from os import getpid
import gzip
from random import random, seed

from helpers import Wrappers, generate_item


def main():
    seed()
    item_count = 50000
    name = 'data/merge.{pid}.txt.gz'.format(pid=getpid())
    with gzip.open(name, 'at') as f:
        iterations = 0
        while True:
            for item_count_range in [1000, 5000, 10000, 50000]:
                for key_length in [3, 4, 5, 6]:
                    for merge_count in range(1, 50):

                        items_list = []
                        for n in range(merge_count):
                            item_count = 10 + int(random() * item_count_range)
                            items_list.append([generate_item()[:key_length] for i in range(item_count)])

                        for W in Wrappers:

                            w = W()
                            w_name = w.name()
                            real_set = set()

                            for merge_count, items in enumerate(items_list):

                                w2 = W()
                                assert w2.name() == w_name
                                add_items = items[:1000] if w_name.startswith('hyperloglog:') else items
                                for item in add_items:
                                    w2.add(item)
                                    real_set.add(item)

                                w.merge(w2)

                                error = (w.count() - len(real_set)) / len(real_set)
                                f.write('{} {} {:.6f}\n'.format(w_name, merge_count, error))

                            f.flush()


            iterations += 1
            print(iterations)


if __name__ == '__main__':
    main()
