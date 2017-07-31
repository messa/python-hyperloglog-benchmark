#!/usr/bin/env python3

import argparse
from collections import defaultdict, Counter
from math import log, log2
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--debug', action='store_true')
    args = p.parse_args()

    data = load(args.debug)

    for algo_name, algo_data in sorted(data.items()):
        print('Graphing {}'.format(algo_name))

        points = [(occurrence_count, item_count, error) for (item_count, error), occurrence_count in algo_data.items()]
        points.sort(reverse=True)

        x, y, s, c = [], [], [], []
        for occurrence_count, item_count, error in points:
            x.append(item_count)
            y.append(error)
            s.append(occurrence_count ** (1/3))
            c.append(int(log2(occurrence_count)))

        plt.rcParams["figure.figsize"] = [16, 8]
        plt.clf()
        plt.axhline(0, color='k', alpha=.2)
        plt.axhline(0.01, alpha=.2)
        plt.axhline(-0.01, alpha=.2)
        plt.scatter(x, y, s=s, c=c)

        filename = 'merge.{}.png'.format(algo_name)
        plt.savefig(filename)


def load(debug):
    # data: {algorithm: Counter({ (item_count, error): occurrence_count })  }
    data = defaultdict(Counter)
    for n, line in enumerate(sys.stdin):
        if debug and n > 1000000:
            break
        occurrence_count, algorithm, merge_count, error = line.split()

        scaledown_y = 300

        key = (
            merge_count,
            round(float(error) * scaledown_y) / scaledown_y,
        )
        data[algorithm][key] += int(occurrence_count)

    return data


if __name__ == '__main__':
    main()
