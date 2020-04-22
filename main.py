from __future__ import annotations

import warnings
import argparse

from DatabaseOperation import *
from Algorithm import *


warnings.filterwarnings('ignore')


def title(text):
    def decorator(func):
        import functools
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            print(text)
            return func(*args, **kwargs)
        return wrapper
    return decorator


@title('rank - score - insightType - S - SG - Ce')
def display_results(results):
    for i, result in enumerate(results):
        print(i+1, result)


def main(args):
    DB = Database()
    DB.connect('localhost', 5432, 'postgres', 'postgres')

    driver = TopKInsight(DB)

    results = driver.insights(args.table, args.k, args.insight_dim, args.verbose)
    display_results(results)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', default='sales', type=str)
    parser.add_argument('--k', default=10, type=int, help='top K result')
    parser.add_argument('--insight_dim', default=[2,0,1], nargs="+", type=int)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    args.table = 'paper_score_1000'
    args.k = 5
    args.insight_dim = [0, 1]
    args.verbose = False

    main(args)
