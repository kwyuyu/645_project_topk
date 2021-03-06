from __future__ import annotations

import os
import warnings
import argparse
import logging
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


@title('\n####################\n  display_results\n####################\n rank - score - insightType - S - SG - Ce')
def display_all_results(results):
    for i, result in enumerate(results):
        print(i+1, result)
        logging.info(f'{i+1} {result}')


def main(args):
    if not os.path.exists("output"):
        os.makedirs("output")
    outdir = f'output/{args.table}_k{str(args.k)}_dim{"_".join([str(d) for d in args.insight_dim])}'
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    logging.basicConfig(filename=f'{outdir}/result.txt', level=logging.INFO)

    DB = Database()
    DB.connect('localhost', 5432, 'postgres', 'postgres')

    driver = TopKInsight(DB)

    results = driver.insights(args.table, args.k, args.insight_dim)
    display_all_results(results)

    logging.info(str((args.table, args.k, args.insight_dim)))

    # reproduce results
    for i, Ce in enumerate(results):
        phi = driver.reproduce_phi(Ce.SG, Ce)
        x, measure, x_name, others, Cei, title_name = TopKInsight.convert_result_to_drawing_input(Ce, phi)

        logging.info(f'{i+1}, {x}, {measure}, {x_name}, {others}, {Cei}, {title_name}')

        TopKInsight.draw_result(f'{outdir}/rank_{i+1}_result.png', x, measure, x_name, others, Cei, title_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', default='sales', type=str)
    parser.add_argument('--k', default=10, type=int, help='top K result')
    parser.add_argument('--insight_dim', default=[2,0,1],
                        help="[measure id (SHOULD BE last COLUMN of TABLE), D1 id, D2 id, ...]", nargs="+", type=int)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    # args.table = 's_q1'
    # args.k = 10
    # args.insight_dim = [2,1]

    main(args)
