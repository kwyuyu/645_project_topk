from __future__ import annotations

import warnings

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


def main():
    DB = Database()
    DB.connect('localhost', 5432, 'postgres', 'postgres')

    driver = TopKInsight(DB)

    results = driver.insights('sales', 10, [2, 0, 1])
    display_results(results)

    DB.disconnect()


if __name__ == '__main__':
    main()