from __future__ import annotations

from DatabaseOperation import *
from Algorithm import *

def main():
    DB = Database()
    DB.connect('localhost', 5432, 'postgres', 'postgres')

    driver = TopKInsight(DB)

    result = driver.insghts('temp', 3, [0, 1, 2])
    print(result)

    DB.disconnect()


if __name__ == '__main__':
    main()