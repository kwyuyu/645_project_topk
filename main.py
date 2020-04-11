from DatabaseOperation import *
from Algorithm import *

def main():
    DB = Database()
    DB.connect('localhost', 5432, 'postgres', 'postgres')

    driver = TopKInsight(DB)
    result = driver.insghts(5, [3, 1, 0])
    print(result)

    DB.disconnnect()



if __name__ == '__main__':
    main()