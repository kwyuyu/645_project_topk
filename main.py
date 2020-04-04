from DatabaseOperation import *
from Algorithm import *

def main():
    DB = Database()
    DB.connect('localhost', 5432, 'postgres', 'postgres')

    driver = TopKInsight(DB)

    DB.disconnnect()



if __name__ == '__main__':
    main()



