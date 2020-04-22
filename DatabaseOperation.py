import psycopg2


class Database(object):
    def __init__(self):
        self.conn = None
        self.curs = None

    def connect(self, host, port, user, database):
        self.conn = psycopg2.connect(host=host, port=port, database=database, user=user)
        self.curs = self.conn.cursor()

    def execute(self, query):
        """

        :param query:
        :type query: str
        :return:
        :rtype: list of tuples
        """
        self.curs.execute(query)
        self.conn.commit()
        if query.split(' ')[0] != 'select':
            return []
        return [data for data in self.curs.fetchall()]

    def disconnect(self):
        self.curs.close()
        self.conn.close()
