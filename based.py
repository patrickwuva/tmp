import psycopg2

class based:
    def __init__(self):
        self.info = "host='34.48.163.96' port='5432' user='tpv' password='PSpsalm14!!!' dbname='tpv-dev' sslmode = 'require'"
        self.connection = None
        self.cursor = None
    def connect(self):
        try:
            self.connection = psycopg2.connect(self.info)
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(f'Error connecting: {e}')

    def close(self):
        if self.connection is not None:
            self.cursor.close()
            self.connection.close()
        else:
            print('Connection already closed')

    def get_version(self):
        query = "SELECT version();"
        self.cursor.execute(query)
        return(self.cursor.fetchone())
