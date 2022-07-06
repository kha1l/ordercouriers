import psycopg2
from config.cfg import Settings


class Database:
    @property
    def connection(self):
        stg = Settings()
        return psycopg2.connect(
            database=stg.dbase,
            user=stg.user,
            password=stg.password,
            host=stg.host,
            port='5432'
        )

    def execute(self, sql: str, parameters: tuple = None, fetchone=False, fetchall=False, commit=False):
        if not parameters:
            parameters = tuple()
        connection = self.connection
        cursor = connection.cursor()
        data = None
        cursor.execute(sql, parameters)
        if commit:
            connection.commit()
        if fetchone:
            data = cursor.fetchone()
        if fetchall:
            data = cursor.fetchall()
        connection.close()
        return data

    def get_data(self, name: str):
        sql = '''
            SELECT restId, restName, restUuid, userLogs, userPass, countryCode 
            FROM orders 
            WHERE restName=%s 
            order by restId
        '''
        parameters = (name,)
        return self.execute(sql, parameters=parameters, fetchone=True)

    def get_users(self):
        sql = '''
            SELECT restName, restId, restTime
            FROM orders
            WHERE status=%s 
            order by restId
        '''
        parameters = ('work',)
        return self.execute(sql, parameters=parameters, fetchall=True)

    def add_wait(self, name_rest: str, dt, person: str, number: int, meat: str, queue: str,
                 delivery: str, orders: int):
        sql = '''
            INSERT INTO wait
            (name_rest, date, person, number, 
            meat, queue, delivery, orders)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        parameters = (name_rest, dt, person, number, meat, queue, delivery, orders)
        self.execute(sql, parameters=parameters, commit=True)

    def add_pause(self, name_rest: str, dt, person: str, begin: str, duration: str):
        sql = '''
            INSERT INTO pause (name_rest, date, person, begin, duration) 
            VALUES (%s, %s, %s, %s, %s)
        '''
        parameters = (name_rest, dt, person, begin, duration)
        self.execute(sql, parameters=parameters, commit=True)

    def add_later(self, name_rest: str, dt, person: str, person_id: str, duration: str):
        sql = '''
            INSERT INTO later (name_rest, date, person, person_id, duration) 
            VALUES (%s, %s, %s, %s, %s)
        '''
        parameters = (name_rest, dt, person, person_id, duration)
        self.execute(sql, parameters=parameters, commit=True)

    def add_over(self, name_rest: str, dt, person: str, person_id: str, type_over: str):
        sql = '''
            INSERT INTO over (name_rest, date, person, person_id, type) 
            VALUES (%s, %s, %s, %s, %s)
        '''
        parameters = (name_rest, dt, person, person_id, type_over)
        self.execute(sql, parameters=parameters, commit=True)
