from psycopg2 import errors
from postgres.psql import Database
from date_work import DataWork
from psycopg2.errorcodes import UNIQUE_VIOLATION


class Writer:
    def __init__(self, rest, df_w, df_p, df_l, df_o):
        self.db = Database()
        self.dt = DataWork().set_date()
        self.rest = rest
        self.df_w = df_w
        self.df_p = df_p
        self.df_l = df_l
        self.df_o = df_o

    def write_wait(self):
        for i, row in self.df_w.iterrows():
            user = row.user
            number = int(row.orders)
            meat = str(row.meat_time)
            queue = str(row.begin_wait)
            delivery = str(row.begin_delivery)
            orders = row.total
            try:
                self.db.add_wait(self.rest, self.dt, user, number, meat, queue, delivery, orders)
            except errors.lookup(UNIQUE_VIOLATION):
                pass

    def write_pause(self):
        for i, row in self.df_p.iterrows():
            user = row.user_id
            begin = str(row.begin)
            duration = str(row.duration)

            try:
                self.db.add_pause(self.rest, self.dt, user, begin, duration)
            except errors.lookup(UNIQUE_VIOLATION):
                pass

    def write_later(self):
        for i, row in self.df_l.iterrows():
            try:
                user_id = row.user_id
                user = row['name']
                duration = str(row.deadline)
            except AttributeError:
                break
            except KeyError:
                break
            try:
                self.db.add_later(self.rest, self.dt, user, user_id, duration)
            except errors.lookup(UNIQUE_VIOLATION):
                pass

    def write_over(self):
        for i, row in self.df_o.iterrows():
            try:
                user_id = row.user_id
                user = row['name']
                type_over = row.type
            except AttributeError:
                break
            except KeyError:
                break
            try:
                self.db.add_over(self.rest, self.dt, user, user_id, type_over)
            except errors.lookup(UNIQUE_VIOLATION):
                pass
