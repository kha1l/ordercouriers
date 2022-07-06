from postgres.psql import Database
import time
from date_work import DataWork
from orders.export import DataExportDay
from work.reader import Reader
from work.worker import Changer
from work.writer import Writer
import datetime


def start():
    db = Database()
    users = db.get_users()
    dt = DataWork().set_date()
    for user in users:
        data = DataExportDay(dt, user[0])
        data.handover()
        time.sleep(3)
        data.driving_couriers()
        time.sleep(3)
        data.time_work()
        time.sleep(3)
        data.schedule()
        time.sleep(10)


def work():
    db = Database()
    users = db.get_users()
    for user in users:
        cls_df = Reader(user[0])
        cls_df.read_df()
        change = Changer(cls_df)
        wait = change.waiting(datetime.time(0, user[2], 0, 0))
        pause = change.pause()
        later, over = change.later()
        wrt = Writer(user[0], wait, pause, later, over)
        wrt.write_wait()
        wrt.write_pause()
        wrt.write_later()
        wrt.write_over()


if __name__ == '__main__':
    start()
    work()
