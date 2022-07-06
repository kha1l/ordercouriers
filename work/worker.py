from work.reader import Reader
from datetime import timedelta, time, datetime
import pandas as pd
import json
from date_work import DataWork
from json.decoder import JSONDecodeError


class Changer:

    def __init__(self, obj: Reader) -> None:
        self.obj = obj
        self.dt = DataWork().set_date()

    @staticmethod
    def edit_orders(row):
        try:
            row = row.replace(' ', '')
        except AttributeError:
            row = ''
        return row

    @staticmethod
    def change_orders(row):
        try:
            row = row.split('-')[0]
        except AttributeError:
            row = '0'
        return row

    @staticmethod
    def change_time(row):
        try:
            row = row.replace(microsecond=0)
        except AttributeError:
            row = time(0)
        return row

    @staticmethod
    def set_date(row):
        try:
            row = timedelta(hours=row.hour, minutes=row.minute, seconds=row.second)
        except AttributeError:
            row = timedelta(0)
        return row

    @staticmethod
    def change_orders_2(row):
        try:
            m = row.split(', ')
            row = len(m)
        except AttributeError:
            row = 0
        return row

    def waiting(self, time_stand):
        df_time = self.obj.df_order
        df_delivery = self.obj.df_driving
        try:
            df_time = df_time[['Номер заказа', 'Дата и время', 'Ожидание', 'Приготовление', 'Ожидание на полке']]
            df_time.columns = ['num_orders', 'time', 'wait_ready', 'ready', 'wait_delivery']
            df_time = df_time[df_time.wait_delivery > time_stand]

            df_time.time = df_time.time.apply(self.change_time)
            df_time.num_orders = df_time.num_orders.apply(self.change_orders)

            df_time.wait_delivery = df_time['wait_delivery'].apply(self.set_date)
            df_time.wait_ready = df_time['wait_ready'].apply(self.set_date)
            df_time.ready = df_time['ready'].apply(self.set_date)
            df_time.time = df_time['time'].astype('datetime64')

            df_time['all_time'] = df_time.time + df_time.ready + df_time.wait_ready + df_time.wait_delivery
            df_time['meat_time'] = df_time.time + df_time.ready + df_time.wait_ready

            df_result_orders = df_time[['num_orders', 'all_time', 'meat_time']]

        except KeyError:
            df_result_orders = pd.DataFrame()

        try:
            df_delivery['user_id'] = df_delivery[['Фамилия', 'Имя']].apply(' '.join, axis=1)
            df_delivery = df_delivery[['Тип', 'Начало', 'Окончание', 'Длительность', '№ заказа', 'user_id']]
            df_delivery.columns = ['type', 'begin', 'end', 'duration', 'orders', 'user_id']
            df_new_delivery = df_delivery.loc[df_delivery.type == 'На заказе']
            df_orders_rec = pd.DataFrame()
            counter = 0
            for i in df_new_delivery.orders:
                try:
                    m = i.split(', ')
                    for j in m:
                        df_orders_rec = pd.concat([df_orders_rec, df_new_delivery[df_new_delivery.orders == i]],
                                                  ignore_index=True)
                        df_orders_rec.at[counter, 'orders'] = j
                        df_orders_rec.at[counter, 'total'] = i
                        counter += 1
                except AttributeError:
                    pass

        except KeyError:
            df_delivery = pd.DataFrame()
            df_orders_rec = pd.DataFrame()

        try:
            df_orders_rec.total = df_orders_rec['total'].apply(self.change_orders_2)
            df_merge_avg = pd.merge(left=df_orders_rec, left_on=['orders'], right=df_result_orders,
                                    right_on=['num_orders'])
            df_merge = pd.merge(left=df_merge_avg, left_on=['begin'], right=df_delivery, right_on=['end'])
            df_merge = df_merge[
                ['num_orders', 'total', 'user_id_x', 'meat_time', 'all_time', 'begin_x', 'end_x', 'duration_x',
                 'begin_y', 'end_y', 'duration_y']]
            df_merge.columns = ['orders', 'total', 'user', 'meat_time', 'all_time', 'begin_delivery',
                                'end_delivery', 'duration_delivery', 'begin_wait', 'end_wait', 'duration_wait']

            result = df_merge[df_merge.duration_wait > time_stand]
        except KeyError:
            result = pd.DataFrame()
        return result

    def pause(self):
        df_pause = self.obj.df_driving

        try:
            df_pause['user_id'] = df_pause[['Фамилия', 'Имя']].apply(' '.join, axis=1)
            df_pause = df_pause[['Тип', 'Начало', 'Окончание', 'Длительность', '№ заказа', 'user_id']]
            df_pause.columns = ['type', 'begin', 'end', 'duration', 'orders', 'user_id']

            df_pause = df_pause.loc[df_pause['type'] == 'Пауза']
        except KeyError:
            df_pause = pd.DataFrame()
        return df_pause

    def later(self):
        df_time = self.obj.df_timework
        line = self.obj.line

        try:
            df_time = df_time[['Время начала смены', 'Фамилия, имя сотрудника', 'Id сотрудника']]
            df_time = df_time.drop(df_time[df_time['Время начала смены'] == self.dt].index)
            df_time.columns = ['date', 'name', 'user_id']
            df_actual_time = df_time.drop_duplicates(subset=['name', 'user_id'], keep='first', ignore_index=True)
        except KeyError:
            df_actual_time = pd.DataFrame()

        try:
            schedule_day = json.loads(line)
            df_schedule = pd.DataFrame(columns=['user_id', 'date', 'name'])
            for i in schedule_day['courierScheduleItems']:
                new_step = i["intervals"][0]
                start_time = new_step['beginDate']
                deadline = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                user_id = i['id']
                name = i['title']
                df_schedule.loc[len(df_schedule.index)] = [user_id, deadline, name]
            df_schedule['user_id'] = df_schedule['user_id'].apply(lambda x: int(x))
        except JSONDecodeError:
            df_schedule = pd.DataFrame()

        df_over = pd.DataFrame()
        counter = 0
        try:
            for i in df_actual_time.user_id:
                if i not in df_schedule.user_id.values:
                    df_over = pd.concat([df_over, df_actual_time[df_actual_time.user_id == i]], ignore_index=True)
                    df_over.at[counter, 'type'] = 0
                    counter += 1
        except AttributeError:
            pass

        try:
            for j in df_schedule.user_id:
                if j not in df_actual_time.user_id.values:
                    df_over = pd.concat([df_over, df_schedule[df_schedule.user_id == j]], ignore_index=True)
                    df_over.at[counter, 'type'] = 1
                    counter += 1
        except AttributeError:
            pass

        try:
            df_later = df_actual_time.merge(df_schedule, on=["user_id"])
            df_later = df_later[['user_id', 'name_x', 'date_y', 'date_x']]
            df_later = df_later.rename({'date_y': 'schedule', 'date_x': 'real', 'name_x': 'name'}, axis=1)

            df_later = df_later[df_later.real > df_later.schedule]
            try:
                df_later['deadline'] = df_later.real - df_later.schedule
            except TypeError:
                df_later['deadline'] = 0
        except KeyError:
            df_later = pd.DataFrame()
        return df_later, df_over
