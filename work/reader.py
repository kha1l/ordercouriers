import pandas as pd


class ReadFile:

    def __init__(self, name_rest: str):
        self.name = name_rest

    def open_file(self, order: str, rows: int):
        try:
            df = pd.read_excel(f'./orders/export/{order}_{self.name}.xlsx', skiprows=rows)
        except ValueError:
            print(f'Неккоректный отчет в {self.name}:{order}')
            df = pd.DataFrame()
        return df


class Reader(ReadFile):

    df_order = None
    df_driving = None
    df_timework = None
    line = None

    def read_df(self):
        self.df_order = self.open_file('order_handover', 6)
        self.df_driving = self.open_file('driving_couriers', 4)
        self.df_timework = self.open_file('time_work', 4)
        with open(f'./orders/export/soup_{self.name}.json', 'r') as file:
            self.line = file.read()
