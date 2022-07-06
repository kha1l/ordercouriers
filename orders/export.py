import requests
import fake_useragent
from datetime import date
from postgres.psql import Database
from bs4 import BeautifulSoup


class DataExportDay:

    def __init__(self, date_end: date, name: str):
        db = Database()
        data = db.get_data(name)
        self.name = name
        self.rest = data[0]
        self.uuid = data[2]
        self.date_end = date_end
        self.login = data[3]
        self.password = data[4]
        self.code = data[5]
        self.session = None
        self.user = None
        self.header = None
        self.auth()

    def auth(self):
        self.session = requests.Session()
        self.user = fake_useragent.UserAgent().random
        log_data = {
            'CountryCode': self.code,
            'login': self.login,
            'password': self.password
        }
        self.header = {
            'user-agent': self.user
        }
        log_link = f'https://auth.dodopizza.{self.code}/Authenticate/LogOn'
        self.session.post(log_link, data=log_data, headers=self.header)

    def save(self, orders_data):
        for order in orders_data:
            response = self.session.post(orders_data[order]['link'], data=orders_data[order]['data'],
                                         headers=self.header)
            with open(f'./orders/export/{order}_{self.name}.xlsx', 'wb') as file:
                file.write(response.content)
                file.close()

    def handover(self):
        orders_data = {
            'order_handover': {
                'link': 'https://officemanager.dodopizza.ru/Reports/OrderHandoverTime/Export',
                'data': {
                    "unitsIds": self.uuid,
                    "beginDate": self.date_end,
                    "endDate": self.date_end,
                    "orderTypes": "Delivery",
                    "Export": "Экспорт+в+Excel"
                }
            }
        }
        self.save(orders_data)

    def driving_couriers(self):
        orders_data = {
            'driving_couriers': {
                'link': 'https://officemanager.dodopizza.ru/Reports/CourierTasks/Export',
                'data': {
                    "unitId": self.rest,
                    "beginDate": self.date_end,
                    "endDate": self.date_end,
                    "statuses": [
                        "Queued",
                        "Ordering",
                        "Paused"
                    ]
                }
            }
        }
        self.save(orders_data)

    def time_work(self):
        orders_data = {
            'time_work': {
                'link': 'https://officemanager.dodopizza.ru/Reports/ActualTime/Export',
                'data': {
                    "PageIndex": "1",
                    "unitId": self.rest,
                    "EmployeeName": "",
                    "isGroupingByEmployee": "false",
                    "beginDate": self.date_end,
                    "endDate": self.date_end,
                    "EmployeeTypes": "Courier"
                }
            }
        }
        self.save(orders_data)

    def schedule(self):
        data_link = 'https://officemanager.dodopizza.ru/Staff/CourierSchedule/GetDaySchedule?date=' \
                    + str(self.date_end) + '&unitId=' + str(self.rest)
        response_data = self.session.get(data_link, headers=self.header)
        soup = BeautifulSoup(response_data.content, 'html.parser')
        with open(f'./orders/export/soup_{self.name}.json', 'w') as file:
            file.write(soup.text)
            file.close()
