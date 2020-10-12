from typing import Dict, List

import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

ANALYTICS_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
BIGQUERY_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
ADS_SCOPES = ['https://www.googleapis.com/auth/adwords']
KEY_FILE_LOCATION = ''  # Путь до json файла авторизации


REPORTING = {'name': 'analyticsreporting',
             'version': 'v4'}

MANAGEMENT = {'name': 'analytics',
              'version': 'v3'}


class GoogleAnalytics(object):

    def __init__(self, scopes: List[str], key_file_location: str):
        """
        Инициализация объекта класса

        :param scopes: ссылка с областью доступа.
        Подробнее https://developers.google.com/analytics/devguides/reporting/core/v4/authorization
        :type scopes: str

        :param key_file_location: ссылка на json файл ключом сервисного аккаунта
        :type key_file_location: str

        :return: nothing
        """

        self.scopes = scopes
        self.key_file_location = key_file_location

    def initialize_analytics(self, api: Dict[str, str]):
        """
        Сервисный объект для доступа к Google API.

        :param api: информация об имени и версии API, к которому планируется подключение
        :type api: dict

        :return: Сервисный объект для доступа к Google API.

        """
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.key_file_location, self.scopes)
        service = build(api['name'], api['version'], credentials=credentials)
        return service

    @staticmethod
    def get_goals(service, account_id: str, web_property_id: str, profile_id: str):
        """
        Возвращает идентификаторы целей, их названия и ещё кучу всего

        :param service: Сервисный объект для доступа к Google API.
        :type service: googleapiclient.discovery.Resource

        :param account_id: идентификатор аккаунта Google Analytics
        :type account_id: str

        :param web_property_id: идентификатор ресурса Google Analytics
        :type web_property_id: str

        :param profile_id: идентификатор представления Google Analytics
        :type profile_id: str

        :rtype: dict
        :return: всю информацию о настроенных целях для указанного представления. Можно сразу загонять в DataFrame

        """
        return service.management().goals().list(accountId=account_id,
                                                 webPropertyId=web_property_id,
                                                 profileId=profile_id).execute()

    @staticmethod
    def get_report(service, profile_id, dimensions, metrics, filters,
                   date_from, date_to, page_size=1000, next_page_token=None):
        """
        Получаем отчёт с заданными параметрами и показателями

        :param profile_id: идентификатор представления
        :type profile_id: str

        :param service: сервисный объект для доступа к Google API. Результат работы функции initialize_analytics
        :type service: googleapiclient.discovery.Resource

        :param dimensions: список параметров, по которым необходим отчёт
        :type dimensions: list of strings

        :param metrics: список показателей, по которым необходим отчёт
        :type metrics: list of strings

        :param filters: объект фильтра
        :type filters: dict

        :param date_from: дата начала сбора статистического отчета в формате YYYY-mm-DD
        :type date_from: str

        :param date_to: дата окончания сбора статистического отчета в формате YYYY-mm-DD
        :type date_to: str

        :param page_size: размер отчета. Максимум 100000 строк
        :type page_size: int

        :param next_page_token: токен для следующей страницы отчета в случае, если он не влез в определенный выше размер
        :type next_page_token: str

        :rtype: json
        :return: отчет по указанным параметрам и показателям за выбранные даты

        """

        response = service.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': profile_id,
                        'dateRanges': [{'startDate': date_from,
                                        'endDate': date_to}],

                        'dimensions': dimensions,

                        'metrics': metrics,

                        'samplingLevel': 'LARGE',
                        'pageSize': page_size,
                        'pageToken': next_page_token,

                        "dimensionFilterClauses": [filters]
                    }]
            }
        ).execute()

        return response

    def get_full_report(self, profile_id, service, dimensions, metrics, filters,
                        date_from, date_to, page_size, next_page_token=None):

        """
        Запускает функцию get_report до тех пор, пока страницы не кончатся

        :param profile_id: идентификатор представления Google Analytics
        :type profile_id: str

        :param service: Сервисный объект для доступа к Google API.
        :type service: googleapiclient.discovery.Resource

        :param dimensions: список параметров, по которым необходим отчёт
        :type dimensions: list of strings

        :param metrics: список показателей, по которым необходим отчёт
        :type metrics: list of strings

        :param date_from: дата начала сбора статистического отчета в формате YYYY-mm-DD
        :type date_from: str

        :param date_to: дата окончания сбора статистического отчета в формате YYYY-mm-DD
        :type date_to: str

        :param page_size: размер отчета. Максимум 100000 строк
        :type page_size: int

        :param next_page_token: токен для следующей страницы отчета в случае, если он не влез в определенный выше размер
        :type next_page_token: str

        :rtype: json
        :return: отчет по указанным параметрам и показателям за выбранные даты
        """

        response = self.get_report(service,
                                   profile_id,
                                   dimensions,
                                   metrics,
                                   filters,
                                   date_from,
                                   date_to,
                                   page_size,
                                   next_page_token)

        rows = response.get('reports')[0].get('data').get('rows')
        next_page_token = response.get('reports')[0].get('nextPageToken')

        while next_page_token is not None:
            next_page = self.get_report(service,
                                        profile_id,
                                        dimensions,
                                        metrics,
                                        filters,
                                        date_from,
                                        date_to,
                                        page_size,
                                        next_page_token)
            next_page_token = next_page.get('reports')[0].get('nextPageToken')
            rows.extend(next_page.get('reports')[0].get('data').get('rows'))

        response['reports'][0]['data']['rows'] = rows

        return response
    
    @staticmethod
    def del_ga(response):
        """
        Удаляет ga: из имен параметров и показателей
        """

        metric_headers = response['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']

        response['reports'][0]['columnHeader']['dimensions'] = \
            list(map(lambda x: x[3:], response['reports'][0]['columnHeader']['dimensions']))

        for header in metric_headers:
            if header['name'][:3] == 'ga:':
                header['name'] = header['name'][3:]

        response['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries'] = metric_headers

        return response

    @staticmethod
    def response_to_data_frame(response):
        """
        Разибрает ответ и укладывает в pandas DataFrame
        """

        for report in response.get('reports', []):
            column_header = report.get('columnHeader', {})
            dimension_headers = column_header.get('dimensions', [])
            metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])

            df_raws = []
            for row in rows:

                df_row = {}

                dimensions = row.get('dimensions', [])
                date_range_values = row.get('metrics', [])
                for header, dimension in zip(dimension_headers, dimensions):
                    df_row[header] = dimension

                for _, values in enumerate(date_range_values):

                    for metricHeader, value in zip(metric_headers, values.get('values')):
                        df_row[metricHeader.get('name')] = value

                df_raws.append(df_row)

        return pd.DataFrame(df_raws)


if __name__ == '__main__':
    your_account_id = ''
    your_web_property_id = ''
    your_profile_id = ''

    analytics_management = GoogleAnalytics(ANALYTICS_SCOPES, KEY_FILE_LOCATION)
    management_service = analytics_management.initialize_analytics(MANAGEMENT)

    goals = analytics_management.get_goals(service=management_service,
                                           account_id=your_account_id,
                                           web_property_id=your_web_property_id,
                                           profile_id=your_profile_id)

    goals = pd.DataFrame(goals['items'])
    print(goals[['id', 'name', 'type']])

    dims = [{'name': 'ga:date'},
            {'name': 'ga:sourceMedium'}]

    mets = [{'expression': 'ga:sessions'}]

    filts = {
         "operator": 'OR',
         "filters": [
             {
                 "dimensionName": "ga:medium",
                 "operator": "EXACT",
                 "expressions": ["cpc"]
             },
             {
                 "dimensionName": "ga:source",
                 "operator": "EXACT",
                 "expressions": ["google"]
             }
         ]
    }

    analytics_reporting = GoogleAnalytics(ANALYTICS_SCOPES, KEY_FILE_LOCATION)
    reporting_service = analytics_reporting.initialize_analytics(REPORTING)

    report = analytics_reporting.get_full_report(service=reporting_service,
                                                 profile_id=your_account_id,
                                                 dimensions=dims,
                                                 metrics=mets,
                                                 filters=filts,
                                                 date_from='2020-09-01',
                                                 date_to='2020-09-01',
                                                 page_size=1000)

    report_df = analytics_reporting.response_to_data_frame(report)

    print(report_df)
