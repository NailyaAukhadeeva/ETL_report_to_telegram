# coding=utf-8 
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import numpy as np
from io import StringIO
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import telegram
import io
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишем таски в питоне

# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

# параметры Бота
chat_id = -867652742
my_token = '6033778525:AAFYv2EFD_h6QUiZFFpZfqj1HwUVfe0qtmM'
bot = telegram.Bot(token=my_token)

# дата расчета метрик отчета
report_date = '{:%d.%m.%Y}'.format(datetime.today() - timedelta(days=1))
#дата начала вывода данных на график
first_chart_date = '{:%d.%m.%Y}'.format(datetime.today() - timedelta(days=7))
#дата окончания вывода данных на график
last_chart_date = '{:%d.%m.%Y}'.format(datetime.today() - timedelta(days=1))

# Подключение к схеме test
connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.aukhadeeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 7, 10),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

# создаем DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)

def dag_n_auhadeeva_etl_alerts():
    
    #Загружаем значения метрик за вчера
    @task()
    def extract_metrics():
            query = """SELECT
                    count(DISTINCT(user_id)) as DAU,
                    countIf(action = 'like') as likes,
                    countIf(action = 'view') as views,
                    countIf(action = 'like') / countIf(action = 'view') as CTR
                FROM 
                    simulator_20230620.feed_actions
                WHERE toDate(time) = today() - 1
                format TSVWithNames"""
            df_metrics = ch_get_df(query=query)
            return df_metrics
        
    # Загружаем значения метрик за 7 прошедших дней
    @task()    
    def extract_metrics_report():
            query_report = """SELECT
                    toDate(time) as event_date,
                    count(DISTINCT(user_id)) as DAU,
                    countIf(action = 'like') as likes,
                    countIf(action = 'view') as views,
                    (countIf(action = 'like') / countIf(action = 'view')) * 100 as CTR
                FROM 
                    simulator_20230620.feed_actions
                WHERE toDate(time) < today() and toDate(time) >= today() - 7 
                GROUP BY event_date
                format TSVWithNames"""
            df_report = ch_get_df(query=query_report)
            return df_report
    @task()     
    # отправляем показатели метрик за вчера
    def send_mess(df_metrics, chat_id):
        message = (f'Отчет за {report_date}') \
        + '\n\n' + (f'- DAU составляет {df_metrics.DAU[0]}') \
        + '\n' + (f'- Количество likes составляет {df_metrics.likes[0]}') \
        + '\n' + (f'- Количество views составляет {df_metrics.views[0]}') \
        + '\n' + (f'- CTR составляет {df_metrics.CTR[0].round(2)} %')
        bot.sendMessage(chat_id=chat_id, text=message)
        return
   
    # отправляем графики
    @task()
    def send_photo(df_report, chat_id):
        plt.figure(figsize = (20,10))
        plt.suptitle(f'Динамика изменения ключевых метрик за период с {first_chart_date} по {last_chart_date}')

        plt.subplot(4, 1, 2)
        sns_views = sns.lineplot(x="event_date", y="views", data=df_report)
        sns_views.set (xlabel='', ylabel='', title='Views')

        plt.subplot(4, 1, 3)
        sns_likes = sns.lineplot(x="event_date", y="likes", data=df_report)
        sns_likes.set (xlabel='', ylabel='', title='Likes')

        plt.subplot(4, 1, 1)
        sns_dau = sns.lineplot(x="event_date", y="DAU", data=df_report)
        sns_dau.set (xlabel='', ylabel='', title='DAU')

        plt.subplot(4, 1, 4)
        sns_CTR = sns.lineplot(x="event_date", y="CTR", data=df_report)
        sns_CTR.set (xlabel='', ylabel='', title='CTR')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        return

# запуск тасков последовательно    
    df_metrics = extract_metrics()
    df_report = extract_metrics_report()
    send_mess(df_metrics, chat_id)
    send_photo(df_report, chat_id)
      
dag_n_auhadeeva_etl_alerts = dag_n_auhadeeva_etl_alerts()
