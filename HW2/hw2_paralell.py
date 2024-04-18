import pandas as pd 
from datetime import timedelta, datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import tempfile
import os

# Автор: Вяткин Роман.


#перечень обрабатываемых продуктов, может быть расширен
product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

dag = DAG('VyatkinRomanHW2Paralell', 
    schedule="0 0 5 * *",
    start_date=datetime(2024, 3, 31),
    max_active_runs=32,
	max_active_tasks=32,
    concurrency=10
    )

#загрузка и извлекатель данных за месяц
def extract_profit(path: str, **context) -> None: 
    profit_table = pd.read_csv(path)
    date_cur = date.today()
    start_date = pd.to_datetime(date_cur) - pd.DateOffset(months=2)
    end_date = pd.to_datetime(date_cur) + pd.DateOffset(months=1)
    date_list = pd.date_range(
        start=start_date, end=end_date, freq='M'
    ).strftime('%Y-%m-01')
    df_tmp = (
        profit_table[profit_table['date'].isin(date_list)]
        .drop('date', axis=1)
        .groupby('id')
        .sum()
    )
    df_tmp = pd.DataFrame(df_tmp)
    tmp_fn = tempfile.NamedTemporaryFile().name
    df_tmp.to_csv(tmp_fn)
    context['ti'].xcom_push(key='cutted_profit', value=tmp_fn)

# обработчик для параллельной трансформации
def transform_profit_parallel(product:str, **context) -> None: 

    df_name = context['ti'].xcom_pull(key='cutted_profit', task_ids=['extract_profit'])[0]
    df_tmp = pd.read_csv(df_name)
    df_tmp[f'flag_{product}'] = (
         df_tmp.apply(
            lambda x: x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0,
            axis=1
        ).astype(int)    
    )    
    df_tmp = df_tmp.filter(regex='flag').reset_index()
    tmp_fn = tempfile.NamedTemporaryFile().name
    df_tmp.to_csv(tmp_fn)
    context['ti'].xcom_push(key=f'product_{product}', value=tmp_fn)





# склейка трансформированных продуктов и выгрузка
def load_profit(path: str, **context) -> None: 
    for ind, product in enumerate(product_list):
        if ind == 0:
            df_load_name = context['ti'].xcom_pull(key=f'product_{product}', task_ids=[f'transform_profit_{product}'])[0]
            df_load = pd.read_csv(df_load_name)
            os.remove(df_load_name)#Удалим временный файл который уже не нужен
        else:
            df_product_name = context['ti'].xcom_pull(key=f'product_{product}', task_ids=[f'transform_profit_{product}'])[0]
            df_product = pd.read_csv(df_product_name)
            os.remove(df_product_name)#Удалим временный файл который уже не нужен
            df_product.index = df_load.index
            df_load = pd.concat((df_load, df_product), axis=1)
    df_load.to_csv(path, index=None, mode='a')
    #Удалим временный файл
    df_name = context['ti'].xcom_pull(key='cutted_profit', task_ids=['extract_profit'])[0]
    os.remove(df_name)


#задача загрузки и извлечения
task_extract_profit = PythonOperator( 
    task_id='extract_profit',
    python_callable=extract_profit,
    op_kwargs={'path': 'profit_table.csv'},
    dag=dag,
    provide_context=True
)


#создадим пул задач по числу обрабатываемых продуктов
trp=[]
for i in product_list:
    task_transform_profit = PythonOperator( 
       task_id=f'transform_profit_{i}',
        python_callable=transform_profit_parallel,
        op_kwargs={'product': i},
        dag=dag,
        provide_context=True
    )   
    trp.append(task_transform_profit)


#задача склейки результатов и выгрузки
task_load_profit = PythonOperator( 
    task_id='load_profit',
    python_callable=load_profit,
    op_kwargs={'path': 'flags_activity.csv'},
    dag=dag,
    provide_context=True
)


task_extract_profit  >> trp  >> task_load_profit 
