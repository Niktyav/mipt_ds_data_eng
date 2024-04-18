import pandas as pd 
from datetime import timedelta, datetime, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG('VyatkinRomanHW2', 
    schedule="0 0 5 * *",
    start_date=datetime(2024, 3, 31)
    )

def extract_profit(path: str, **context) -> None: 
    profit_table = pd.read_csv(path)
    context['ti'].xcom_push(key='extracted_profit', value=profit_table)

def transform_profit(**context) -> None: 

    profit_table = context['ti'].xcom_pull(key='extracted_profit', task_ids=['extract_profit'])[0]

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
    
    product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    for product in product_list:
        df_tmp[f'flag_{product}'] = (
            df_tmp.apply(
                lambda x: x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0,
                axis=1
            ).astype(int)
        )
        
    df_tmp = df_tmp.filter(regex='flag').reset_index()
    context['ti'].xcom_push(key='transformed_profit', value=df_tmp)

def load_profit(path: str, **context) -> None: 
    transformed_profit = context['ti'].xcom_pull(key='transformed_profit', task_ids=['transform_profit'])
    loaded_profit = pd.DataFrame(transformed_profit[0])
    loaded_profit.to_csv(path, index=None, mode='a')

task_extract_profit = PythonOperator( 
    task_id='extract_profit',
    python_callable=extract_profit,
    op_kwargs={'path': 'profit_table.csv'},
    dag=dag,
    provide_context=True
)

task_transform_profit = PythonOperator( 
    task_id='transform_profit',
    python_callable=transform_profit,
    dag=dag,
    provide_context=True
)

task_load_profit = PythonOperator( 
    task_id='load_profit',
    python_callable=load_profit,
    op_kwargs={'path': 'flags_activity.csv'},
    dag=dag,
    provide_context=True
)

task_extract_profit >> task_transform_profit >> task_load_profit 