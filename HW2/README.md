# mipt_ds_data_eng
##Задача:##
Cоздать DAG в Apache Airflow, который будет по расписанию запускать расчет витрины активности клиентов по сумме и количеству их транзакций.


Нужно наладить следующий ETL-процесс:

Extract. Необходимые данные хранятся в таблице profit_table.csv. Это таблица, в которой для каждого клиента по 10-ти продуктам собраны суммы и количества транзакций за каждый месяц.  

Что в названиях столбцов:  

id - уникальный идентификатор клиента;  
продукты обозначены буквами от a до j латинского алфавита;   
сумма транзакций по соответствующим продуктам начинается с sum_…;   
количество транзакций по соответствующим продуктам начинается с count_….   
Для примера в таблице содержатся данные с октября 2023 года по март 2024.

##Transform.  
Суть витрины, которую необходимо собрать, таблица с флагами активностей клиентов по продуктам. Методика расчёта следующая: клиент считается активным по продукту за дату X, если в предыдущие 3 месяца (X, X-1 и X-2) у него были ненулевая сумма и количество транзакций по нему.  

Но это всё за нас уже реализовали дата-саентисты. Их код для сбора итоговой витрины находится в функции transform в файле transform_script.py, его необходимо встроить в процесс.   

##Load.  
Итоговые результаты необходимо сохранять в csv-файл flags_activity.csv в той же директории, не перезатирая при этом данные старых расчётов в нём. 

##Особенности дага:  
он должен запускаться по расписанию каждый месяц 5-го числа. Предполагается, что данные profit_table хранятся в общедоступной директории и пополняются ежемесячно новыми данными.


##Результат:  
DAG в Apache Airflow    
![plot](https://github.com/Niktyav/mipt_ds_data_eng/tree/main/HW3/img/vyatkin_roman_dags.JPG)   
![plot](https://github.com/Niktyav/mipt_ds_data_eng/tree/main/HW3/img/vyatkin_roman_graph.JPG)   


Распараленный DAG по продуктам   
![plot](https://github.com/Niktyav/mipt_ds_data_eng/tree/main/HW3/img/vyatkin_roman_dags_paralell.JPG)   
![plot](https://github.com/Niktyav/mipt_ds_data_eng/tree/main/HW3/img/vyatkin_roman_graph_paralell.JPG)   