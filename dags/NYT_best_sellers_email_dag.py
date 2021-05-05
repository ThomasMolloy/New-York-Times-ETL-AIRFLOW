from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Thomas',
    'depends_on_past': False,
    'email': ['thomasmolloyorm@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


with DAG( 
    dag_id='nyt_best_sellers_email',
    description='Send email to email list when NYT best sellers is updated',
    schedule_interval=timedelta(minutes=5),
    start_date=days_ago(0),
    default_args = default_args,
    catchup=False,
    tags=['Email']
    ) as dag:

    sensor=ExternalTaskSensor(
        task_id='sensor',
        external_dag_id='nyt_bestsellers_ETL',
        external_task_id='load_nyt_best_sellers'
    )

    '''     
    def build_email():
        mysql_hook = MySqlHook(conn_name_attr='nyt_db_id')
        best_sellers_data_ = mysql_hook.get_pandas_df(
            sql= "SELECT title, author, ranking\
                FROM nyt_bestsellers_fiction\
                UNION SELECT title, author, ranking,\
                FROM nyt_bestsellers_nonfiction")

        email_body = '<h3> The New York Times Best Seller List Has Recently Been Updated! </h3>'
        return email_body

    
    task = PythonOperator(
        task_id='create_email',
        python_callable=build_email,
        do_xcom_push=True,
        dag=dag
    )
    '''

    email = EmailOperator(
                task_id='send_email',
                to= ['tjmolloy15@gmail.com', "{{dag_run.conf['email']}}"],
                subject='New York Times Best Seller List',
                html_content= 'templates/email_body.html'
            )
    sensor >> email