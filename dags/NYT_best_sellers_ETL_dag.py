import requests
import json
import time
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta, datetime

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

default_args = {
    'owner': 'Thomas',
    'depends_on_past': True,
    'email': ['thomasmolloyorm@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args, 
    schedule_interval=timedelta(days=1), 
    start_date=days_ago(0),
    catchup=False,
    tags=['ETL'])
def nyt_bestsellers_ETL():

    @task()
    def extract_nyt_best_sellers(
        date: str = 'current', 
        list_name: list = ['combined-print-and-e-book-fiction',
                        'combined-print-and-e-book-nonfiction']) -> dict :
        
        '''
        Extracts New York Times best seller list information for 
        combined-print-and-e-book-fiction and combined-print-and-e-book-nonfiction
        and returns list of results.
        '''
        requestHeaders = {
        "Accept": "application/json"
        }
        nyt_best_sellers = []
        date = Variable.get('next_update_date')
        api_key = Variable.get('NYT_API_KEY')
        for list_ in list_name:
            url = f'https://api.nytimes.com/svc/books/v3/lists/{date}/{list_}.json?api-key={api_key}'
            data = requests.get(url, requestHeaders)
            if not data:
                raise AirflowFailException('Date or Data Point not available')
            else:
                nyt_best_sellers.append(data.json()['results'])
        #If request succeeds -- than update the next_update_date variable
        next_update_date = datetime.strptime(date, '%Y-%m-%d')
        next_update_date += timedelta(days=7)
        next_update_date = next_update_date.strftime('%Y-%m-%d')
        Variable.set('next_update_date', next_update_date)

        return nyt_best_sellers
    
    @task()
    def transform_nyt_best_sellers(
            nyt_best_sellers: list) -> list :
        
        '''
        Transforms list of combined-print-and-ebook-fiction and
        combined-print-and-e-book-nonfiction into pandas dataframes,
        selects important columns, and returns list of dataframes.
        '''
        combined_print_and_e_book_fiction = pd.DataFrame(
            nyt_best_sellers[0]['books'],
            columns=['title','author','rank'])
        combined_print_and_e_book_nonfiction = pd.DataFrame(
            nyt_best_sellers[1]['books'], 
            columns=['title','author','rank'])
        combined_print_and_e_book_fiction.rename(columns={'rank':'ranking'}, inplace=True)
        combined_print_and_e_book_nonfiction.rename(columns={'rank':'ranking'}, inplace=True)


        return [combined_print_and_e_book_fiction,
                combined_print_and_e_book_nonfiction]

    @task()
    def load_nyt_best_sellers(
        nyt_best_sellers_dfs: list) -> None:
        '''
        Obtains database parameters from airflow variables and
        loads New York Times Best Seller information into separate
        mySQL database tables.
        '''
        engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/postgres")

        fiction_df = nyt_best_sellers_dfs[0]
        nonfiction_df = nyt_best_sellers_dfs[1]

        fiction_df.to_sql('nyt_bestsellers_fiction', con=engine, if_exists='replace')
        nonfiction_df.to_sql('nyt_bestsellers_nonfiction', con=engine, if_exists='replace')

    nyt_bestseller_data = extract_nyt_best_sellers()
    transformed_nyt_bestseller_data = transform_nyt_best_sellers(nyt_bestseller_data)
    load_nyt_best_sellers(transformed_nyt_bestseller_data)

nyt_bestsellers_ETL_dag = nyt_bestsellers_ETL()