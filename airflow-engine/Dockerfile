FROM python:3.7
RUN pip3 install 'apache-airflow'


# Configure Airflow: connect to backend
WORKDIR /root/airflow/
COPY airflow.sh airflow.sh
RUN chmod +x airflow.sh
COPY mysqlconnect.py mysqlconnect.py
CMD ./airflow.sh


