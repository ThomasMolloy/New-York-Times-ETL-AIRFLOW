FROM python:3.7
RUN pip3 install apache-airflow
RUN pip3 install apache-airflow-providers-mysql
RUN pip3 install mysqlclient
RUN pip3 install mysql-connector-python


# Configure Airflow: connect to backend
WORKDIR /root/airflow/
COPY airflow.sh airflow.sh
RUN chmod +x airflow.sh
COPY mysqlconnect.py mysqlconnect.py
CMD ./airflow.sh


