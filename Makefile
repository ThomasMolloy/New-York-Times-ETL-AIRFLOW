build:
	docker build --rm -t puckel/docker-airflow .

run: build
	docker-compose up
