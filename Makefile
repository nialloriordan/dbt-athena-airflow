.DEFAULT_GOAL := help 
help:
	@egrep -h '\s#\s' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?# "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build-airflow-image: # build the airflow image
	docker build -f Dockerfile -t docker_airflow .
restart-airflow-stack: # restart airflow locally
	docker-compose down && docker-compose up
