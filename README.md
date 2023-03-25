# de-project

## Prerequisites
```
Python 3.8.10
docker 23.0.1
docker-compose 1.28.5
```

## Repository Structure
- docker folder: all docker files
- src folder: all source codes
- .gitignore file
- README.md file

## airflow (with dwh Postgres)
Create file .env in docker folder with content like that (note using absolute path for AIRFLOW_PROJ_DIR)
```
AIRFLOW_UID=1000
AIRFLOW_PROJ_DIR=../src/airflow
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
_PIP_ADDITIONAL_REQUIREMENTS=
```
after that, run these commands
```
docker compose -f docker/airflow.yml up airflow-init
docker compose -f docker/airflow.yml up
```

## superset
```
docker compose -f docker/superset.yml up
```