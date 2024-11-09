export AIRFLOW_HOME=/mnt/e/capstone3/airflow

nohup airflow webserver --port 8080 > webserver.out 2>&1 &
nohup airflow scheduler > scheduler.out 2>&1 &
