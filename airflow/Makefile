all:
	helm repo add apache-airflow https://airflow.apache.org
	kubectl apply -f pv-airflow-dags.yaml
	kubectl apply -f pv-airflow-logs.yaml
	helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace -f values.yaml

clean:
	helm uninstall airflow -n airflow
	kubectl delete pv pv-airflow-dags
	kubectl delete pv pv-airflow-logs