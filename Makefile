# terraform validate
# terraform destroy
# subir cluster eks

NOTEBOOKS=notebooks/
AWS_ID=
AWS_SECRET=
NODES=5

jupyterup:
	docker build -f docker/jupyter.dockerfile -t julioszeferino/jupyter:v1 . \
	&& docker run --name "jupyter" -d -p 8888:8888 -v $(PWD)/$(NOTEBOOKS):/home/jovyan/work julioszeferino/jupyter:v1

jupyterdown:
	docker stop jupyter && docker rm jupyter


clustereks:
	eksctl create cluster --name=meuclusterk8s --managed --instance-types=m5.large \
	--spot --nodes-min=${NODES} --region=us-east-2 --alb-ingress-access --node-private-networking \
	--full-ecr-access --nodegroup-name=ng-meuclusterk8s --color=fabulous --version=1.22 \



deployairflow:
	kubectl create namespace airflow \
	&& helm install airflow apache-airflow/airflow -f kubernetes/airflow/deploy.yaml -n airflow --debug \
	&& kubectl get svc airflow-webserver -n airflow \
	&& kubectl apply -f kubernetes/airflow/rolebinding.yaml -n airflow 


destroyall:
	helm uninstall airflow -n airflow \
	&& kubectl delete svc airflow-webserver -n airflow \
	&& kubectl delete pvc data-airflow-postgresql-0 -n airflow
	&& kubectl delete namespace airflow 