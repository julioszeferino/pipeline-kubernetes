cd docker
docker build -f sparkoperator.dockerfile -t julioszeferino/spark-operator:v1 .
docker login --username=julioszeferino
docker image push julioszeferino/spark-operator:v1
