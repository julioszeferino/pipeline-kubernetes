from airflow import DAG

# importando as bibliotecas
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator 
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import boto3

# definindo variaveis
aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
glue = boto3.client('glue', region_name='us-east-2',
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)


default_args: dict = {
    'owner': 'Julio',
    'depends_on_past': False,
    'email': ['julioszeferino@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
}

def trigger_crawler_inscricao_func() -> None:
    """
    Funcao responsavel por dar trigger no glue crawler
    enem_anon_crawler.
    """
    glue.start_crawler(Name='enem_anon_crawler')


def trigger_crawler_final_func() -> None:
    """
    Funcao responsavel por dar trigger no glue crawler
    enem_uf_final_crawler
    """
    glue.start_crawler(Name='enem_uf_final_crawler')


with DAG(

    "enem_batch_spark_k8s_processing",
    default_args=default_args,
    description="DAG para processamento de dados do ENEM",
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['enem', 'spark', 'k8s', 'glue'],
) as dag:

    """
    Converte os arquivos csv da landing zone para parquet
    e armazena na zona de processamento.
    """
    converte_parquet = SparkKubernetesOperator(
        task_id="converte_parquet",
        namespace="airflow",
        application_file="enemjob01.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )


    """
    Resgata o status do job de conversao de arquivos csv 
    para parquet.
    """
    converte_parquet_monitor = SparkKubernetesSensor(
        task_id="converte_parquet_monitor",
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    """
    Funcao que anonimiza a coluna de inscricao, seleciona
    colunas do arquivo e armazena na zona de de consumo.
    """ 
    anonimiza_inscricao = SparkKubernetesOperator(
        task_id="anonimiza_inscricao",
        namespace="airflow",
        application_file="enemjob02.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    """
    Resgata o status do job de anonimizacao dos dados.
    """
    anonimiza_inscricao_monitor = SparkKubernetesSensor(
        task_id='anonimiza_inscricao_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='anonimiza_inscricao')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    """
    Funcao que ativa o glue crawler para o arquivo anonimizado.
    """
    trigger_crawler_inscricao = PythonOperator(
        task_id='trigger_crawler_inscricao',
        python_callable=trigger_crawler_inscricao_func,
    )


    """
    Funcao que agrupa as medias de idade por estado e armazena
    na zona de processamento.
    """
    agrega_idade = SparkKubernetesOperator(
        task_id="agrega_idade",
        namespace="airflow",
        application_file="enemjob03.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )


    """
    Resgata o status do job de agrupamento por idade.
    """
    agrega_idade_monitor = SparkKubernetesSensor(
        task_id='agrega_idade_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agrega_idade')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    """
    Funcao que agrupa por estado a quantidade de pessoas
    por sexo e armazena na zona de processamento.
    """
    agrega_sexo = SparkKubernetesOperator(
        task_id="agrega_sexo",
        namespace="airflow",
        application_file="enemjob04.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )


    """
    Resgata o status do job de agrupamento por sexo.
    """
    agrega_sexo_monitor = SparkKubernetesSensor(
        task_id='agrega_sexo_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agrega_sexo')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    """
    Funcao que agrupa por estado a media das notas 
    e armazena na zona de processamento.
    """
    agrega_notas = SparkKubernetesOperator(
        task_id="agrega_notas",
        namespace="airflow",
        application_file="enemjob05.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )


    """
    Resgata o status do job de agrupamento por nota.
    """
    agrega_notas_monitor = SparkKubernetesSensor(
        task_id='agrega_notas_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agrega_notas')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    """
    Funcao que faz o join das tabelas de processamento
    intermediario e armazena na zona de consumo.
    """
    join_final = SparkKubernetesOperator(
        task_id="join_final",
        namespace="airflow",
        application_file="enemjob06.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )


    """
    Resgata o status do job de juncao dos datasets de agrupamento.
    """
    join_final_monitor = SparkKubernetesSensor(
        task_id='join_final_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='join_final')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )


    """
    Funcao que ativa o glue crawler para o arquivo de ufs.
    """
    trigger_crawler_final = PythonOperator(
        task_id='trigger_crawler_final',
        python_callable=trigger_crawler_final_func,
    )


    # definindo o pipeline
    converte_parquet >> converte_parquet_monitor >> anonimiza_inscricao >> anonimiza_inscricao_monitor
    anonimiza_inscricao_monitor >> trigger_crawler_inscricao

    converte_parquet_monitor >> agrega_idade >> agrega_idade_monitor
    converte_parquet_monitor >> agrega_sexo >> agrega_sexo_monitor
    converte_parquet_monitor >> agrega_notas >> agrega_notas_monitor
    [agrega_idade_monitor, agrega_sexo_monitor, agrega_notas_monitor] >> join_final >> join_final_monitor

    join_final_monitor >> trigger_crawler_final

    [agrega_idade_monitor, agrega_notas_monitor] >> agrega_sexo
    [agrega_idade_monitor, agrega_notas_monitor] >> anonimiza_inscricao



