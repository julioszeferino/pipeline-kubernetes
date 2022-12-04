# importa as bibliotecas
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

# Definindo o SparkContext
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

sc = SparkContext(conf=conf).getOrCreate()

path_pz1: str = "s3a://julioszeferino-dl-processing-zone/enem/"
path_pz2: str = "s3a://julioszeferino-dl-processing-zone/intermediarias/uf_idade"

if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .appName("Enem Spark Job 3")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Buscando os arquivos na zona de processamento..")
    df = (
        spark
        .read
        .format("parquet")
        .load(path_pz1)
    )

    print(10*"#")
    print("Iniciando a agregação dos dados..")
    uf_idade = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(df.NU_IDADE).alias("med_nu_idade"))
    )

    print(10*"#")
    print("Agregação concluída!")
    print("Iniciando a escrita dos dados..")
    (
        uf_idade
        .write
        .mode("overwrite")
        .format("parquet")
        .save(path_pz2)
    )

    print(10*"#")
    print("Escrita concluída!")
    print("Fim do Job!")
    print(10*"#")
    spark.stop()