# importando as bibliotecas
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, substring

# Definindo o SparkContext
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

sc = SparkContext(conf=conf).getOrCreate()

path_pz: str = "s3a://julioszeferino-dl-processing-zone/enem/"
path_consumer: str = "s3a://julioszeferino-dl-consumer-zone/enem_anon"

if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .appName("Enem Spark Job 2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Buscando os arquivos na zona de processamento..")
    df = (
        spark
        .read
        .format("parquet")
        .load("path_pz")
    )

    print(10*"#")
    print("Iniciando a anonimização dos dados..")

    inscricao_oculta = (
        df
        .withColumn("inscricao_string", df.NU_INSCRICAO.cast("string"))
        .withColumn("inscricao_menor", substring(col("inscricao_string"), 5, 4))
        .withColumn("inscricao_oculta", concat(lit("*****"), col("inscricao_menor"), lit("***")))
        .select("NU_INSCRICAO", "inscricao_oculta", "NU_NOTA_MT", "SG_UF_RESIDENCIA")
    )

    print(10*"#")
    print("Anonimização concluída!")
    print("Iniciando a escrita dos dados..")
    (
        inscricao_oculta
        .write
        .mode("overwrite")
        .format("parquet")
        .save(path_consumer)
    )

    print(10*"#")
    print("Escrita concluída!")
    print("Fim do job!")
    print(10*"#")
    spark.stop()



    