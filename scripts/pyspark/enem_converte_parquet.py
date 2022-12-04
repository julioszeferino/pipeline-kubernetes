# Importando as Bibliotecas
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Definindo o SparkContext
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

sc = SparkContext(conf=conf).getOrCreate()

# arquivos
path_lz: str = "s3a://julioszeferino-dl-landing-zone/enem/"
path_pz: str = "s3a://julioszeferino-dl-processing-zone/enem/"

if __name__ == "__main__":

    # criando a sessao spark
    spark = (
        SparkSession
        .builder
        .appName("Enem Spark Job 1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Iniciando a leitura dos arquivos no s3..")
    df = (
        spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ";")
        .load(path_lz)
    )

    df.printSchema()

    print("Iniciando a leitura dos arquivos no na zona de processamento..")
    (
        df
        .write
        .mode("overwrite")
        .format("parquet")
        .save(path_pz)
    )

    print(10*"#")
    print("Escrito com Sucesso!")
    print(10*"#")

    spark.stop()



