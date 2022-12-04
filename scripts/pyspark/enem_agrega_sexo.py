# importando as bibliotecas
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

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
path_pz2: str = "s3a://julioszeferino-dl-processing-zone/intermediarias/uf_sexo"

if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .appName("Enem Spark Job 4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Iniciando a leitura dos dados da zona de processamento")
    df = (
        spark
        .read
        .format("parquet")
        .load(path_pz1)
    )

    print(10*"#")
    print("Iniciando a agregacao dos dados")
    uf_m = (
        df
        .filter(col("TP_SEXO") == "M")
        .groupBy("SG_UF_RESIDENCIA")
        .agg(count(col("TP_SEXO")).alias("count_m"))
    )

    uf_f = (
        df
        .filter(col("TP_SEXO") == "F")
        .groupBy("SG_UF_RESIDENCIA")
        .agg(count(col("TP_SEXO")).alias("count_f"))
    )

    uf_sexo = uf_m.join(uf_f, on="SG_UF_RESIDENCIA", how="inner")

    print(10*"#")
    print("Iniciando a escrita dos dados na zona de processamento")

    (
        uf_sexo
        .write
        .mode("overwrite")
        .format("parquet")
        .save(path_pz2)
    )

    print(10 *"#")
    print("Escrita concluída!")
    print("Fim do job!")
    print(10*"#")
    spark.stop()


