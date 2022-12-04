# importando as bibliotecas
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean

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
path_pz2: str = "s3a://julioszeferino-dl-processing-zone/intermediarias/uf_notas"

if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .appName("Enem Spark Job 5")
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
    uf_mt =(
         df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_MT")).alias("med_mt"))
    )

    uf_cn = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_CN")).alias("med_cn"))
    )

    uf_ch = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_CH")).alias("med_ch"))
    )

    uf_lc = (
        df
        .groupBy("SG_UF_RESIDENCIA")
        .agg(mean(col("NU_NOTA_LC")).alias("med_lc"))
    )

    uf_notas = (
        uf_mt
        .join(uf_cn, on="SG_UF_RESIDENCIA", how="inner")
        .join(uf_ch, on="SG_UF_RESIDENCIA", how="inner")
        .join(uf_lc, on="SG_UF_RESIDENCIA", how="inner")
    )

    print(10*"#")
    print("Iniciando a escrita dos dados na zona de processamento")
    (
        uf_notas
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