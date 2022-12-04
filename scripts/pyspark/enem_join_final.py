# importando as bibliotecas
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

path_pz: str = "s3a://julioszeferino-dl-processing-zone/intermediarias/"
path_consumer: str = "s3a://julioszeferino-dl-consumer-zone/enem_uf"


if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .appName("Enem Spark Job 6")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Iniciando a leitura dos dados da zona de processamento")
    uf_idade = (
        spark
        .read
        .format("parquet")
        .load(f"{path_pz}uf_idade")
    )

    uf_sexo = (
        spark
        .read
        .format("parquet")
        .load(f"{path_pz}uf_sexo")
    )

    uf_notas = (
        spark
        .read
        .format("parquet")
        .load(f"{path_pz}uf_notas")
    )

    print(10*"#")
    print("Iniciando a junção dos dados..")
    uf_final = (
        uf_idade
        .join(uf_sexo, on="SG_UF_RESIDENCIA", how="inner")
        .join(uf_notas, on="SG_UF_RESIDENCIA", how="inner")
    )

    print(10*"#")
    print("Iniciando a escrita dos dados..")
    (
        uf_final
        .write
        .format("parquet")
        .mode("overwrite")
        .save(path_consumer)
    )

    print(10*"#")
    print("Escrita concluída!")
    print("Fim do job!")
    print(10*"#")
    spark.stop()
