resource "aws_s3_bucket_object" "converte_parquet" {
  bucket = aws_s3_bucket.dl_codes.id
  key    = "pyspark/enem_converte_parquet.py"
  acl    = "private"
  source = "../scripts/pyspark/enem_converte_parquet.py"
  etag   = filemd5("../scripts/pyspark/enem_converte_parquet.py")
}

resource "aws_s3_bucket_object" "anonimiza_inscricao" {
  bucket = aws_s3_bucket.dl_codes.id
  key    = "pyspark/enem_anonimiza_inscricao.py"
  acl    = "private"
  source = "../scripts/pyspark/enem_anonimiza_inscricao.py"
  etag   = filemd5("../scripts/pyspark/enem_anonimiza_inscricao.py")
}

resource "aws_s3_bucket_object" "agrega_idade" {
  bucket = aws_s3_bucket.dl_codes.id
  key    = "pyspark/enem_agrega_idade.py"
  acl    = "private"
  source = "../scripts/pyspark/enem_agrega_idade.py"
  etag   = filemd5("../scripts/pyspark/enem_agrega_idade.py")
}

resource "aws_s3_bucket_object" "agrega_sexo" {
  bucket = aws_s3_bucket.dl_codes.id
  key    = "pyspark/enem_agrega_sexo.py"
  acl    = "private"
  source = "../scripts/pyspark/enem_agrega_sexo.py"
  etag   = filemd5("../scripts/pyspark/enem_agrega_sexo.py")
}

resource "aws_s3_bucket_object" "agrega_notas" {
  bucket = aws_s3_bucket.dl_codes.id
  key    = "pyspark/enem_agrega_notas.py"
  acl    = "private"
  source = "../scripts/pyspark/enem_agrega_notas.py"
  etag   = filemd5("../scripts/pyspark/enem_agrega_notas.py")
}

resource "aws_s3_bucket_object" "join_final" {
  bucket = aws_s3_bucket.dl_codes.id
  key    = "pyspark/enem_join_final.py"
  acl    = "private"
  source = "../scripts/pyspark/enem_join_final.py"
  etag   = filemd5("../scripts/pyspark/enem_join_final.py")
}