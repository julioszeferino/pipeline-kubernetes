# criando uma database no glue
resource "aws_glue_catalog_database" "dl_consumer_zone" {
  name = "dl_consumer_zone"
}

# criando o crawler para dados anonimizados do enem
resource "aws_glue_crawler" "enem-anom-crawler" {
  database_name = aws_glue_catalog_database.dl_consumer_zone.name # database criado no passo anterior
  name          = "enem_anom_crawler"
  role          = aws_iam_role.glue_role.arn # role criada no arquivo iam.tf

  # configurando o crawler -> buscar tudo na pasta firehose do bucket
  s3_target {
    path = "s3://${aws_s3_bucket.dl_consumer_zone.bucket}/enem_anom/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF

  tags = {
    IES   = "IGTI",
    CURSO = "EDC"
  }
}

# criando o crawler para dados do enem agrupados por uf
resource "aws_glue_crawler" "stream" {
  database_name = aws_glue_catalog_database.dl_consumer_zone.name # database criado no passo anterior
  name          = "enem_uf_final_crawler"
  role          = aws_iam_role.glue_role.arn # role criada no arquivo iam.tf

  # configurando o crawler -> buscar tudo na pasta firehose do bucket
  s3_target {
    path = "s3://${aws_s3_bucket.dl_consumer_zone.bucket}/enem_uf/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF

  tags = {
    IES   = "IGTI",
    CURSO = "EDC"
  }
}

