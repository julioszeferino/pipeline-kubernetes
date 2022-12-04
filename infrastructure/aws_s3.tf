# bucket airflow logs
resource "aws_s3_bucket" "airflow_logs" {
    bucket = "julioszeferino-airflow-logs"
    acl    = "private"

    tags = {
        IES = "IGTI"
        CURSO = "EDC"
    }

    server_side_encryption_configuration {
        rule {
            apply_server_side_encryption_by_default {
                sse_algorithm = "AES256"
            }
        }
    }
}

# bucket landing zone
resource "aws_s3_bucket" "dl_landing_zone" {
    bucket = "julioszeferino-dl-landing-zone"
    acl    = "private"

    tags = {
        IES = "IGTI"
        CURSO = "EDC"
    }

    server_side_encryption_configuration {
        rule {
            apply_server_side_encryption_by_default {
                sse_algorithm = "AES256"
            }
        }
    }
}

# bucket processing zone
resource "aws_s3_bucket" "dl_processing_zone" {
    bucket = "julioszeferino-dl-processing-zone"
    acl    = "private"

    tags = {
        IES = "IGTI"
        CURSO = "EDC"
    }

    server_side_encryption_configuration {
        rule {
            apply_server_side_encryption_by_default {
                sse_algorithm = "AES256"
            }
        }
    }
}

# bucket consumer zone
resource "aws_s3_bucket" "dl_consumer_zone" {
    bucket = "julioszeferino-dl-consumer-zone"
    acl    = "private"

    tags = {
        IES = "IGTI"
        CURSO = "EDC"
    }

    server_side_encryption_configuration {
        rule {
            apply_server_side_encryption_by_default {
                sse_algorithm = "AES256"
            }
        }
    }
}

# bucket spark codes
resource "aws_s3_bucket" "dl_codes" {
    bucket = "julioszeferino-dl-codes"
    acl    = "private"

    tags = {
        IES = "IGTI"
        CURSO = "EDC"
    }

    server_side_encryption_configuration {
        rule {
            apply_server_side_encryption_by_default {
                sse_algorithm = "AES256"
            }
        }
    }
}