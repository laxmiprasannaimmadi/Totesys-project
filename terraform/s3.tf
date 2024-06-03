#----------------------------------------------------------------------------------------------------------------------------
#---------------------------INGESTION-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "nc-team-reveries-ingestion"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "example" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

#----------------------------------------------------------------------------------------------------------------------------
#---------------------------PROCESSING-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

resource "aws_s3_bucket" "processing_bucket" {
  bucket = "nc-team-reveries-processing"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "processing_bucket_ver" {
  bucket = aws_s3_bucket.processing_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.processing_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "timestamp"
  }

  depends_on = [aws_lambda_permission.allow_bucket]
}

#----------------------------------------------------------------------------------------------------------------------------
#---------------------------PROCESSING-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------


resource "aws_s3_bucket_notification" "processing_bucket_notification" {
  bucket = aws_s3_bucket.processing_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.warehouse_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "timestamp"
  }

  depends_on = [aws_lambda_permission.allow_bucket]
}