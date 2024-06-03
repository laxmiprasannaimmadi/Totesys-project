variable "lambda_name" {
  type = string
  default = "ingestion_lambda"
}

variable "Processing_lambda" {
  type = string
  default = "processing_lambda"
}

variable "warehouse_lambda" {
  type = string
  default = "warehouse_lambda"
}

variable "aws_region" {
  type = string
  default = "eu-west-2"
}

variable "python_runtime" {
  type = string
  default = "python3.11"
}

variable "source_file" {
  type = string
  default = "../src/ingestion/utils/ingestion_lambda_handler.py" 
}

variable "output_path" {
  type = string
  default = "../src/ingestion/utils/ingestion_lambda_handler.zip"
}

variable "metric_namespace_error" {
  type = string
  default = "Errors"
}

variable "metric_namespace_success" {
  type = string
  default = "Successes"
}

variable "metric_transformation_name_success" {
  type = string
  default = "SuccessfulIngestion"
}

variable "metric_transformation_name_error" {
  type = string
  default = "ErrorCount"
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

