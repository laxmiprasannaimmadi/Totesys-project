#----------------------------------------------------------------------------------------------------------------------------
#---------------------------INGESTION-TERRAFORM------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

data "archive_file" "ingestion_lambda_file" {
  type        = "zip"
  output_file_mode = "0666"
  source_dir = "../src/ingestion/"   
  output_path = "../terraform/deploy.zip"          
}

data "archive_file" "dependancies" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "../aws_depedancy_layers/ingestion_layer"
  output_path = "../python.zip"
}

resource "aws_lambda_function" "ingestion_lambda" {
    function_name = "ingestion_lambda"
    filename = "deploy.zip"
    role = aws_iam_role.lambda_role.arn
    handler = "ingestion_lambda_handler.ingestion_lambda_handler"       
    runtime = var.python_runtime        
    timeout = 60
    source_code_hash = data.archive_file.ingestion_lambda_file.output_base64sha256
    layers = [aws_lambda_layer_version.dependancies_layer.arn]
    
}

resource "aws_lambda_permission" "lambda_invoke" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.ingestion_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}


resource "aws_lambda_permission" "allow_eventbridge" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.ingestion_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_lambda_layer_version" "dependancies_layer" {
  layer_name = "dependancies_layer"
  compatible_runtimes = [var.python_runtime]
  compatible_architectures = ["x86_64", "arm64"]
  filename = "../python.zip"
}

resource "aws_lambda_function_event_invoke_config" "lambda_invoke_config" {
  function_name                = aws_lambda_function.ingestion_lambda.function_name
  maximum_event_age_in_seconds = 60
  maximum_retry_attempts       = 0
  qualifier     = "$LATEST"
}

#----------------------------------------------------------------------------------------------------------------------------
#---------------------------PROCESSING-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

data "archive_file" "processing_lambda_data" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "../src/processing/"
  output_path = "../processing_deploy.zip"
}

data "archive_file" "processing_dependencies" {
  type        = "zip"
  output_file_mode = "0666"
  source_dir = "../aws_depedancy_layers/processing_layer"
  output_path = "../processing_requirements.zip"       
}

resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processing_lambda.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.ingestion_bucket.arn
}

resource "aws_lambda_function" "processing_lambda" {
    function_name = "processing_lambda"
    filename = "../processing_deploy.zip"
    role = aws_iam_role.processing_lambda_role.arn 
    handler = "processing_lambda_handler.processed_lambda_handler"
    memory_size   = 1024 
    runtime = var.python_runtime        
    timeout = 180               
    source_code_hash = data.archive_file.processing_lambda_data.output_base64sha256
    layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:12", aws_lambda_layer_version.processing_dependencies_layer.arn] 
    
}

resource "aws_lambda_function_event_invoke_config" "processing_lambda_invoke_config" {
  function_name                = aws_lambda_function.processing_lambda.function_name
  maximum_event_age_in_seconds = 60
  maximum_retry_attempts       = 0
  qualifier     = "$LATEST"
}

resource "aws_lambda_layer_version" "processing_dependencies_layer" {
  layer_name = "processing_dependancies_layer"
  compatible_runtimes = [var.python_runtime]
  compatible_architectures = ["x86_64", "arm64"]
  filename = "../processing_requirements.zip"
}

#----------------------------------------------------------------------------------------------------------------------------
#---------------------------WAREHOUSE-TERRAFORM------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

data "archive_file" "warehouse_lambda_data" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "../src/warehouse/"
  output_path = "../warehouse_deploy.zip"
}

data "archive_file" "warehouse_dependencies" {
  type        = "zip"
  output_file_mode = "0666"
  source_dir = "../aws_depedancy_layers/warehouse_layer"
  output_path = "../warehouse_requirements.zip"       
}

resource "aws_lambda_permission" "allow_bucket_warehouse" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.warehouse_lambda.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.processing_bucket.arn
}

resource "aws_lambda_function" "warehouse_lambda" {
    function_name = "warehouse_lambda"
    filename = "../warehouse_deploy.zip"
    role = aws_iam_role.warehouse_lambda_role.arn 
    handler = "warehouse_lambda_handler.warehouse_lambda_handler"
    memory_size   = 1024
    runtime = var.python_runtime        
    timeout = 180            
    source_code_hash = data.archive_file.warehouse_lambda_data.output_base64sha256
    layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:12", aws_lambda_layer_version.warehouse_dependencies_layer.arn] 
}

resource "aws_lambda_function_event_invoke_config" "warehouse_lambda_invoke_config" {
  function_name                = aws_lambda_function.warehouse_lambda.function_name
  maximum_event_age_in_seconds = 60
  maximum_retry_attempts       = 0
  qualifier     = "$LATEST"
}

resource "aws_lambda_layer_version" "warehouse_dependencies_layer" {
  layer_name = "warehouse_dependancies_layer"
  compatible_runtimes = [var.python_runtime]
  compatible_architectures = ["x86_64", "arm64"]
  filename = "../warehouse_requirements.zip"
}